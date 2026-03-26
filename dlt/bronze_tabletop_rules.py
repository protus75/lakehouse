"""Bronze layer: raw extraction from tabletop RPG PDFs.

Extracts and stores immutable raw data:
- Marker markdown (full document)
- pymupdf page texts with printed page numbers
- ToC entries with page ranges
- Known entry names from index sections
- Detected watermarks

No cleanup, no entry building, no chunking. That's silver/gold.

Run: docker exec lakehouse-workspace python -u dlt/bronze_tabletop_rules.py
"""

import hashlib
import json
import re
from pathlib import Path
from datetime import datetime, timezone

import duckdb
import fitz  # pymupdf

from dlt.lib.tabletop_cleanup import _log, load_config, _extract_toc_line

DB_PATH = "/workspace/db/lakehouse.duckdb"
DOCUMENTS_DIR = Path("/workspace/documents/tabletop_rules/raw")
CONFIGS_DIR = Path("/workspace/documents/tabletop_rules/configs")


# ── Schema ───────────────────────────────────────────────────────

def init_bronze_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze_tabletop")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_tabletop.files (
            source_file     VARCHAR PRIMARY KEY,
            pdf_size_bytes  BIGINT NOT NULL,
            total_pages     INTEGER NOT NULL,
            config_hash     VARCHAR,
            extracted_at    TIMESTAMP NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_tabletop.marker_extractions (
            source_file     VARCHAR PRIMARY KEY,
            markdown_text   VARCHAR NOT NULL,
            char_count      INTEGER NOT NULL,
            extracted_at    TIMESTAMP NOT NULL
        )
    """)

    # marker_pages reserved for future per-page extraction if needed

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_tabletop.page_texts (
            source_file       VARCHAR NOT NULL,
            page_index        INTEGER NOT NULL,
            page_text         VARCHAR NOT NULL,
            printed_page_num  INTEGER,
            PRIMARY KEY (source_file, page_index)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_tabletop.toc_raw (
            source_file     VARCHAR NOT NULL,
            title           VARCHAR NOT NULL,
            page_start      INTEGER NOT NULL,
            page_end        INTEGER,
            is_excluded     BOOLEAN DEFAULT FALSE,
            PRIMARY KEY (source_file, title)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_tabletop.known_entries_raw (
            source_file     VARCHAR NOT NULL,
            entry_name      VARCHAR NOT NULL,
            entry_class     VARCHAR,
            entry_level     INTEGER,
            ref_page        INTEGER,
            source_section  VARCHAR,
            school          VARCHAR,
            sphere          VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_tabletop.tables_raw (
            source_file       VARCHAR NOT NULL,
            table_number      INTEGER NOT NULL,
            table_title       VARCHAR NOT NULL,
            format            VARCHAR NOT NULL,
            row_index         INTEGER NOT NULL,
            cells             VARCHAR NOT NULL,
            PRIMARY KEY (source_file, table_number, row_index)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_tabletop.authority_table_entries (
            source_file       VARCHAR NOT NULL,
            entry_name        VARCHAR NOT NULL,
            entry_type        VARCHAR NOT NULL,
            source_table      VARCHAR NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_tabletop.spell_list_entries (
            source_file       VARCHAR NOT NULL,
            entry_name        VARCHAR NOT NULL,
            entry_class       VARCHAR NOT NULL,
            entry_level       INTEGER NOT NULL,
            is_reversible     BOOLEAN NOT NULL,
            source_section    VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_tabletop.watermarks (
            source_file       VARCHAR NOT NULL,
            watermark_text    VARCHAR NOT NULL,
            occurrence_count  INTEGER NOT NULL,
            PRIMARY KEY (source_file, watermark_text)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_tabletop.ocr_issues (
            source_file       VARCHAR NOT NULL,
            wrong_text        VARCHAR NOT NULL,
            suggested_fix     VARCHAR NOT NULL,
            context           VARCHAR,
            status            VARCHAR DEFAULT 'pending',
            checked_at        TIMESTAMP NOT NULL
        )
    """)


# ── Extraction Functions ─────────────────────────────────────────

def extract_page_texts(filepath: Path, config: dict) -> tuple[list[str], dict[int, int], int]:
    """Extract page texts and printed page numbers from PDF via pymupdf.
    Returns (page_texts, page_printed, total_pages)."""
    page_pattern = config.get("toc", {}).get("page_number_pattern", r"^\d{1,3}$")
    doc = fitz.open(str(filepath))
    total_pages = len(doc)
    page_texts = []
    page_printed = {}

    # First pass: detect raw candidates from bottom/top of each page
    raw_printed = {}
    # Use text POSITION to find printed page numbers reliably.
    # Page numbers live in the bottom margin (93-97% of page height),
    # below all body content and above watermarks.
    # This avoids false matches from table data in the body.
    margin_min_pct = 0.92  # page number must be below 92% of page height
    margin_max_pct = 0.98  # and above 98% (watermark zone)

    for page_idx in range(total_pages):
        page = doc[page_idx]
        page_texts.append(page.get_text("text"))
        page_height = page.rect.height

        # Collect all text spans in the bottom margin zone
        margin_texts = []
        for block in page.get_text("dict")["blocks"]:
            if "lines" not in block:
                continue
            for line in block["lines"]:
                for span in line["spans"]:
                    y_pct = span["bbox"][1] / page_height
                    if margin_min_pct <= y_pct <= margin_max_pct:
                        text = span["text"].strip()
                        if text and re.match(page_pattern, text):
                            margin_texts.append(int(text))

        if margin_texts:
            # Take the number closest to the bottom (highest Y) — should be
            # exactly one page number in the margin zone
            page_printed[page_idx] = margin_texts[-1]
        else:
            page_printed[page_idx] = page_idx

    doc.close()

    # ── Validate page numbers ──────────────────────────────────────
    _validate_page_numbers(page_printed, total_pages, filepath.name)

    return page_texts, page_printed, total_pages


def _validate_page_numbers(page_printed: dict[int, int], total_pages: int, filename: str) -> None:
    """Validate that detected page numbers are sane. Fails fast if not.

    Checks:
    1. Monotonically non-decreasing (each page >= previous)
    2. No backwards jumps > 1 (indicates table data contamination)
    3. Coverage: most pages (>80%) should have been detected from margin
    4. Consistency: the offset (printed - idx) shouldn't vary wildly
    """
    errors = []

    # Check monotonicity
    backwards = []
    prev = -1
    for page_idx in range(total_pages):
        printed = page_printed.get(page_idx, page_idx)
        if printed < prev:
            backwards.append((page_idx, printed, prev))
        prev = printed
    if backwards:
        examples = backwards[:5]
        errors.append(
            f"Page numbers go backwards at {len(backwards)} pages. "
            f"Examples: {', '.join(f'idx {i}: {p} after {pv}' for i, p, pv in examples)}"
        )

    # Check for big jumps (>5 pages at once = likely contamination)
    big_jumps = []
    prev = page_printed.get(0, 0)
    for page_idx in range(1, total_pages):
        printed = page_printed.get(page_idx, page_idx)
        gap = printed - prev
        if gap > 5:
            big_jumps.append((page_idx, prev, printed))
        prev = printed
    if big_jumps:
        examples = big_jumps[:5]
        errors.append(
            f"Page numbers jump >5 at {len(big_jumps)} pages. "
            f"Examples: {', '.join(f'idx {i}: {p1}->{p2}' for i, p1, p2 in examples)}"
        )

    # Check that the last page's number is reasonable
    last_printed = page_printed.get(total_pages - 1, 0)
    if last_printed > total_pages * 2:
        errors.append(
            f"Last page number ({last_printed}) is >2x total pages ({total_pages})"
        )

    if errors:
        msg = f"Page number validation FAILED for {filename}:\n  " + "\n  ".join(errors)
        _log(f"  WARNING: {msg}")
        raise ValueError(msg)


MARKER_CACHE_DIR = Path("/workspace/cache/marker")


def _clean_marker_md(md: str) -> str:
    """Strip image references and rejoin hyphenated words."""
    md = re.sub(r"!\[.*?\]\(.*?\)", "", md)
    md = re.sub(r"(\w)-\s*\n\s*(\w)", r"\1\2", md)
    return md


def extract_marker_markdown(filepath: Path) -> str:
    """Run Marker on full document. Checks disk cache first (legacy)."""
    cache_path = MARKER_CACHE_DIR / f"{filepath.stem}.md"
    if cache_path.exists():
        _log(f"  Marker: using disk cache {cache_path.name}")
        md = cache_path.read_text(encoding="utf-8")
    else:
        from marker.converters.pdf import PdfConverter
        from marker.models import create_model_dict
        models = create_model_dict()
        converter = PdfConverter(artifact_dict=models)
        rendered = converter(str(filepath))
        md = rendered.markdown
    return _clean_marker_md(md)


def extract_marker_pages(filepath: Path, total_pages: int) -> list[str]:
    """Run Marker per-page for page boundary ground truth.
    Returns list of markdown strings, one per page."""
    cache_dir = MARKER_CACHE_DIR / filepath.stem
    if cache_dir.exists() and len(list(cache_dir.glob("*.md"))) == total_pages:
        _log(f"  Marker pages: using disk cache ({total_pages} pages)")
        pages = []
        for i in range(total_pages):
            page_file = cache_dir / f"page_{i:04d}.md"
            pages.append(page_file.read_text(encoding="utf-8") if page_file.exists() else "")
        return pages

    from marker.converters.pdf import PdfConverter
    from marker.models import create_model_dict
    models = create_model_dict()

    _log(f"  Marker pages: extracting {total_pages} pages...")
    cache_dir.mkdir(parents=True, exist_ok=True)
    pages = []
    for i in range(total_pages):
        converter = PdfConverter(artifact_dict=models)
        rendered = converter(str(filepath), pages=[i])
        md = _clean_marker_md(rendered.markdown)
        pages.append(md)
        (cache_dir / f"page_{i:04d}.md").write_text(md, encoding="utf-8")
        if (i + 1) % 50 == 0:
            _log(f"  Marker pages: {i + 1}/{total_pages}")

    _log(f"  Marker pages: {total_pages} pages extracted")
    return pages


def extract_toc(page_texts: list[str], config: dict) -> tuple[list[dict], list[dict]]:
    """Parse ToC from first N pages.

    Returns (sections, toc_tables):
      sections: [{title, page_start, page_end, is_excluded}, ...]
      toc_tables: [{table_number, title, page}, ...]
    """
    toc_config = config.get("toc", {})
    chapter_patterns = [re.compile(p, re.IGNORECASE) for p in toc_config.get("chapter_patterns", [])]
    table_pattern_str = toc_config.get("table_pattern", "")
    table_pattern = re.compile(table_pattern_str, re.IGNORECASE) if table_pattern_str else None
    scan_pages = toc_config.get("toc_scan_pages", 15)
    exclude_set = set(t.lower() for t in config.get("exclude_chapters", []))

    sections = []
    toc_tables = []
    seen_sections = set()
    seen_tables = set()

    for page_idx in range(min(scan_pages, len(page_texts))):
        for line in page_texts[page_idx].split("\n"):
            parsed = _extract_toc_line(line)
            if not parsed:
                continue
            title, page = parsed

            # Check for table entries
            if table_pattern and table_pattern.match(title):
                # Extract table number from title like "Table 37: Nonweapon Proficiency Groups"
                num_match = re.search(r'\d+', title)
                if num_match:
                    table_num = int(num_match.group())
                    if table_num not in seen_tables:
                        seen_tables.add(table_num)
                        # Title without "Table N:" prefix
                        colon_idx = title.find(":")
                        table_title = title[colon_idx + 1:].strip() if colon_idx >= 0 else title
                        toc_tables.append({
                            "table_number": table_num,
                            "title": table_title,
                            "page": page,
                        })

            # Check for chapter entries
            for pat in chapter_patterns:
                if pat.match(title):
                    if title not in seen_sections:
                        seen_sections.add(title)
                        sections.append({
                            "title": title, "page_start": page,
                            "is_excluded": title.lower() in exclude_set,
                        })
                    break

    sections.sort(key=lambda e: e["page_start"])
    for i, entry in enumerate(sections):
        entry["page_end"] = sections[i + 1]["page_start"] - 1 if i + 1 < len(sections) else 9999

    toc_tables.sort(key=lambda t: t["table_number"])
    return sections, toc_tables


def _parse_ordinal_level(text: str) -> int | None:
    """Parse '1st', '2nd', '3rd', '4th' etc. to integer."""
    text = text.lower().strip().rstrip(")")
    for suffix in ("st", "nd", "rd", "th"):
        if text.endswith(suffix):
            num = text[:-len(suffix)]
            if num.isdigit():
                return int(num)
    return None


def extract_known_entries(page_texts: list[str], page_printed: dict[int, int],
                          toc_sections: list[dict], config: dict) -> list[dict]:
    """Get entry names and full metadata from ALL excluded index sections.

    Handles multiple index formats:
    - Spell Index (Appendix 7): 'Name (Pr 4) . . . . Page' → name, class, level, page
    - Spells by School (Appendix 5): heading = school, lines = 'Name (1st)' → name, school, level
    - Spells by Sphere (Appendix 6): heading = sphere, lines = 'Name (1st)' → name, sphere, level

    Returns list of dicts with all available fields."""
    excluded = [s for s in toc_sections if s["is_excluded"]]
    if not excluded:
        return []

    ingestion = config.get("ingestion", {})
    min_idx = ingestion.get("min_index_entry_length", 3)
    max_idx = ingestion.get("max_index_entry_length", 50)

    entries = []
    seen = set()

    for section in excluded:
        section_title = section["title"].lower()
        is_school_index = "by school" in section_title
        is_sphere_index = "by sphere" in section_title
        is_grouped_index = is_school_index or is_sphere_index

        current_group = None  # current school or sphere heading

        for page_idx in range(len(page_texts)):
            printed = page_printed.get(page_idx, page_idx)
            if not (section["page_start"] <= printed <= section["page_end"]):
                continue

            for line in page_texts[page_idx].split("\n"):
                stripped = line.strip()
                if not stripped or len(stripped) < 2:
                    continue

                if is_grouped_index:
                    # Grouped index: headings are school/sphere names,
                    # lines underneath are 'SpellName (1st)' or 'SpellName (2nd)'
                    # A heading is a short line with no parenthetical and starts uppercase
                    if stripped[0].isupper() and "(" not in stripped and len(stripped) < 30:
                        current_group = stripped
                        continue

                    # Parse spell line: 'SpellName (1st)'
                    paren_start = stripped.rfind("(")
                    if paren_start < 0:
                        continue
                    name = stripped[:paren_start].strip()
                    paren_end = stripped.rfind(")")
                    inner = stripped[paren_start + 1:paren_end].strip() if paren_end > paren_start else ""
                    level = _parse_ordinal_level(inner)

                    if not name or len(name) < min_idx or len(name) > max_idx:
                        continue
                    if not name[0].isupper():
                        continue

                    entry = {
                        "entry_name": name.lower(),
                        "entry_class": "wizard" if is_school_index else "priest",
                        "entry_level": level,
                        "ref_page": None,
                        "source_section": section["title"],
                        "school": current_group if is_school_index else None,
                        "sphere": current_group if is_sphere_index else None,
                    }

                    key = (entry["entry_name"], entry["entry_class"],
                           entry.get("school"), entry.get("sphere"))
                    if key not in seen:
                        seen.add(key)
                        entries.append(entry)

                else:
                    # Standard index: 'Name (Pr 4) . . . . Page'
                    parsed = _extract_toc_line(stripped)
                    if not parsed:
                        continue
                    raw_title, ref_page = parsed

                    entry_class = None
                    entry_level = None
                    name = raw_title

                    paren_start = raw_title.rfind("(")
                    if paren_start > 0:
                        paren_end = raw_title.rfind(")")
                        if paren_end > paren_start:
                            inner = raw_title[paren_start + 1:paren_end].strip()
                            parts = inner.split()
                            if parts:
                                cls = parts[0].lower()
                                if cls in ("pr", "pri", "priest"):
                                    entry_class = "priest"
                                elif cls in ("wiz", "wizard"):
                                    entry_class = "wizard"
                                if len(parts) >= 2 and parts[1].isdigit():
                                    entry_level = int(parts[1])
                        name = raw_title[:paren_start].strip()

                    if not name or len(name) < min_idx or len(name) > max_idx:
                        continue
                    if not name[0].isupper():
                        continue

                    entry = {
                        "entry_name": name.lower(),
                        "entry_class": entry_class,
                        "entry_level": entry_level,
                        "ref_page": ref_page,
                        "source_section": section["title"],
                        "school": None,
                        "sphere": None,
                    }

                    key = (entry["entry_name"], entry["entry_class"])
                    if key not in seen:
                        seen.add(key)
                        entries.append(entry)

    return entries


def extract_spell_list_entries(filepath: Path, page_printed: dict[int, int],
                               toc_sections: list[dict]) -> list[dict]:
    """Parse Appendix 1: Spell Lists using pymupdf font info.

    Extracts: name, level, is_reversible (italic), spell_class (wizard/priest).
    Uses font flags to detect italic (reversible) and bold (level headings)."""
    import fitz

    # Find spell list sections
    spell_list_sections = [s for s in toc_sections
                           if s["is_excluded"] and "spell list" in s["title"].lower()]
    if not spell_list_sections:
        return []

    doc = fitz.open(str(filepath))

    # Phase 1: collect all text lines with formatting, sorted by reading order (y, x)
    all_lines = []  # (y, x, text, is_italic, is_bold, page_idx)
    for section in spell_list_sections:
        page_offset = 0
        for page_idx in range(doc.page_count):
            printed = page_printed.get(page_idx, page_idx)
            if not (section["page_start"] <= printed <= section["page_end"]):
                continue
            page = doc[page_idx]
            page_height = page.rect.height
            for block in page.get_text("dict")["blocks"]:
                if "lines" not in block:
                    continue
                for line in block["lines"]:
                    x0, y0 = line["bbox"][0], line["bbox"][1]
                    text = ""
                    italic = False
                    bold = False
                    for span in line["spans"]:
                        t = span["text"].strip()
                        if t:
                            text += t + " "
                            if span["flags"] & 2:
                                italic = True
                            if span["flags"] & 16:
                                bold = True
                    text = text.strip()
                    if text and len(text) >= 2:
                        # Use absolute y for cross-page ordering
                        abs_y = page_offset + y0
                        all_lines.append((abs_y, x0, text, italic, bold))
            page_offset += page_height
    doc.close()

    if not all_lines:
        return []

    # Phase 2: find bold headings to identify class and level columns
    class_headings = []  # (abs_y, class_name)
    level_columns = []   # (abs_y, x, level_num)
    for abs_y, x, text, italic, bold in all_lines:
        if not bold:
            continue
        text_lower = text.lower().strip()
        if "wizard" in text_lower and "spell" in text_lower:
            class_headings.append((abs_y, "wizard"))
        elif "priest" in text_lower and "spell" in text_lower:
            class_headings.append((abs_y, "priest"))
        for ordinal, num in [("1st", 1), ("2nd", 2), ("3rd", 3), ("4th", 4),
                             ("5th", 5), ("6th", 6), ("7th", 7), ("8th", 8), ("9th", 9)]:
            if text_lower.startswith(ordinal):
                level_columns.append((abs_y, round(x), num))
                break

    if not level_columns:
        return []

    # Phase 3: for each level column, determine its class and collect spells
    entries = []
    section_title = spell_list_sections[0]["title"]

    for col_y, col_x, level_num in level_columns:
        # Determine class: find the most recent class heading before this column
        spell_class = None
        for h_y, h_class in sorted(class_headings):
            if h_y <= col_y:
                spell_class = h_class
        if not spell_class:
            continue

        # Collect non-bold lines in this column:
        # - Same x bucket (within ±25px of col_x)
        # - Below the level heading (abs_y > col_y)
        # - Above the next level heading at same x, or next class heading
        next_y = float("inf")
        for other_y, other_x, _ in level_columns:
            if other_y > col_y and abs(other_x - col_x) < 25:
                next_y = min(next_y, other_y)
                break
        for h_y, _ in class_headings:
            if h_y > col_y:
                next_y = min(next_y, h_y)

        col_spans = []
        for abs_y, x, text, italic, bold in all_lines:
            if bold or text.isdigit():
                continue
            if abs_y <= col_y or abs_y >= next_y:
                continue
            if abs(round(x) - col_x) > 30:
                continue
            if len(text) < 2:
                continue
            text_stripped = text.strip()
            if "order #" in text_stripped.lower() or text_stripped.startswith("*"):
                continue
            col_spans.append((abs_y, x, text_stripped, italic))

        col_spans.sort()

        if not col_spans:
            continue

        # Determine base x for spell names in this column (most common x)
        from collections import Counter
        x_freq = Counter(round(s[1]) for s in col_spans)
        base_x_col = x_freq.most_common(1)[0][0]

        # Parse into spell names with continuation detection
        pending_name = ""
        pending_italic = False
        last_name = ""

        for abs_y, x, text, italic in col_spans:
            # Continuation: indented > 5px from base_x
            is_continuation = round(x) > base_x_col + 5

            if is_continuation and pending_name:
                pending_name += " " + text
                pending_italic = pending_italic or italic
            else:
                # Flush previous
                if pending_name:
                    name_lower = pending_name.lower().strip()
                    # Alphabetical sanity check
                    if last_name and name_lower < last_name:
                        _log(f"    Warning: '{name_lower}' before '{last_name}' in {spell_class} L{level_num}")
                    entries.append({
                        "entry_name": name_lower,
                        "entry_class": spell_class,
                        "entry_level": level_num,
                        "is_reversible": pending_italic,
                        "source_section": section_title,
                    })
                    last_name = name_lower
                pending_name = text
                pending_italic = italic

        # Flush last
        if pending_name:
            entries.append({
                "entry_name": pending_name.lower().strip(),
                "entry_class": spell_class,
                "entry_level": level_num,
                "is_reversible": pending_italic,
                "source_section": section_title,
            })

    return [e for e in entries if len(e["entry_name"]) >= 3]


def extract_all_tables(markdown: str, toc_tables: list[dict],
                       page_texts: list[str],
                       page_printed: dict[int, int],
                       config: dict | None = None) -> list[dict]:
    """Parse ALL tables by walking markdown lines sequentially.

    Scan lines top-to-bottom. When we see a 'Table N' label line followed
    by pipe-delimited rows, that's the data for Table N. Match the number
    to the ToC to get the title. Tables appear in document order — once a
    table number is found, it won't appear again as a real table.

    Returns list of dicts: {table_number, table_title, rows: [[cell, ...], ...]}
    """
    from rapidfuzz import fuzz

    toc_lookup = {t["table_number"]: t["title"] for t in toc_tables}
    toc_nums_sorted = sorted(toc_lookup.keys())  # sequential order
    toc_nums = set(toc_nums_sorted)
    # Next expected table number — only accept this or higher (sequential)
    next_expected_idx = 0

    # Build title → table_number lookup for fuzzy matching headings
    title_to_num = {t["title"].lower(): t["table_number"] for t in toc_tables if t["title"]}

    lines = markdown.split("\n")
    tables = []
    found_nums = set()

    pending_table_num = None
    lines_since_label = 0  # how many lines since we set pending_table_num
    i = 0

    def _try_table_label(text: str) -> int | None:
        """Check if text is a 'Table N:' label (not a prose reference).
        Returns the table number or None.

        A label: 'Table 37:', '#### Table 37:', '| Table 33:<br>Bard...'
        NOT a label: 'Table 37 lists all...', 'as shown in Table 37'
        """
        clean = text.lstrip("#").lstrip().lstrip("*").lstrip()
        clean = clean.lstrip("|").lstrip()
        clean = clean.split("<br>")[0].strip()
        if not clean.lower().startswith("table "):
            return None
        rest = clean[6:].lstrip()
        num_str = ""
        j = 0
        for j, ch in enumerate(rest):
            if ch.isdigit():
                num_str += ch
            else:
                break
        if not num_str:
            return None
        # What follows the number? Must be ':' or end of string or just a title.
        # Reject if followed by prose verbs (lists, shows, contains, etc.)
        after = rest[len(num_str):].lstrip()
        if after and not after.startswith(":") and not after.startswith("."):
            # Check first word — if it's a verb, this is prose not a label
            first_word = after.split()[0].lower().rstrip(".,;") if after.split() else ""
            prose_verbs = {"lists", "shows", "contains", "gives", "indicates",
                           "details", "describes", "summarizes", "provides",
                           "determines", "displays", "includes", "is", "has",
                           "for", "to", "of", "and", "the", "in", "on",
                           "would", "can", "also", "or"}
            if first_word in prose_verbs:
                return None
        candidate = int(num_str)
        if candidate in toc_nums and candidate not in found_nums:
            return candidate
        return None

    while i < len(lines):
        stripped = lines[i].strip()

        # Check if this line contains a "Table N" label
        candidate = _try_table_label(stripped)
        if candidate is not None:
            # If we already have a pending label that never found pipes,
            # capture it as a text table before moving on
            if pending_table_num is not None and pending_table_num != candidate:
                label_line = i - lines_since_label
                text_rows = []
                for j in range(label_line, i):
                    s = lines[j].strip()
                    if s:
                        text_rows.append([s])
                if len(text_rows) >= 2:
                    tables.append({
                        "table_number": pending_table_num,
                        "table_title": toc_lookup.get(pending_table_num, ""),
                        "format": "text",
                        "rows": text_rows,
                    })
                    found_nums.add(pending_table_num)
                    while (next_expected_idx < len(toc_nums_sorted)
                           and toc_nums_sorted[next_expected_idx] <= pending_table_num):
                        next_expected_idx += 1
            pending_table_num = candidate
            lines_since_label = 0

        # If no table number match, try fuzzy matching heading text to ToC titles.
        # Only match lines that look like table headings (short, formatted lines)
        # and NOT section headings that happen to share a table title.
        # Require either: pipes on the next few lines OR "table" in the heading.
        if pending_table_num is None and stripped and next_expected_idx < len(toc_nums_sorted):
            clean_heading = stripped.lstrip("#").lstrip().lstrip("*").strip().rstrip("*").strip()
            if 5 <= len(clean_heading) <= 80:
                # Check if pipes follow within 5 lines (actual table, not prose)
                has_nearby_pipes = any(
                    j < len(lines) and lines[j].strip().startswith("|")
                    for j in range(i + 1, min(i + 6, len(lines)))
                )
                if has_nearby_pipes:
                    tnum = toc_nums_sorted[next_expected_idx]
                    if tnum not in found_nums:
                        title = toc_lookup.get(tnum, "").lower()
                        if title:
                            score = fuzz.ratio(clean_heading.lower(), title)
                            if score >= 85:
                                pending_table_num = tnum
                                lines_since_label = 0

        is_pipe_row = stripped.startswith("|") and stripped.count("|") >= 2

        # Check if a pipe row contains a "Table N:" label that starts a new table.
        # Only match if previous line was NOT a pipe row (= start of a new block,
        # not mid-stream in an existing block like the ToC).
        if is_pipe_row and pending_table_num is None:
            prev = lines[i - 1].strip() if i > 0 else ""
            prev_is_pipe = prev.startswith("|") and prev.count("|") >= 2
            if not prev_is_pipe:
                pipe_candidate = _try_table_label(stripped)
                if pipe_candidate is not None:
                    pending_table_num = pipe_candidate
                    lines_since_label = 0

        # If we have a pending table and hit pipe rows, capture as pipe format
        # But if the gap since label is large (>10), the pipes are likely unrelated
        # — trigger text capture instead
        if is_pipe_row and pending_table_num is not None and lines_since_label > 10:
            label_line = i - lines_since_label
            text_rows = []
            for j in range(label_line, i):
                s = lines[j].strip()
                if s:
                    text_rows.append([s])
            if len(text_rows) >= 2:
                tables.append({
                    "table_number": pending_table_num,
                    "table_title": toc_lookup.get(pending_table_num, ""),
                    "format": "text",
                    "rows": text_rows,
                })
                found_nums.add(pending_table_num)
                while (next_expected_idx < len(toc_nums_sorted)
                       and toc_nums_sorted[next_expected_idx] <= pending_table_num):
                    next_expected_idx += 1
            pending_table_num = None
            # Don't consume the pipe row — let it be captured by a future label
        if is_pipe_row and pending_table_num is not None:
            raw_rows = []
            while i < len(lines):
                s = lines[i].strip()
                if s.startswith("|") and s.count("|") >= 2:
                    raw_rows.append(s)
                    i += 1
                elif not s:
                    if i + 1 < len(lines) and lines[i + 1].strip().startswith("|"):
                        i += 1
                        continue
                    break
                else:
                    break

            parsed_rows = []
            for row in raw_rows:
                cells = [c.strip() for c in row.split("|")]
                if cells and cells[0] == "":
                    cells = cells[1:]
                if cells and cells[-1] == "":
                    cells = cells[:-1]
                if all(re.match(r'^[\s\-:]+$', c) or c == "" for c in cells):
                    continue
                parsed_rows.append(cells)

            if parsed_rows:
                # Reject suspiciously large blocks (>100 rows = likely ToC, not data)
                if len(parsed_rows) > 100:
                    _log(f"  Tables: skipping T{pending_table_num} — {len(parsed_rows)} rows (likely ToC)")
                    pending_table_num = None
                    continue
                tables.append({
                    "table_number": pending_table_num,
                    "table_title": toc_lookup.get(pending_table_num, ""),
                    "format": "pipe",
                    "rows": parsed_rows,
                })
                found_nums.add(pending_table_num)
                while (next_expected_idx < len(toc_nums_sorted)
                       and toc_nums_sorted[next_expected_idx] <= pending_table_num):
                    next_expected_idx += 1
                pending_table_num = None
            continue

        # Track gap since label
        if pending_table_num is not None:
            lines_since_label += 1
            # If no pipes within 15 lines, capture as text table
            if lines_since_label > 15:
                # Grab text from the label line forward until next heading/table label
                label_line = i - lines_since_label
                text_rows = []
                for j in range(label_line, len(lines)):
                    s = lines[j].strip()
                    # Stop at next "Table N:" label for a DIFFERENT table
                    if j > label_line + 1:
                        next_label = _try_table_label(s)
                        if next_label is not None and next_label != pending_table_num:
                            break
                    # Stop at chapter headings (## or #)
                    if s.startswith("##") and j > label_line + 2:
                        break
                    if s:
                        text_rows.append([s])
                if len(text_rows) >= 2:
                    tables.append({
                        "table_number": pending_table_num,
                        "table_title": toc_lookup.get(pending_table_num, ""),
                        "format": "text",
                        "rows": text_rows,
                    })
                    found_nums.add(pending_table_num)
                    while (next_expected_idx < len(toc_nums_sorted)
                           and toc_nums_sorted[next_expected_idx] <= pending_table_num):
                        next_expected_idx += 1
                pending_table_num = None

        i += 1

    # Second pass: use config missing_tables to find remaining tables
    missing_cfg = (config or {}).get("missing_tables", [])
    for mt in missing_cfg:
        tnum = mt["table_number"]
        if tnum in found_nums:
            continue
        heading = mt.get("heading", "").lower()
        content_marker = mt.get("content_marker", "")

        # Strategy 1: find by content_marker (literal string in a line)
        if content_marker:
            for li in range(len(lines)):
                if content_marker in lines[li]:
                    # Capture this pipe block or text block
                    start = li
                    # Walk back to find start of pipe block
                    while start > 0 and lines[start - 1].strip().startswith("|"):
                        start -= 1
                    block_rows = []
                    for j in range(start, len(lines)):
                        s = lines[j].strip()
                        if s.startswith("|") and s.count("|") >= 2:
                            cells = [c.strip() for c in s.split("|")]
                            if cells and cells[0] == "":
                                cells = cells[1:]
                            if cells and cells[-1] == "":
                                cells = cells[:-1]
                            if not all(re.match(r'^[\s\-:]+$', c) or c == "" for c in cells):
                                block_rows.append(cells)
                        elif not s:
                            if j + 1 < len(lines) and lines[j + 1].strip().startswith("|"):
                                continue
                            break
                        else:
                            break
                    if block_rows:
                        tables.append({
                            "table_number": tnum,
                            "table_title": toc_lookup.get(tnum, mt.get("heading", "")),
                            "format": "pipe",
                            "rows": block_rows,
                        })
                        found_nums.add(tnum)
                        _log(f"  Tables: T{tnum} recovered via content marker")
                    break
            if tnum in found_nums:
                continue

        # Strategy 2: find by heading text (fuzzy match)
        if heading:
            for li in range(len(lines)):
                clean = lines[li].strip().lstrip("#").lstrip().lstrip("*").strip().rstrip("*").strip()
                if fuzz.ratio(clean.lower(), heading) >= 85:
                    text_rows = []
                    for j in range(li, min(li + 40, len(lines))):
                        s = lines[j].strip()
                        if s.startswith("#") and j > li + 1:
                            break
                        if j > li + 1:
                            nl = _try_table_label(s)
                            if nl is not None and nl != tnum:
                                break
                        if s:
                            text_rows.append([s])
                    if len(text_rows) >= 2:
                        tables.append({
                            "table_number": tnum,
                            "table_title": toc_lookup.get(tnum, mt.get("heading", "")),
                            "format": "text",
                            "rows": text_rows,
                        })
                        found_nums.add(tnum)
                        _log(f"  Tables: T{tnum} recovered via config heading match")
                    break

    missed = sorted(toc_nums - found_nums)
    if missed:
        _log(f"  Tables: missed {len(missed)} — T{', T'.join(str(n) for n in missed)}")
    _log(f"  Tables: matched {len(found_nums)}/{len(toc_tables)} from ToC")
    return tables


def extract_authority_entries(all_tables: list[dict], config: dict) -> list[dict]:
    """Extract entry names from authority tables specified in config.

    Config `authority_tables` is a list:
      [{table: "Table 37", page: 76, type: "proficiency"}, ...]

    Uses table_number to find the right parsed table, then extracts
    name-like cells from the first column of data rows.

    Returns list of dicts: {entry_name, entry_type, source_table}"""
    entries = []

    # Config-based authority names (no table source needed)
    for entry_type, names in config.get("authority_names", {}).items():
        for name in names:
            entries.append({
                "entry_name": name.lower().strip(),
                "entry_type": entry_type,
                "source_table": "config",
            })

    authority = config.get("authority_tables", [])
    if not authority:
        return entries

    # Build lookup: table_number → parsed table
    table_lookup = {t["table_number"]: t for t in all_tables}

    # Values that are metadata/headers, not entry names — from config or defaults
    default_skip = [
        "proficiency", "required", "ability", "modifier", "check",
        "# of slots", "relevant", "slots", "na", "special", "none",
        "intelligence", "wisdom", "strength", "dexterity", "charisma",
        "constitution", "roll", "d100", "secondary skill",
    ]
    skip_lower = set(s.lower() for s in config.get("authority_skip_values", default_skip))

    for auth in authority:
        table_name = auth["table"]
        entry_type = auth["type"]

        # Extract table number from "Table 37" → 37
        num_match = re.search(r'\d+', table_name)
        if not num_match:
            _log(f"  Warning: can't parse table number from '{table_name}'")
            continue
        table_num = int(num_match.group())

        parsed = table_lookup.get(table_num)
        if not parsed:
            _log(f"  Warning: {table_name} not found in parsed tables")
            continue

        # Extract name-like values from table cells
        # name_column: restrict to specific column index (e.g. 0 for first col)
        # Default: scan all cells (for multi-column tables like T37)
        name_col = auth.get("name_column")

        for row in parsed["rows"]:
            cells_to_check = [row[name_col]] if name_col is not None and name_col < len(row) else row
            for cell in cells_to_check:
                cell = cell.strip().rstrip("*")
                # Handle <br> joined cells (Marker uses this for multi-line cells)
                for part in re.split(r'<br\s*/?>', cell):
                    part = part.strip().rstrip("*").strip()
                    if not part or len(part) < 3:
                        continue
                    if not part[0].isupper():
                        continue
                    if part.lower() in skip_lower:
                        continue
                    # Skip pure numbers/modifiers/prices
                    cleaned = part.replace("-", "").replace("+", "").replace("–", "").replace(" ", "").replace(",", "")
                    if cleaned.isdigit():
                        continue
                    # Skip price-like values (e.g. "3 sp", "10 gp", "500 gp")
                    if re.match(r'^\d[\d,]*\s*(?:cp|sp|gp|pp|ep|lbs?\.?|ft\.?)$', part, re.IGNORECASE):
                        continue
                    # Skip cells that are just numbers with units
                    if re.match(r'^[\d,./⁄½¼¾\s\-–+*]+(?:\s*(?:cp|sp|gp|pp|ep|lbs?\.?|ft\.?))?$', part):
                        continue
                    # Skip modifier values like "+1", "–2", "0"
                    if re.match(r'^[+\-–]?\d{1,2}$', part):
                        continue
                    # Skip "Table N" references
                    if re.match(r'^Table\s+\d+', part, re.IGNORECASE):
                        continue

                    entries.append({
                        "entry_name": part.lower(),
                        "entry_type": entry_type,
                        "source_table": table_name,
                    })

        _log(f"  Authority: {table_name} ({entry_type}) → {sum(1 for e in entries if e['source_table'] == table_name)} raw entries")

    # Deduplicate
    seen = set()
    unique = []
    for e in entries:
        key = (e["entry_name"], e["entry_type"])
        if key not in seen:
            seen.add(key)
            unique.append(e)

    return unique


def detect_watermarks(page_texts: list[str], threshold: float = 0.3) -> dict[str, int]:
    """Detect watermark lines. Returns {text: count}."""
    line_counts = {}
    for text in page_texts:
        seen = set()
        for line in text.split("\n"):
            stripped = line.strip()
            if stripped and len(stripped) > 2 and stripped not in seen:
                seen.add(stripped)
                line_counts[stripped] = line_counts.get(stripped, 0) + 1
    min_count = max(int(len(page_texts) * threshold), 3)
    return {line: count for line, count in line_counts.items() if count >= min_count}


def config_hash(config: dict) -> str:
    """Hash config dict for cache invalidation."""
    import json
    return hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()[:16]


# ── Store to Bronze ──────────────────────────────────────────────

def store_bronze(filepath: Path, config: dict,
                 page_texts: list[str], page_printed: dict[int, int],
                 markdown: str, toc_sections: list[dict],
                 known_entries: list[dict], spell_list: list[dict],
                 all_tables: list[dict], authority_entries: list[dict],
                 watermarks: dict[str, int]) -> None:
    """Write all raw extraction data to bronze_tabletop schema."""
    conn = duckdb.connect(DB_PATH)
    init_bronze_schema(conn)
    now = datetime.now(timezone.utc)
    sf = filepath.name

    # Delete old data for this file (idempotent re-ingestion)
    for table in ["files", "marker_extractions", "page_texts", "toc_raw",
                   "known_entries_raw", "spell_list_entries", "tables_raw",
                   "authority_table_entries", "watermarks"]:
        conn.execute(f"DELETE FROM bronze_tabletop.{table} WHERE source_file = ?", [sf])

    # Files
    conn.execute(
        "INSERT INTO bronze_tabletop.files VALUES (?, ?, ?, ?, ?)",
        [sf, filepath.stat().st_size, len(page_texts), config_hash(config), now],
    )

    # Marker extraction
    conn.execute(
        "INSERT INTO bronze_tabletop.marker_extractions VALUES (?, ?, ?, ?)",
        [sf, markdown, len(markdown), now],
    )

    # Page texts
    for page_idx, text in enumerate(page_texts):
        printed = page_printed.get(page_idx, page_idx)
        conn.execute(
            "INSERT INTO bronze_tabletop.page_texts VALUES (?, ?, ?, ?)",
            [sf, page_idx, text, printed],
        )

    # ToC
    for section in toc_sections:
        conn.execute(
            "INSERT INTO bronze_tabletop.toc_raw VALUES (?, ?, ?, ?, ?)",
            [sf, section["title"], section["page_start"],
             section["page_end"], section["is_excluded"]],
        )

    # Known entries (full metadata from all index appendixes)
    for entry in known_entries:
        conn.execute(
            "INSERT INTO bronze_tabletop.known_entries_raw VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [sf, entry["entry_name"], entry.get("entry_class"),
             entry.get("entry_level"), entry.get("ref_page"),
             entry.get("source_section"), entry.get("school"),
             entry.get("sphere")],
        )

    # All parsed tables (every table in the document)
    import json as _json
    for tbl in all_tables:
        for row_idx, cells in enumerate(tbl["rows"]):
            conn.execute(
                "INSERT INTO bronze_tabletop.tables_raw VALUES (?, ?, ?, ?, ?, ?)",
                [sf, tbl["table_number"], tbl["table_title"],
                 tbl.get("format", "pipe"), row_idx, _json.dumps(cells)],
            )

    # Authority table entries (proficiencies, equipment from specified tables)
    for entry in authority_entries:
        conn.execute(
            "INSERT INTO bronze_tabletop.authority_table_entries VALUES (?, ?, ?, ?)",
            [sf, entry["entry_name"], entry["entry_type"], entry["source_table"]],
        )

    # Spell list entries (Appendix 1 — with reversible flag)
    for entry in spell_list:
        conn.execute(
            "INSERT INTO bronze_tabletop.spell_list_entries VALUES (?, ?, ?, ?, ?, ?)",
            [sf, entry["entry_name"], entry["entry_class"],
             entry["entry_level"], entry["is_reversible"],
             entry.get("source_section")],
        )

    # Watermarks
    for text, count in watermarks.items():
        conn.execute(
            "INSERT INTO bronze_tabletop.watermarks VALUES (?, ?, ?)",
            [sf, text, count],
        )

    total_table_rows = sum(len(t["rows"]) for t in all_tables)
    conn.close()
    _log(f"  Bronze stored: {len(page_texts)} pages, {len(toc_sections)} ToC, "
         f"{len(known_entries)} index entries, {len(spell_list)} spell list, "
         f"{len(all_tables)} tables ({total_table_rows} rows), "
         f"{len(authority_entries)} authority entries, {len(watermarks)} watermarks")


# ── Pipeline ─────────────────────────────────────────────────────

def extract_pdf(filepath: Path) -> None:
    """Extract raw data from a single PDF into bronze layer."""
    import time
    start = time.time()
    step_start = start

    def step(msg: str) -> None:
        nonlocal step_start
        now = time.time()
        _log(f"  [{now - step_start:.1f}s] {msg}")
        step_start = now

    _log(f"\nBronze: {filepath.name} ({filepath.stat().st_size / 1024 / 1024:.1f} MB)")

    config = load_config(filepath, CONFIGS_DIR)

    # Check if already extracted with same config
    conn = duckdb.connect(DB_PATH)
    init_bronze_schema(conn)
    existing = conn.execute(
        "SELECT config_hash FROM bronze_tabletop.files WHERE source_file = ?",
        [filepath.name],
    ).fetchone()
    current_hash = config_hash(config)
    conn.close()

    if existing and existing[0] == current_hash:
        _log(f"  Bronze: already extracted (config unchanged), skipping")
        return

    # 1. Page texts + printed page numbers
    page_texts, page_printed, total_pages = extract_page_texts(filepath, config)
    step(f"PDF: {total_pages} pages")

    # 2. ToC (sections + tables)
    toc_sections, toc_tables = extract_toc(page_texts, config)
    included = sum(1 for s in toc_sections if not s["is_excluded"])
    excluded = sum(1 for s in toc_sections if s["is_excluded"])
    step(f"ToC: {included} sections, {excluded} excluded, {len(toc_tables)} tables")

    # 3. Marker full document (uses disk cache if available)
    _log("  Marker: extracting full document...")
    markdown = extract_marker_markdown(filepath)
    step(f"Marker doc: {len(markdown):,} chars")

    # 5. Known entries from indexes
    known_entries = extract_known_entries(page_texts, page_printed, toc_sections, config)
    step(f"Known entries: {len(known_entries)}")

    # 6. Spell list entries (Appendix 1 — with italic/reversible info)
    spell_list = extract_spell_list_entries(filepath, page_printed, toc_sections)
    step(f"Spell list: {len(spell_list)} entries")

    # 7. Parse ALL tables from markdown (matched to ToC via page positions)
    all_tables = extract_all_tables(markdown, toc_tables, page_texts, page_printed, config)
    step(f"Tables: {len(all_tables)} parsed")

    # 8. Authority entries from config-specified tables
    authority_entries = extract_authority_entries(all_tables, config)
    step(f"Authority entries: {len(authority_entries)}")

    # 9. Watermarks
    watermarks = detect_watermarks(page_texts)
    step(f"Watermarks: {len(watermarks)}")

    # 10. Store everything
    store_bronze(filepath, config, page_texts, page_printed,
                 markdown, toc_sections, known_entries, spell_list,
                 all_tables, authority_entries, watermarks)
    step("Stored")

    _log(f"  Bronze total: {time.time() - start:.1f}s")


# ── OCR Validation ─────────────────────────────────────────────

OCR_PROMPT = (
    'You are proofreading OCR-scanned text from a tabletop RPG rulebook '
    '(AD&D 2nd Edition).\n\n'
    'Identify ONLY clear OCR errors and misspellings. For each error, '
    'output a JSON object with "wrong" and "correct" fields.\n\n'
    'Rules:\n'
    '- Do NOT flag game terms, archaic spellings, or proper nouns\n'
    '- Do NOT flag markdown formatting, abbreviations, or dice notation\n'
    '- ONLY flag words clearly garbled by OCR (wrong letters, merged words)\n'
    '- Output ONLY a JSON array. Empty array [] if no errors.\n\n'
    'Example: [{{"wrong": "Tumans", "correct": "Humans"}}]\n\n'
    'Text:\n---\n{text}\n---\n\nJSON array:'
)


def _call_ollama(prompt: str, config: dict) -> str | None:
    """Call Ollama API. URL and model from config or defaults."""
    import requests
    url = config.get("ollama_url", "http://host.docker.internal:11434")
    model = config.get("ollama_model", "llama3:70b")
    try:
        resp = requests.post(
            f"{url}/api/generate",
            json={"model": model, "prompt": prompt, "stream": False,
                  "options": {"temperature": 0.0, "num_predict": 500}},
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json().get("response", "").strip()
    except Exception as e:
        _log(f"  Ollama error: {e}")
        return None


def _parse_ocr_response(text: str) -> list[dict]:
    """Extract JSON array from LLM response."""
    if not text:
        return []
    start = text.find("[")
    end = text.rfind("]")
    if start < 0 or end < 0:
        return []
    try:
        return json.loads(text[start:end + 1])
    except json.JSONDecodeError:
        return []


def check_ocr(source_file: str, sample: int = 0) -> None:
    """Bronze validation: scan markdown for OCR issues using Ollama.

    Reads already-extracted markdown, applies existing content_substitutions,
    sends text chunks to LLM, stores findings in bronze_tabletop.ocr_issues.
    """
    conn = duckdb.connect(DB_PATH)
    init_bronze_schema(conn)

    configs_dir = DOCUMENTS_DIR.parent / "configs"
    config = load_config(Path(source_file), configs_dir)

    row = conn.execute(
        f"SELECT markdown_text FROM bronze_tabletop.marker_extractions "
        f"WHERE source_file = ?", [source_file]
    ).fetchone()
    if not row:
        _log(f"No markdown found for {source_file}")
        return

    md = _clean_marker_md(row[0])

    # Apply existing substitutions — don't re-flag known issues
    for sub in config.get("content_substitutions", []):
        if len(sub) == 2:
            md = md.replace(sub[0], sub[1])

    # Chunk into ~3000 char segments, skip table-heavy chunks
    paragraphs = md.split("\n\n")
    chunks = []
    current = ""
    for para in paragraphs:
        if len(current) + len(para) + 2 > 3000 and current:
            chunks.append(current)
            current = para
        else:
            current = current + "\n\n" + para if current else para
    if current:
        chunks.append(current)

    text_chunks = []
    for chunk in chunks:
        pipe_lines = sum(1 for line in chunk.split("\n") if line.strip().startswith("|"))
        total_lines = max(1, len(chunk.split("\n")))
        if pipe_lines / total_lines <= 0.5:
            text_chunks.append(chunk)

    if sample and sample < len(text_chunks):
        import random
        random.seed(42)
        text_chunks = random.sample(text_chunks, sample)

    _log(f"OCR check: {source_file} — {len(text_chunks)} text chunks"
         f"{f' (sampled from {len(chunks)})' if sample else ''}")

    # Clear previous issues for this file
    conn.execute(
        "DELETE FROM bronze_tabletop.ocr_issues WHERE source_file = ?",
        [source_file]
    )

    all_errors = {}
    from datetime import datetime
    now = datetime.now()

    for i, chunk in enumerate(text_chunks):
        prompt = OCR_PROMPT.format(text=chunk)
        print(f"  Chunk {i + 1}/{len(text_chunks)}...", end=" ", flush=True)
        response = _call_ollama(prompt, config)
        errors = _parse_ocr_response(response)

        if errors:
            for err in errors:
                wrong = err.get("wrong", "")
                correct = err.get("correct", "")
                if wrong and correct and wrong != correct and wrong not in all_errors:
                    # Find context line
                    ctx = ""
                    for line in md.split("\n"):
                        if wrong in line:
                            ctx = line.strip()[:200]
                            break
                    all_errors[wrong] = (correct, ctx)
            print(f"{len(errors)} issues")
        else:
            print("clean")

    # Store in bronze table
    for wrong, (correct, ctx) in all_errors.items():
        conn.execute(
            "INSERT INTO bronze_tabletop.ocr_issues VALUES (?, ?, ?, ?, 'pending', ?)",
            [source_file, wrong, correct, ctx, now]
        )

    _log(f"  OCR check complete: {len(all_errors)} issues stored in bronze_tabletop.ocr_issues")


def run(directory: Path | None = None) -> None:
    """Extract all PDFs in directory to bronze layer."""
    doc_dir = directory or DOCUMENTS_DIR
    files = sorted(doc_dir.glob("*.pdf"))
    if not files:
        _log(f"No PDFs in {doc_dir}")
        return
    _log(f"Bronze: {len(files)} PDFs ({sum(f.stat().st_size for f in files) / 1024 / 1024:.1f} MB)")
    for f in files:
        extract_pdf(f)
    _log(f"\nBronze done: {len(files)} files")


if __name__ == "__main__":
    import sys
    if "--check-ocr" in sys.argv:
        # Usage: python -m dlt.bronze_tabletop_rules --check-ocr Player [--sample 50]
        args = [a for a in sys.argv[1:] if a != "--check-ocr"]
        book_filter = next((a for a in args if not a.startswith("--")), None)
        sample_idx = next((i for i, a in enumerate(args) if a == "--sample"), None)
        sample_n = int(args[sample_idx + 1]) if sample_idx is not None else 0

        conn = duckdb.connect(DB_PATH)
        init_bronze_schema(conn)
        query = "SELECT DISTINCT source_file FROM bronze_tabletop.marker_extractions"
        if book_filter:
            query += f" WHERE source_file LIKE '%{book_filter}%'"
        files = conn.execute(query).fetchall()
        conn.close()
        for (sf,) in files:
            check_ocr(sf, sample=sample_n)
    elif len(sys.argv) > 1:
        extract_pdf(Path(sys.argv[1]))
    else:
        run()
