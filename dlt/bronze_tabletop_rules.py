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
        CREATE TABLE IF NOT EXISTS bronze_tabletop.pipeline_runs (
            run_id            VARCHAR PRIMARY KEY,
            source_file       VARCHAR NOT NULL,
            step              VARCHAR NOT NULL,
            pipeline_version  VARCHAR,
            config_hash       VARCHAR,
            status            VARCHAR NOT NULL DEFAULT 'running',
            started_at        TIMESTAMP NOT NULL,
            finished_at       TIMESTAMP,
            row_counts        VARCHAR,
            error_message     VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_tabletop.catalog (
            source_file       VARCHAR NOT NULL,
            table_name        VARCHAR NOT NULL,
            row_count         INTEGER NOT NULL,
            refreshed_at      TIMESTAMP NOT NULL,
            run_id            VARCHAR,
            PRIMARY KEY (source_file, table_name)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_tabletop.files (
            source_file     VARCHAR PRIMARY KEY,
            pdf_size_bytes  BIGINT NOT NULL,
            total_pages     INTEGER NOT NULL,
            config_hash     VARCHAR NOT NULL,
            run_id          VARCHAR NOT NULL,
            extracted_at    TIMESTAMP NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_tabletop.marker_extractions (
            source_file     VARCHAR PRIMARY KEY,
            markdown_text   VARCHAR NOT NULL,
            char_count      INTEGER NOT NULL,
            run_id          VARCHAR NOT NULL,
            extracted_at    TIMESTAMP NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_tabletop.page_texts (
            source_file       VARCHAR NOT NULL,
            page_index        INTEGER NOT NULL,
            page_text         VARCHAR NOT NULL,
            printed_page_num  INTEGER,
            run_id            VARCHAR NOT NULL,
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
            run_id          VARCHAR NOT NULL,
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
            sphere          VARCHAR,
            run_id          VARCHAR NOT NULL
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
            run_id            VARCHAR NOT NULL,
            PRIMARY KEY (source_file, table_number, row_index)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_tabletop.authority_table_entries (
            source_file       VARCHAR NOT NULL,
            entry_name        VARCHAR NOT NULL,
            entry_type        VARCHAR NOT NULL,
            source_table      VARCHAR NOT NULL,
            run_id            VARCHAR NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_tabletop.spell_list_entries (
            source_file       VARCHAR NOT NULL,
            entry_name        VARCHAR NOT NULL,
            entry_class       VARCHAR NOT NULL,
            entry_level       INTEGER NOT NULL,
            is_reversible     BOOLEAN NOT NULL,
            source_section    VARCHAR,
            run_id            VARCHAR NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_tabletop.watermarks (
            source_file       VARCHAR NOT NULL,
            watermark_text    VARCHAR NOT NULL,
            occurrence_count  INTEGER NOT NULL,
            run_id            VARCHAR NOT NULL,
            PRIMARY KEY (source_file, watermark_text)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_tabletop.validation_results (
            source_file       VARCHAR NOT NULL,
            check_name        VARCHAR NOT NULL,
            status            VARCHAR NOT NULL,
            message           VARCHAR,
            details           VARCHAR,
            run_id            VARCHAR NOT NULL,
            checked_at        TIMESTAMP NOT NULL,
            PRIMARY KEY (source_file, check_name)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_tabletop.ocr_issues (
            source_file       VARCHAR NOT NULL,
            wrong_text        VARCHAR NOT NULL,
            suggested_fix     VARCHAR NOT NULL,
            context           VARCHAR,
            status            VARCHAR DEFAULT 'candidate',
            model             VARCHAR,
            run_id            VARCHAR NOT NULL,
            checked_at        TIMESTAMP NOT NULL
        )
    """)


# ── Lineage & Catalog ────────────────────────────────────────────

import uuid


def start_run(conn, source_file: str, step: str, config: dict) -> str:
    """Begin a pipeline run. Returns run_id."""
    run_id = str(uuid.uuid4())[:12]
    lineage_cfg = config.get("lineage", {})
    version = lineage_cfg.get("pipeline_version", "unknown")
    c_hash = config_hash(config)
    now = datetime.now(timezone.utc)
    conn.execute(
        "INSERT INTO bronze_tabletop.pipeline_runs VALUES (?, ?, ?, ?, ?, 'running', ?, NULL, NULL, NULL)",
        [run_id, source_file, step, version, c_hash, now]
    )
    return run_id


def finish_run(conn, run_id: str, status: str = "success",
               row_counts: dict | None = None, error: str | None = None) -> None:
    """Complete a pipeline run."""
    now = datetime.now(timezone.utc)
    conn.execute(
        "UPDATE bronze_tabletop.pipeline_runs "
        "SET status = ?, finished_at = ?, row_counts = ?, error_message = ? "
        "WHERE run_id = ?",
        [status, now, json.dumps(row_counts) if row_counts else None, error, run_id]
    )


def refresh_catalog(conn, source_file: str, run_id: str, config: dict) -> None:
    """Snapshot row counts for all bronze tables into the catalog."""
    lineage_cfg = config.get("lineage", {})
    tables = lineage_cfg.get("catalog_tables", [])
    now = datetime.now(timezone.utc)
    for table in tables:
        try:
            row = conn.execute(
                f"SELECT count(*) FROM bronze_tabletop.{table} WHERE source_file = ?",
                [source_file]
            ).fetchone()
            count = row[0] if row else 0
        except Exception:
            count = 0
        conn.execute(
            "DELETE FROM bronze_tabletop.catalog WHERE source_file = ? AND table_name = ?",
            [source_file, table]
        )
        conn.execute(
            "INSERT INTO bronze_tabletop.catalog VALUES (?, ?, ?, ?, ?)",
            [source_file, table, count, now, run_id]
        )


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

    grouped_class_map = config.get("grouped_index_class_map", {})
    index_abbrevs = config.get("spell_index_abbreviations", {})

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
                    max_heading = config.get("spell_list_layout", {}).get("max_grouped_heading_length", 30)
                    if stripped[0].isupper() and "(" not in stripped and len(stripped) < max_heading:
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
                        "entry_class": grouped_class_map.get("by_school", "wizard") if is_school_index else grouped_class_map.get("by_sphere", "priest"),
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
                                for class_name, abbrevs in index_abbrevs.items():
                                    if cls in abbrevs:
                                        entry_class = class_name
                                        break
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
                               toc_sections: list[dict],
                               config: dict | None = None) -> list[dict]:
    """Parse Appendix 1: Spell Lists using pymupdf font info.

    Extracts: name, level, is_reversible (italic), spell_class (wizard/priest).
    Uses font flags to detect italic (reversible) and bold (level headings)."""
    config = config or {}
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
    class_keywords = config.get("spell_class_keywords", {"wizard": ["wizard", "mage"], "priest": ["priest", "cleric"]})
    layout = config.get("spell_list_layout", {})
    col_match_tol = layout.get("column_match_tolerance", 25)
    col_collect_tol = layout.get("column_collect_tolerance", 30)
    skip_patterns = [s.lower() for s in layout.get("skip_patterns", ["order #"])]

    class_headings = []  # (abs_y, class_name)
    level_columns = []   # (abs_y, x, level_num)
    for abs_y, x, text, italic, bold in all_lines:
        if not bold:
            continue
        text_lower = text.lower().strip()
        for class_name, keywords in class_keywords.items():
            if any(kw in text_lower for kw in keywords) and "spell" in text_lower:
                class_headings.append((abs_y, class_name))
                break
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
            if other_y > col_y and abs(other_x - col_x) < col_match_tol:
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
            if abs(round(x) - col_x) > col_collect_tol:
                continue
            if len(text) < 2:
                continue
            text_stripped = text.strip()
            if any(sp in text_stripped.lower() for sp in skip_patterns) or text_stripped.startswith("*"):
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

    # Values that are metadata/headers, not entry names — from config
    skip_lower = set(s.lower() for s in config.get("authority_skip_values", []))
    skip_regexes = [re.compile(p, re.IGNORECASE) for p in config.get("authority_skip_patterns", [])]

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
                    # Skip values matching config patterns (prices, units, modifiers, table refs)
                    if any(rx.match(part) for rx in skip_regexes):
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

def store_bronze(filepath: Path, config: dict, run_id: str,
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
        "INSERT INTO bronze_tabletop.files VALUES (?, ?, ?, ?, ?, ?)",
        [sf, filepath.stat().st_size, len(page_texts), config_hash(config), run_id, now],
    )

    # Marker extraction
    conn.execute(
        "INSERT INTO bronze_tabletop.marker_extractions VALUES (?, ?, ?, ?, ?)",
        [sf, markdown, len(markdown), run_id, now],
    )

    # Page texts
    for page_idx, text in enumerate(page_texts):
        printed = page_printed.get(page_idx, page_idx)
        conn.execute(
            "INSERT INTO bronze_tabletop.page_texts VALUES (?, ?, ?, ?, ?)",
            [sf, page_idx, text, printed, run_id],
        )

    # ToC
    for section in toc_sections:
        conn.execute(
            "INSERT INTO bronze_tabletop.toc_raw VALUES (?, ?, ?, ?, ?, ?)",
            [sf, section["title"], section["page_start"],
             section["page_end"], section["is_excluded"], run_id],
        )

    # Known entries (full metadata from all index appendixes)
    for entry in known_entries:
        conn.execute(
            "INSERT INTO bronze_tabletop.known_entries_raw VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [sf, entry["entry_name"], entry.get("entry_class"),
             entry.get("entry_level"), entry.get("ref_page"),
             entry.get("source_section"), entry.get("school"),
             entry.get("sphere"), run_id],
        )

    # All parsed tables (every table in the document)
    for tbl in all_tables:
        for row_idx, cells in enumerate(tbl["rows"]):
            conn.execute(
                "INSERT INTO bronze_tabletop.tables_raw VALUES (?, ?, ?, ?, ?, ?, ?)",
                [sf, tbl["table_number"], tbl["table_title"],
                 tbl.get("format", "pipe"), row_idx, json.dumps(cells), run_id],
            )

    # Authority table entries (proficiencies, equipment from specified tables)
    for entry in authority_entries:
        conn.execute(
            "INSERT INTO bronze_tabletop.authority_table_entries VALUES (?, ?, ?, ?, ?)",
            [sf, entry["entry_name"], entry["entry_type"], entry["source_table"], run_id],
        )

    # Spell list entries (Appendix 1 — with reversible flag)
    for entry in spell_list:
        conn.execute(
            "INSERT INTO bronze_tabletop.spell_list_entries VALUES (?, ?, ?, ?, ?, ?, ?)",
            [sf, entry["entry_name"], entry["entry_class"],
             entry["entry_level"], entry["is_reversible"],
             entry.get("source_section"), run_id],
        )

    # Watermarks
    for text, count in watermarks.items():
        conn.execute(
            "INSERT INTO bronze_tabletop.watermarks VALUES (?, ?, ?, ?)",
            [sf, text, count, run_id],
        )

    # Refresh catalog
    row_counts = {
        "page_texts": len(page_texts), "toc_raw": len(toc_sections),
        "known_entries_raw": len(known_entries), "spell_list_entries": len(spell_list),
        "tables_raw": sum(len(t["rows"]) for t in all_tables),
        "authority_table_entries": len(authority_entries), "watermarks": len(watermarks),
    }
    refresh_catalog(conn, sf, run_id, config)
    conn.close()

    _log(f"  Bronze stored (run {run_id}): {row_counts['page_texts']} pages, "
         f"{row_counts['toc_raw']} ToC, {row_counts['known_entries_raw']} index entries, "
         f"{row_counts['spell_list_entries']} spell list, "
         f"{len(all_tables)} tables ({row_counts['tables_raw']} rows), "
         f"{row_counts['authority_table_entries']} authority, {row_counts['watermarks']} watermarks")


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

    if existing and existing[0] == current_hash:
        conn.close()
        _log(f"  Bronze: already extracted (config unchanged), skipping")
        return

    # Start pipeline run
    run_id = start_run(conn, filepath.name, "extract", config)
    conn.close()
    _log(f"  Run: {run_id}")

    try:
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

        # 4. Known entries from indexes
        known_entries = extract_known_entries(page_texts, page_printed, toc_sections, config)
        step(f"Known entries: {len(known_entries)}")

        # 5. Spell list entries (Appendix 1 — with italic/reversible info)
        spell_list = extract_spell_list_entries(filepath, page_printed, toc_sections, config)
        step(f"Spell list: {len(spell_list)} entries")

        # 6. Parse ALL tables from markdown (matched to ToC via page positions)
        all_tables = extract_all_tables(markdown, toc_tables, page_texts, page_printed, config)
        step(f"Tables: {len(all_tables)} parsed")

        # 7. Authority entries from config-specified tables
        authority_entries = extract_authority_entries(all_tables, config)
        step(f"Authority entries: {len(authority_entries)}")

        # 8. Watermarks
        watermarks = detect_watermarks(page_texts)
        step(f"Watermarks: {len(watermarks)}")

        # 9. Store everything
        store_bronze(filepath, config, run_id, page_texts, page_printed,
                     markdown, toc_sections, known_entries, spell_list,
                     all_tables, authority_entries, watermarks)
        step("Stored")

        # Complete the run
        conn = duckdb.connect(DB_PATH)
        row_counts = {
            "page_texts": len(page_texts), "toc_raw": len(toc_sections),
            "known_entries_raw": len(known_entries), "spell_list_entries": len(spell_list),
            "tables_raw": sum(len(t["rows"]) for t in all_tables),
            "authority_table_entries": len(authority_entries), "watermarks": len(watermarks),
        }
        finish_run(conn, run_id, "success", row_counts)
        conn.close()

    except Exception as e:
        conn = duckdb.connect(DB_PATH)
        finish_run(conn, run_id, "failed", error=str(e))
        conn.close()
        raise

    _log(f"  Bronze total: {time.time() - start:.1f}s")


# ── OCR Validation ─────────────────────────────────────────────

def _unload_ollama_model(model: str, config: dict) -> None:
    """Unload a model from Ollama to free RAM/VRAM."""
    import requests
    url = config.get("ocr_check", {}).get("ollama_url", "http://host.docker.internal:11434")
    try:
        requests.post(f"{url}/api/generate", json={"model": model, "keep_alive": 0}, timeout=30)
        _log(f"  Unloaded model: {model}")
    except Exception:
        pass


def _call_ollama(prompt: str, config: dict, model_override: str | None = None,
                 max_tokens_override: int | None = None) -> str | None:
    """Call Ollama API with retries. All settings from config ocr_check section."""
    import requests
    import time as _time
    ocr_cfg = config.get("ocr_check", {})
    url = ocr_cfg.get("ollama_url", "http://host.docker.internal:11434")
    model = model_override
    if not model:
        raise ValueError("model_override is required — caller must specify which model to use")
    timeout = ocr_cfg.get("timeout", 180)
    retries = ocr_cfg.get("retries", 3)
    temperature = ocr_cfg.get("temperature", 0.0)
    max_tokens = max_tokens_override or ocr_cfg.get("bronze_max_tokens", 500)
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(
                f"{url}/api/generate",
                json={"model": model, "prompt": prompt, "stream": False,
                      "options": {"temperature": temperature, "num_predict": max_tokens}},
                timeout=timeout,
            )
            resp.raise_for_status()
            return resp.json().get("response", "").strip()
        except Exception as e:
            _log(f"  Ollama error (attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                _time.sleep(5)
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


def check_ocr(source_file: str, sample: int = 0, resume: bool = True) -> None:
    """Bronze validation: scan markdown for OCR issues using Ollama.

    Reads already-extracted markdown, applies existing content_substitutions,
    sends text chunks to LLM, stores findings in bronze_tabletop.ocr_issues.
    Supports resume — skips chunks already checked (by hash) in ocr_progress.
    """
    import time as _time
    conn = duckdb.connect(DB_PATH)
    init_bronze_schema(conn)

    # Progress tracking table (no run_id — progress spans multiple runs by design)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_tabletop.ocr_progress (
            source_file   VARCHAR NOT NULL,
            chunk_hash    VARCHAR NOT NULL,
            chunk_index   INTEGER NOT NULL,
            checked_at    TIMESTAMP NOT NULL,
            PRIMARY KEY (source_file, chunk_hash)
        )
    """)

    configs_dir = DOCUMENTS_DIR.parent / "configs"
    config = load_config(Path(source_file), configs_dir)
    run_id = start_run(conn, source_file, "check_ocr", config)

    row = conn.execute(
        "SELECT markdown_text FROM bronze_tabletop.marker_extractions "
        "WHERE source_file = ?", [source_file]
    ).fetchone()
    if not row:
        _log(f"No markdown found for {source_file}")
        return

    md = _clean_marker_md(row[0])

    # Apply existing substitutions — don't re-flag known issues
    for sub in config.get("content_substitutions", []):
        if len(sub) == 2:
            md = md.replace(sub[0], sub[1])

    # Chunk into segments, skip table-heavy chunks
    ocr_cfg = config.get("ocr_check", {})
    chunk_size = ocr_cfg.get("chunk_size", 3000)
    table_threshold = ocr_cfg.get("table_line_threshold", 0.5)

    paragraphs = md.split("\n\n")
    chunks = []
    current = ""
    for para in paragraphs:
        if len(current) + len(para) + 2 > chunk_size and current:
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
        if pipe_lines / total_lines <= table_threshold:
            text_chunks.append(chunk)

    if sample and sample < len(text_chunks):
        import random
        random.seed(42)
        text_chunks = random.sample(text_chunks, sample)

    # Check which chunks already processed (resume support)
    done_hashes = set()
    if resume:
        rows = conn.execute(
            "SELECT chunk_hash FROM bronze_tabletop.ocr_progress WHERE source_file = ?",
            [source_file]
        ).fetchall()
        done_hashes = {r[0] for r in rows}

    chunk_hashes = [hashlib.md5(c.encode()).hexdigest()[:12] for c in text_chunks]
    remaining = [(i, c, h) for i, (c, h) in enumerate(zip(text_chunks, chunk_hashes))
                 if h not in done_hashes]

    total = len(text_chunks)
    skipped = total - len(remaining)

    _log(f"OCR check: {source_file} — {total} text chunks"
         f"{f' (sampled from {len(chunks)})' if sample else ''}"
         f"{f', resuming ({skipped} already done)' if skipped else ''}")

    if not remaining:
        _log("  All chunks already checked. Use --no-resume to rerun.")
        return

    all_errors = {}
    now = datetime.now()
    start_time = _time.time()
    failures = 0
    bronze_prompt = ocr_cfg.get("bronze_prompt",
        "Identify OCR errors. Output JSON array of {{\"wrong\": ..., \"correct\": ...}}.\n\n"
        "Text:\n---\n{text}\n---\n\nJSON array:")
    bronze_model = ocr_cfg.get("bronze_model", "llama3:8b")
    max_consecutive_failures = ocr_cfg.get("max_consecutive_failures", 5)

    _log(f"  Using model: {bronze_model}")

    for seq, (i, chunk, chunk_hash) in enumerate(remaining):
        prompt = bronze_prompt.format(text=chunk)
        elapsed = _time.time() - start_time
        rate = elapsed / max(seq, 1)
        eta_s = rate * (len(remaining) - seq)
        eta_min = eta_s / 60
        print(f"  Chunk {i + 1}/{total} ({seq + 1}/{len(remaining)} remaining, "
              f"ETA {eta_min:.0f}m)... ", end="", flush=True)

        bronze_max_tokens = ocr_cfg.get("bronze_max_tokens", 500)
        response = _call_ollama(prompt, config, model_override=bronze_model,
                                max_tokens_override=bronze_max_tokens)

        if response is None:
            failures += 1
            print(f"FAILED ({failures} consecutive)")
            if failures >= max_consecutive_failures:
                _log(f"  Aborting: {max_consecutive_failures} consecutive failures. "
                     f"Progress saved — rerun to resume.")
                break
            continue

        failures = 0  # reset on success
        errors = _parse_ocr_response(response)

        if errors:
            for err in errors:
                wrong = err.get("wrong", "")
                correct = err.get("correct", "")
                if wrong and correct and wrong != correct and wrong not in all_errors:
                    ctx = ""
                    for line in md.split("\n"):
                        if wrong in line:
                            ctx = line.strip()[:200]
                            break
                    all_errors[wrong] = (correct, ctx)
            print(f"{len(errors)} issues")
        else:
            print("clean")

        # Record progress and store issues incrementally
        conn.execute(
            "INSERT OR REPLACE INTO bronze_tabletop.ocr_progress VALUES (?, ?, ?, ?)",
            [source_file, chunk_hash, i, now]
        )
        for wrong, (correct, ctx) in all_errors.items():
            conn.execute(
                "DELETE FROM bronze_tabletop.ocr_issues "
                "WHERE source_file = ? AND wrong_text = ?",
                [source_file, wrong]
            )
            conn.execute(
                "INSERT INTO bronze_tabletop.ocr_issues VALUES (?, ?, ?, ?, 'candidate', ?, ?, ?)",
                [source_file, wrong, correct, ctx, bronze_model, run_id, now]
            )

    total_time = _time.time() - start_time
    checked = len(remaining) - failures
    _log(f"  OCR check complete: {len(all_errors)} candidates found, "
         f"{checked} chunks checked in {total_time / 60:.1f}m")

    # Finish run, refresh catalog, free model
    finish_run(conn, run_id, "success",
               {"candidates": len(all_errors), "chunks_checked": checked})
    refresh_catalog(conn, source_file, run_id, config)
    _unload_ollama_model(bronze_model, config)


def review_ocr(source_file: str) -> None:
    """Silver pass: review OCR candidates with the large model.

    Reads 'candidate' issues from bronze_tabletop.ocr_issues,
    sends each to the silver model for confirmation, updates status
    to 'confirmed' or 'rejected'.
    """
    import time as _time
    conn = duckdb.connect(DB_PATH)
    init_bronze_schema(conn)

    configs_dir = DOCUMENTS_DIR.parent / "configs"
    config = load_config(Path(source_file), configs_dir)
    run_id = start_run(conn, source_file, "review_ocr", config)
    ocr_cfg = config.get("ocr_check", {})
    silver_model = ocr_cfg.get("silver_model", "llama3:70b")
    silver_prompt = ocr_cfg.get("silver_prompt",
        'Review this OCR error: "{wrong}" -> "{correct}". Context: {context}\n'
        'Respond with JSON: {{"verdict": "confirmed" or "rejected", "reason": "brief"}}')

    candidates = conn.execute(
        "SELECT wrong_text, suggested_fix, context FROM bronze_tabletop.ocr_issues "
        "WHERE source_file = ? AND status = 'candidate'",
        [source_file]
    ).fetchall()

    if not candidates:
        _log(f"No OCR candidates to review for {source_file}")
        return

    _log(f"OCR review: {source_file} — {len(candidates)} candidates, using {silver_model}")
    start_time = _time.time()
    confirmed = 0
    rejected = 0
    now = datetime.now()

    for i, (wrong, correct, ctx) in enumerate(candidates):
        prompt = silver_prompt.format(wrong=wrong, correct=correct, context=ctx or "")
        print(f"  [{i + 1}/{len(candidates)}] '{wrong}' -> '{correct}'... ", end="", flush=True)

        silver_max_tokens = ocr_cfg.get("silver_max_tokens", 300)
        response = _call_ollama(prompt, config, model_override=silver_model,
                                max_tokens_override=silver_max_tokens)
        if response is None:
            print("FAILED")
            continue

        # Parse verdict
        verdict = "rejected"
        reason = ""
        try:
            start = response.find("{")
            end = response.rfind("}")
            if start >= 0 and end > start:
                result = json.loads(response[start:end + 1])
                verdict = result.get("verdict", "rejected").lower()
                reason = result.get("reason", "")
        except json.JSONDecodeError:
            if "confirmed" in response.lower():
                verdict = "confirmed"

        if verdict == "confirmed":
            confirmed += 1
            print(f"CONFIRMED — {reason}")
        else:
            rejected += 1
            print(f"rejected — {reason}")

        conn.execute(
            "UPDATE bronze_tabletop.ocr_issues "
            "SET status = ?, model = ?, checked_at = ? "
            "WHERE source_file = ? AND wrong_text = ?",
            [verdict, silver_model, now, source_file, wrong]
        )

    total_time = _time.time() - start_time
    _log(f"  OCR review complete: {confirmed} confirmed, {rejected} rejected "
         f"in {total_time / 60:.1f}m")

    # Finish run, refresh catalog, free model
    finish_run(conn, run_id, "success",
               {"confirmed": confirmed, "rejected": rejected})
    refresh_catalog(conn, source_file, run_id, config)
    _unload_ollama_model(silver_model, config)


# ── Bronze Validation ──────────────────────────────────────────

def _store_validation(conn, source_file: str, check_name: str,
                      status: str, message: str, run_id: str,
                      details: str = "") -> None:
    """Upsert a validation result."""
    now = datetime.now(timezone.utc)
    conn.execute(
        "DELETE FROM bronze_tabletop.validation_results "
        "WHERE source_file = ? AND check_name = ?",
        [source_file, check_name]
    )
    conn.execute(
        "INSERT INTO bronze_tabletop.validation_results VALUES (?, ?, ?, ?, ?, ?, ?)",
        [source_file, check_name, status, message, details, run_id, now]
    )


def validate_bronze(source_file: str) -> None:
    """Run all bronze validation checks for a source file.

    Checks: table completeness, entry coverage, spell cross-check,
    content gaps, duplicate entries. Results stored in
    bronze_tabletop.validation_results.
    """
    conn = duckdb.connect(DB_PATH)
    init_bronze_schema(conn)
    sf = source_file

    configs_dir = DOCUMENTS_DIR.parent / "configs"
    config = load_config(Path(sf), configs_dir)
    run_id = start_run(conn, sf, "validate", config)
    val_cfg = config.get("bronze_validation", {})

    _log(f"Validating bronze: {sf} (run {run_id})")
    passed = 0
    warned = 0
    failed = 0

    # ── 1. Table completeness ──
    toc_tables = conn.execute(
        "SELECT title FROM bronze_tabletop.toc_raw "
        "WHERE source_file = ? AND title LIKE 'Table %'",
        [sf]
    ).fetchall()
    toc_table_nums = set()
    for (title,) in toc_tables:
        m = re.search(r'\d+', title)
        if m:
            toc_table_nums.add(int(m.group()))

    parsed_table_nums = set()
    if toc_table_nums:
        rows = conn.execute(
            "SELECT DISTINCT table_number FROM bronze_tabletop.tables_raw WHERE source_file = ?",
            [sf]
        ).fetchall()
        parsed_table_nums = {r[0] for r in rows}

    if toc_table_nums:
        missing = sorted(toc_table_nums - parsed_table_nums)
        pct = len(parsed_table_nums) / len(toc_table_nums) * 100
        min_pct = val_cfg.get("min_table_match_pct", 80)
        if pct >= min_pct and not missing:
            status, msg = "pass", f"{len(parsed_table_nums)}/{len(toc_table_nums)} tables parsed (100%)"
            passed += 1
        elif pct >= min_pct:
            status, msg = "warn", f"{len(parsed_table_nums)}/{len(toc_table_nums)} tables parsed ({pct:.0f}%), missing: T{', T'.join(str(n) for n in missing)}"
            warned += 1
        else:
            status, msg = "fail", f"Only {pct:.0f}% tables parsed ({len(parsed_table_nums)}/{len(toc_table_nums)}), missing: T{', T'.join(str(n) for n in missing)}"
            failed += 1
        _store_validation(conn, sf, "table_completeness", status, msg, run_id,
                          json.dumps({"missing": missing, "pct": round(pct, 1)}))
        _log(f"  Table completeness: {status} — {msg}")

    # ── 2. Spell index vs spell list cross-check ──
    index_spells = set()
    rows = conn.execute(
        "SELECT entry_name, entry_class FROM bronze_tabletop.known_entries_raw "
        "WHERE source_file = ? AND entry_class IS NOT NULL",
        [sf]
    ).fetchall()
    for name, cls in rows:
        index_spells.add((name.lower().strip(), cls.lower().strip()))

    list_spells = set()
    rows = conn.execute(
        "SELECT entry_name, entry_class FROM bronze_tabletop.spell_list_entries "
        "WHERE source_file = ?",
        [sf]
    ).fetchall()
    for name, cls in rows:
        list_spells.add((name.lower().strip(), cls.lower().strip()))

    if index_spells and list_spells:
        in_index_only = sorted(index_spells - list_spells)
        in_list_only = sorted(list_spells - index_spells)
        max_mismatch = val_cfg.get("max_spell_mismatch", 10)
        total_diff = len(in_index_only) + len(in_list_only)

        if total_diff == 0:
            status, msg = "pass", f"Spell index and list match perfectly ({len(index_spells)} spells)"
            passed += 1
        elif total_diff <= max_mismatch:
            status, msg = "warn", f"{total_diff} mismatches (index-only: {len(in_index_only)}, list-only: {len(in_list_only)})"
            warned += 1
        else:
            status, msg = "fail", f"{total_diff} mismatches (index-only: {len(in_index_only)}, list-only: {len(in_list_only)})"
            failed += 1
        details = json.dumps({
            "index_only": in_index_only[:20],
            "list_only": in_list_only[:20],
            "index_count": len(index_spells),
            "list_count": len(list_spells),
        })
        _store_validation(conn, sf, "spell_cross_check", status, msg, run_id, details)
        _log(f"  Spell cross-check: {status} — {msg}")

    # ── 3. Content gap detection ──
    page_rows = conn.execute(
        "SELECT printed_page_num FROM bronze_tabletop.page_texts "
        "WHERE source_file = ? AND printed_page_num IS NOT NULL "
        "ORDER BY printed_page_num",
        [sf]
    ).fetchall()
    if page_rows:
        pages = [r[0] for r in page_rows]
        max_gap = val_cfg.get("max_page_gap", 5)
        gaps = []
        for j in range(1, len(pages)):
            gap = pages[j] - pages[j - 1]
            if gap > max_gap:
                gaps.append((pages[j - 1], pages[j], gap))

        if not gaps:
            status, msg = "pass", f"No page gaps > {max_gap} in {len(pages)} pages"
            passed += 1
        else:
            status, msg = "warn", f"{len(gaps)} page gaps > {max_gap}: {', '.join(f'p{a}-{b} ({c} pages)' for a, b, c in gaps)}"
            warned += 1
        _store_validation(conn, sf, "content_gaps", status, msg, run_id,
                          json.dumps({"gaps": gaps}))
        _log(f"  Content gaps: {status} — {msg}")

    # ── 4. Duplicate entry detection ──
    sig_chars = val_cfg.get("duplicate_signature_chars", 200)
    toc_rows = conn.execute(
        "SELECT title FROM bronze_tabletop.toc_raw "
        "WHERE source_file = ? AND is_excluded = false",
        [sf]
    ).fetchall()
    # Check known_entries for dupes
    entry_rows = conn.execute(
        "SELECT entry_name, COUNT(*) as cnt FROM bronze_tabletop.known_entries_raw "
        "WHERE source_file = ? GROUP BY entry_name HAVING cnt > 1",
        [sf]
    ).fetchall()
    dupes = [(name, cnt) for name, cnt in entry_rows]

    if not dupes:
        status, msg = "pass", "No duplicate entry names in known_entries"
        passed += 1
    else:
        status, msg = "warn", f"{len(dupes)} duplicate entry names: {', '.join(f'{n}({c}x)' for n, c in dupes[:10])}"
        warned += 1
    _store_validation(conn, sf, "duplicate_entries", status, msg, run_id,
                      json.dumps({"duplicates": dupes[:50]}))
    _log(f"  Duplicate entries: {status} — {msg}")

    # ── 5. Authority entry coverage ──
    authority = conn.execute(
        "SELECT entry_name, entry_type FROM bronze_tabletop.authority_table_entries "
        "WHERE source_file = ?",
        [sf]
    ).fetchall()
    if authority:
        authority_names = {(name.lower(), etype) for name, etype in authority}
        known_names = set()
        for (name,) in conn.execute(
            "SELECT entry_name FROM bronze_tabletop.known_entries_raw WHERE source_file = ?", [sf]
        ).fetchall():
            known_names.add(name.lower())

        not_in_known = [(n, t) for n, t in authority_names if n not in known_names]
        if not not_in_known:
            status, msg = "pass", f"All {len(authority_names)} authority entries found in known_entries"
            passed += 1
        else:
            status, msg = "warn", f"{len(not_in_known)}/{len(authority_names)} authority entries not in known_entries"
            warned += 1
        _store_validation(conn, sf, "authority_coverage", status, msg, run_id,
                          json.dumps({"missing": not_in_known[:30]}))
        _log(f"  Authority coverage: {status} — {msg}")

    status = "success" if failed == 0 else "failed"
    finish_run(conn, run_id, status,
               {"passed": passed, "warned": warned, "failed": failed})
    refresh_catalog(conn, sf, run_id, config)
    conn.close()
    _log(f"  Validation summary: {passed} passed, {warned} warnings, {failed} failed")


def run(directory: Path | None = None, force: bool = False) -> None:
    """Extract new/changed PDFs to bronze layer.

    Change detection: skips a PDF if its file size and config hash
    match what's already stored in bronze_tabletop.files.
    Use force=True to re-extract everything.
    """
    doc_dir = directory or DOCUMENTS_DIR
    files = sorted(doc_dir.glob("*.pdf"))
    if not files:
        _log(f"No PDFs in {doc_dir}")
        return

    conn = duckdb.connect(DB_PATH)
    init_bronze_schema(conn)

    # Load existing extraction state
    existing = {}
    for row in conn.execute("SELECT source_file, pdf_size_bytes, config_hash FROM bronze_tabletop.files").fetchall():
        existing[row[0]] = (row[1], row[2])
    conn.close()

    to_extract = []
    skipped = []
    configs_dir = DOCUMENTS_DIR.parent / "configs"

    for f in files:
        sf = f.name
        current_size = f.stat().st_size
        config = load_config(f, configs_dir)
        current_hash = config_hash(config)

        if not force and sf in existing:
            prev_size, prev_hash = existing[sf]
            if current_size == prev_size and current_hash == prev_hash:
                skipped.append(sf)
                continue

        to_extract.append(f)

    total_mb = sum(f.stat().st_size for f in files) / 1024 / 1024
    _log(f"Bronze: {len(files)} PDFs ({total_mb:.1f} MB), "
         f"{len(to_extract)} new/changed, {len(skipped)} unchanged")

    if not to_extract:
        _log("Nothing to extract.")
        return

    for f in to_extract:
        extract_pdf(f)
    _log(f"\nBronze done: {len(to_extract)} files extracted")


if __name__ == "__main__":
    import sys
    if "--check-ocr" in sys.argv:
        # Usage: python -m dlt.bronze_tabletop_rules --check-ocr Player [--sample 50] [--no-resume]
        args = [a for a in sys.argv[1:] if a != "--check-ocr"]
        book_filter = next((a for a in args if not a.startswith("--")), None)
        sample_idx = next((i for i, a in enumerate(args) if a == "--sample"), None)
        sample_n = int(args[sample_idx + 1]) if sample_idx is not None else 0
        do_resume = "--no-resume" not in sys.argv

        conn = duckdb.connect(DB_PATH)
        init_bronze_schema(conn)
        query = "SELECT DISTINCT source_file FROM bronze_tabletop.marker_extractions"
        if book_filter:
            query += f" WHERE source_file LIKE '%{book_filter}%'"
        files = conn.execute(query).fetchall()
        conn.close()
        for (sf,) in files:
            check_ocr(sf, sample=sample_n, resume=do_resume)
    elif "--review-ocr" in sys.argv:
        # Usage: python -m dlt.bronze_tabletop_rules --review-ocr Player
        args = [a for a in sys.argv[1:] if a != "--review-ocr"]
        book_filter = next((a for a in args if not a.startswith("--")), None)

        conn = duckdb.connect(DB_PATH)
        init_bronze_schema(conn)
        query = "SELECT DISTINCT source_file FROM bronze_tabletop.ocr_issues WHERE status = 'candidate'"
        if book_filter:
            query += f" AND source_file LIKE '%{book_filter}%'"
        files = conn.execute(query).fetchall()
        conn.close()
        for (sf,) in files:
            review_ocr(sf)
    elif "--validate" in sys.argv:
        # Usage: python -m dlt.bronze_tabletop_rules --validate Player
        args = [a for a in sys.argv[1:] if a != "--validate"]
        book_filter = next((a for a in args if not a.startswith("--")), None)

        conn = duckdb.connect(DB_PATH)
        init_bronze_schema(conn)
        query = "SELECT DISTINCT source_file FROM bronze_tabletop.files"
        if book_filter:
            query += f" WHERE source_file LIKE '%{book_filter}%'"
        files = conn.execute(query).fetchall()
        conn.close()
        for (sf,) in files:
            validate_bronze(sf)
    elif "--force" in sys.argv:
        run(force=True)
    elif len(sys.argv) > 1:
        extract_pdf(Path(sys.argv[1]))
    else:
        run()
