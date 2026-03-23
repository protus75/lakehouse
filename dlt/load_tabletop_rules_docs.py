"""
ToC-first PDF ingestion for tabletop RPG rule books.

- pymupdf: page numbers → ToC chapter assignment (truth)
- Marker: text extraction as markdown (handles columns, tables, headings)
- Merge: tag each page's Marker content with its chapter from pymupdf

No heuristic heading detection. Marker's # headings are used directly.
"""

import re
from pathlib import Path
from datetime import datetime, timezone

import duckdb
import fitz  # pymupdf — page numbers only
import yaml

DB_PATH = "/workspace/db/lakehouse.duckdb"
DOCUMENTS_DIR = Path("/workspace/documents/tabletop_rules/raw")
CONFIGS_DIR = Path("/workspace/documents/tabletop_rules/configs")


# ── Config ───────────────────────────────────────────────────────

def load_config(filepath: Path) -> dict:
    default_path = CONFIGS_DIR / "_default.yaml"
    book_path = CONFIGS_DIR / f"{filepath.stem}.yaml"
    config = {}
    if default_path.exists():
        with open(default_path) as f:
            config = yaml.safe_load(f) or {}
    if book_path.exists():
        with open(book_path) as f:
            book = yaml.safe_load(f) or {}
        config = _deep_merge(config, book)
        print(f"  Config: {book_path.name}", flush=True)
    else:
        print(f"  Config: defaults", flush=True)
    return config


def _deep_merge(base: dict, override: dict) -> dict:
    result = dict(base)
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


# ── Schema ───────────────────────────────────────────────────────

def init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS documents_tabletop_rules")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS documents_tabletop_rules.toc (
            toc_id          INTEGER PRIMARY KEY,
            source_file     VARCHAR NOT NULL,
            title           VARCHAR NOT NULL,
            page_start      INTEGER NOT NULL,
            page_end        INTEGER,
            is_excluded     BOOLEAN DEFAULT FALSE,
            sub_headings    VARCHAR,
            tables          VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS documents_tabletop_rules.chunks (
            chunk_id        INTEGER PRIMARY KEY,
            source_file     VARCHAR NOT NULL,
            toc_id          INTEGER,
            section_title   VARCHAR,
            entry_title     VARCHAR,
            content         VARCHAR NOT NULL,
            page_numbers    VARCHAR NOT NULL,
            char_count      INTEGER NOT NULL,
            chunk_type      VARCHAR DEFAULT 'content',
            parsed_at       TIMESTAMP NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS documents_tabletop_rules.files (
            source_file     VARCHAR PRIMARY KEY,
            document_title  VARCHAR,
            game_system     VARCHAR,
            content_type    VARCHAR,
            total_chunks    INTEGER NOT NULL,
            total_toc_entries INTEGER NOT NULL,
            parsed_at       TIMESTAMP NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS documents_tabletop_rules.known_entries (
            source_file     VARCHAR NOT NULL,
            entry_name      VARCHAR NOT NULL
        )
    """)


# ── PDF Cache ────────────────────────────────────────────────────

class PDFCache:
    """Read the PDF once, cache page texts and printed page numbers."""

    def __init__(self, filepath: Path, config: dict):
        page_pattern = config.get("toc", {}).get("page_number_pattern", r"^\d{1,3}$")
        doc = fitz.open(str(filepath))
        self.total_pages = len(doc)
        self.page_texts = []
        self.page_printed = {}

        for page_idx in range(self.total_pages):
            text = doc[page_idx].get_text("text")
            self.page_texts.append(text)
            # Extract printed page number
            printed = page_idx
            for line in reversed(text.split("\n")):
                stripped = line.strip()
                if stripped and re.match(page_pattern, stripped):
                    printed = int(re.search(r"\d+", stripped).group())
                    break
            else:
                for line in text.split("\n")[:5]:
                    stripped = line.strip()
                    if stripped and re.match(page_pattern, stripped):
                        printed = int(re.search(r"\d+", stripped).group())
                        break
            self.page_printed[page_idx] = printed

        doc.close()
        print(f"  PDF cache: {self.total_pages} pages read", flush=True)


# ── Step 1: Parse ToC ────────────────────────────────────────────

def _extract_toc_line(line: str) -> tuple[str, int] | None:
    """Extract (title, page_number) from a ToC line using string ops.
    ToC lines look like: 'Chapter 1: Introduction ......... 5'
    Returns None if not a valid ToC line."""
    stripped = line.strip()
    if not stripped or len(stripped) < 5:
        return None

    # Page number must be at the end — find trailing digits
    rstripped = stripped.rstrip()
    i = len(rstripped) - 1
    while i >= 0 and rstripped[i].isdigit():
        i -= 1
    if i >= len(rstripped) - 1:
        return None  # no trailing number
    page_str = rstripped[i + 1:]
    if not page_str or int(page_str) == 0:
        return None

    # Title is everything before dot leaders / whitespace + page number
    before = rstripped[:i + 1].rstrip()
    # Strip dot leaders: sequences of dots, spaces, dots
    while before and before[-1] in '.… ':
        before = before.rstrip('.… ')
    title = before.strip()
    if not title:
        return None

    return (title, int(page_str))


def parse_toc(pdf: PDFCache, config: dict) -> dict:
    toc_config = config.get("toc", {})
    chapter_patterns = [re.compile(p, re.IGNORECASE) for p in toc_config.get("chapter_patterns", [])]
    table_pattern = re.compile(toc_config["table_pattern"], re.IGNORECASE) if toc_config.get("table_pattern") else None
    scan_pages = toc_config.get("toc_scan_pages", 15)
    exclude_set = set(t.lower() for t in config.get("exclude_chapters", []))

    _log(f"  ToC: scanning {scan_pages} pages with {len(chapter_patterns)} patterns")

    sections = []
    tables = []
    seen = set()

    for page_idx in range(min(scan_pages, pdf.total_pages)):
        text = pdf.page_texts[page_idx]
        for line in text.split("\n"):
            parsed = _extract_toc_line(line)
            if not parsed:
                continue
            title, page = parsed

            for pat in chapter_patterns:
                if pat.match(title):
                    if title not in seen:
                        seen.add(title)
                        sections.append({
                            "title": title, "page_start": page,
                            "is_excluded": title.lower() in exclude_set,
                            "sub_headings": [], "tables": [],
                        })
                    break
            if table_pattern and table_pattern.match(title):
                if title not in seen:
                    seen.add(title)
                    tables.append({"title": title, "page_number": page})

    sections.sort(key=lambda e: e["page_start"])
    for i, entry in enumerate(sections):
        entry["page_end"] = sections[i + 1]["page_start"] - 1 if i + 1 < len(sections) else 9999

    for table in tables:
        for section in sections:
            if section["page_start"] <= table["page_number"] <= section["page_end"]:
                section["tables"].append(table["title"])
                break

    included = sum(1 for e in sections if not e["is_excluded"])
    excluded = sum(1 for e in sections if e["is_excluded"])
    print(f"  ToC: {included} sections, {excluded} excluded, {len(tables)} tables", flush=True)
    return {"sections": sections, "tables": tables}


# ── Step 2: Marker extraction ───────────────────────────────────

_marker_models = None

def _get_marker_models():
    global _marker_models
    if _marker_models is None:
        from marker.models import create_model_dict
        _marker_models = create_model_dict()
    return _marker_models


MARKER_CACHE_DIR = Path("/workspace/cache/marker")


def extract_marker_markdown(filepath: Path) -> str:
    """Run Marker to get full document markdown. No page splitting.
    Caches result to disk so re-ingestion skips the slow Marker step."""
    MARKER_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = MARKER_CACHE_DIR / f"{filepath.stem}.md"

    if cache_path.exists():
        print(f"  Marker: using cached {cache_path.name}", flush=True)
        md = cache_path.read_text(encoding="utf-8")
    else:
        from marker.converters.pdf import PdfConverter
        models = _get_marker_models()
        converter = PdfConverter(artifact_dict=models)
        rendered = converter(str(filepath))
        md = rendered.markdown
        cache_path.write_text(md, encoding="utf-8")
        print(f"  Marker: cached to {cache_path.name}", flush=True)

    # Strip image references
    md = re.sub(r"!\[.*?\]\(.*?\)", "", md)
    # Rejoin hyphenated words
    md = re.sub(r"(\w)-\s*\n\s*(\w)", r"\1\2", md)

    return md


# ── Step 3: Merge Marker headings with chapter assignments ──────

def _detect_watermarks(pdf: PDFCache, threshold: float = 0.3) -> set[str]:
    """Detect watermark lines from cached page text."""
    line_counts = {}
    for text in pdf.page_texts:
        seen = set()
        for line in text.split("\n"):
            stripped = line.strip()
            if stripped and len(stripped) > 2 and stripped not in seen:
                seen.add(stripped)
                line_counts[stripped] = line_counts.get(stripped, 0) + 1
    min_count = max(int(pdf.total_pages * threshold), 3)
    watermarks = {line for line, count in line_counts.items() if count >= min_count}
    if watermarks:
        print(f"  Watermarks: {len(watermarks)} patterns detected", flush=True)
    return watermarks


def _normalize_toc_title(title: str) -> str:
    """Strip 'Chapter X:', 'Appendix X:' prefixes for flexible heading matching."""
    cleaned = re.sub(
        r'^(?:Chapter|Appendix|Part|Section)\s+\d*\s*:?\s*',
        '', title, flags=re.IGNORECASE,
    )
    return cleaned.strip().lower()


def _toc_title_matches(heading: str, normalized_toc: str, full_toc_lower: str) -> bool:
    """Check if a markdown heading matches a ToC section title.

    Handles Marker splitting chapter headings (e.g. "Chapter 5" separate from
    "Proficiencies"), OCR mangling, and partial matches.
    Guards against false positives from short titles appearing inside long headings."""
    if not heading or len(heading) < 3:
        return False

    # Short headings (< 50 chars) can match by containment in either direction,
    # but the match must be a significant portion to avoid false positives
    # like "experience" matching inside a 200-char heading about proficiency levels
    if normalized_toc and len(normalized_toc) > 3:
        if heading == normalized_toc:
            return True
        if normalized_toc in heading:
            # normalized_toc must be at least 40% of the heading to avoid false matches
            if len(normalized_toc) >= len(heading) * 0.4:
                return True
        if heading in normalized_toc:
            if len(heading) >= len(normalized_toc) * 0.4:
                return True

    if heading == full_toc_lower:
        return True
    if heading in full_toc_lower and len(heading) >= len(full_toc_lower) * 0.3:
        return True

    # Match "Chapter N" / "Appendix N" prefix — Marker often splits these
    full_words = full_toc_lower.split()
    heading_words = heading.split()
    if len(heading_words) >= 2 and len(full_words) >= 2:
        if heading_words[0] == full_words[0] and heading_words[1].rstrip(":") == full_words[1].rstrip(":"):
            return True

    return False


def build_heading_chapter_map(
    markdown: str,
    toc_sections: list[dict],
    pdf: PDFCache,
) -> dict[int, dict]:
    """Map heading positions in markdown to ToC chapters using a state machine.

    Walks Marker headings sequentially and matches against ToC sections in order.
    The ToC determines chapter assignment — no brittle forward-only page text search.
    Page numbers are resolved by targeted search within the matched section's page range.

    Returns {char_position_in_markdown: {"toc_entry": section_dict, "page": page_idx}}."""
    included = [s for s in toc_sections if not s["is_excluded"]]
    if not included:
        return {}

    normalized_titles = [_normalize_toc_title(s["title"]) for s in included]

    page_texts_lower = [t.lower() for t in pdf.page_texts]

    # Precompute which page_idxs belong to each section (by printed page range)
    section_page_idxs = {}
    for s in included:
        section_page_idxs[s["title"]] = [
            idx for idx, printed in pdf.page_printed.items()
            if s["page_start"] <= printed <= s["page_end"]
        ]

    # State machine: walk headings, advance through ToC
    toc_idx = 0
    heading_chapters = {}
    last_page = 0

    for m in re.finditer(r"^(#{1,4})\s+(.+)", markdown, re.MULTILINE):
        level = len(m.group(1))
        heading = re.sub(r"\*+", "", m.group(2)).strip()
        heading_lower = heading.lower()
        heading_clean = re.sub(r"\s*\([\w/,\s]+\)\s*$", "", heading_lower).strip()

        if len(heading_clean) < 3:
            continue

        # Advance ToC state: H1/H2 match any section title, H3/H4 only match
        # chapter/appendix identifiers (Marker sometimes renders these at H3+)
        is_chapter_id = heading_clean.startswith(("chapter", "appendix", "part", "section"))
        if level <= 2 or is_chapter_id:
            for i in range(toc_idx, len(included)):
                if _toc_title_matches(heading_clean, normalized_titles[i], included[i]["title"].lower()):
                    toc_idx = i
                    break

        if toc_idx < len(included):
            current_section = included[toc_idx]

            # Resolve page number within section's page range
            page = last_page
            for page_idx in section_page_idxs.get(current_section["title"], []):
                if page_idx >= last_page and heading_clean in page_texts_lower[page_idx]:
                    page = page_idx
                    break

            last_page = page
            heading_chapters[m.start()] = {"toc_entry": current_section, "page": page}

    print(f"  Heading-chapter map: {len(heading_chapters)} headings mapped (state machine)", flush=True)
    return heading_chapters


# ── Step 4: Build entries from Marker headings ───────────────────

def _should_strip_line(stripped: str, config: dict) -> bool:
    """Check if a line should be stripped from entry content based on config patterns.
    Used to remove school/type annotations, tags, etc. that are metadata not content."""
    strip_patterns = config.get("strip_content_patterns", [])
    for pattern in strip_patterns:
        if re.match(pattern, stripped, re.IGNORECASE):
            return True
    return False


def _deduplicate_marker_blocks(content: str, field_names: list[str], config: dict) -> str:
    """Remove duplicate metadata blocks from Marker page-boundary re-renders.

    Marker sometimes re-renders the same entry at a page boundary, producing
    a near-duplicate block with the same metadata fields. The duplicate often
    starts with a mid-word fragment. This function splits on metadata blocks,
    keeps the longest/most complete version, and appends any unique trailing text.
    """
    if not field_names:
        return content

    ingestion = config.get("ingestion", {})
    max_fragment = ingestion.get("max_fragment_length", 60)
    max_interblock_fragment = ingestion.get("max_interblock_fragment_length", 80)
    min_desc_block = ingestion.get("min_description_block", 15)
    dedup_sig_chars = ingestion.get("dedup_signature_chars", 80)

    # Find all positions where a metadata field starts a line
    meta_starts = []
    lines = content.split("\n")
    for i, line in enumerate(lines):
        stripped = line.strip()
        for field in field_names:
            if stripped.lower().startswith(field.lower() + ":"):
                meta_starts.append(i)
                break

    if len(meta_starts) < 2:
        return content

    # Find metadata block boundaries: a block starts at a metadata field
    # and we look for repeated first-field occurrences
    first_field = lines[meta_starts[0]].strip().split(":")[0].strip().lower()
    block_starts = []
    for i in meta_starts:
        stripped = lines[i].strip()
        if stripped.split(":")[0].strip().lower() == first_field:
            block_starts.append(i)

    if len(block_starts) < 2:
        return content

    # Split into: heading (before first block), then each metadata block
    # Strip any mid-word junk between heading and first metadata block
    heading_lines = []
    for line in lines[:block_starts[0]]:
        stripped = line.strip()
        if stripped and stripped[0].islower() and len(stripped) < max_fragment:
            continue  # mid-word fragment or junk preamble
        heading_lines.append(line)
    blocks = []
    for idx, start in enumerate(block_starts):
        end = block_starts[idx + 1] if idx + 1 < len(block_starts) else len(lines)
        block_lines = lines[start:end]

        # Separate metadata lines from description lines
        meta_end = 0
        for j, line in enumerate(block_lines):
            stripped = line.strip()
            if any(stripped.lower().startswith(f.lower() + ":")
                   for f in field_names):
                meta_end = j + 1

        meta = "\n".join(block_lines[:meta_end])
        # Strip preamble junk between blocks (mid-word fragments, duplicate headings)
        desc_lines = block_lines[meta_end:]
        # Remove leading blank lines and lines starting with lowercase (mid-word fragments)
        while desc_lines:
            stripped = desc_lines[0].strip()
            if not stripped:
                desc_lines.pop(0)
            elif stripped[0].islower() and len(stripped) < max_interblock_fragment:
                desc_lines.pop(0)
            else:
                break
        desc = "\n".join(desc_lines).strip()
        blocks.append({"meta": meta, "desc": desc})

    # Keep the block with the most complete metadata
    best = max(blocks, key=lambda b: len(b["meta"]))

    # Collect unique descriptions from all blocks
    all_descs = []
    seen = set()
    for block in blocks:
        if block["desc"] and len(block["desc"]) > min_desc_block:
            sig = block["desc"][:dedup_sig_chars]
            if sig not in seen:
                seen.add(sig)
                all_descs.append(block["desc"])

    # Reassemble: heading + best metadata + all unique descriptions
    result = "\n".join(heading_lines).rstrip()
    if result and best["meta"]:
        result += "\n" + best["meta"]
    elif best["meta"]:
        result = best["meta"]

    for desc in all_descs:
        result += "\n\n" + desc

    return result.strip()


def _clean_entry_content(content: str, config: dict) -> str:
    """Clean entry content at ingestion time.

    Fixes Marker artifacts so stored data is clean:
    - Deduplicate metadata blocks from page-boundary re-renders
    - Split smashed metadata fields onto separate lines
    - Strip leading spaces from all lines
    - Clean partial image references
    - Collapse excessive blank lines
    """
    field_names = config.get("metadata_field_names", [])
    field_names_lower = [f.lower() for f in field_names]

    lines = []
    for line in content.split("\n"):
        stripped = line.lstrip()
        # Skip image artifacts
        if stripped.startswith("![") or (stripped and stripped[0].islower() and stripped.endswith((".jpeg", ".png", ".jpeg)", ".png)"))):
            continue

        # Split smashed metadata fields onto separate lines
        # e.g. "Sphere: All Range: 60 yds." → "Sphere: All\nRange: 60 yds."
        if field_names:
            for fname in field_names:
                # Find field name preceded by space (smashed onto previous field's line)
                idx = stripped.find(" " + fname + ":")
                if idx < 0:
                    idx = stripped.find("  " + fname + ":")
                if idx > 0:
                    lines.append(stripped[:idx].rstrip())
                    stripped = stripped[idx:].lstrip()

        # Split description smashed after metadata value
        # e.g. "Saving Throw: Special  This spell is..." → two lines
        if field_names and "  " in stripped:
            for fname in field_names:
                if stripped.lower().startswith(fname.lower() + ":"):
                    # Find double-space followed by uppercase letter
                    pos = stripped.find("  ", len(fname) + 1)
                    while pos > 0:
                        after = stripped[pos:].lstrip()
                        if after and after[0].isupper():
                            lines.append(stripped[:pos].rstrip())
                            stripped = after
                            break
                        pos = stripped.find("  ", pos + 2)
                    break

        lines.append(stripped)

    content = "\n".join(lines)
    content = re.sub(r"\n{3,}", "\n\n", content)

    # Apply config-driven substitutions (OCR artifact fixes)
    for sub in config.get("content_substitutions", []):
        if len(sub) == 2:
            content = content.replace(sub[0], sub[1])

    # Deduplicate after all other cleaning
    content = _deduplicate_marker_blocks(content, field_names, config)

    return content.strip()


def _has_metadata_but_no_description(content: str, config: dict) -> bool:
    """Check if entry has metadata fields but little/no description after them."""
    field_names = config.get("metadata_field_names", [])
    if not field_names:
        return False
    last_meta_pos = -1
    for field in field_names:
        idx = content.lower().rfind(field.lower() + ":")
        if idx > last_meta_pos:
            last_meta_pos = idx
    if last_meta_pos < 0:
        return False
    min_desc = config.get("validation", {}).get("min_description_chars", 20)
    after_meta = content[last_meta_pos:].split("\n", 1)
    return len(after_meta) < 2 or len(after_meta[1].strip()) < min_desc


def _is_orphan_continuation(content: str) -> bool:
    """Check if entry content looks like an orphan continuation (split from previous entry).
    Looks past the heading line to check if the first body line starts with lowercase/mid-word."""
    lines = [l for l in content.split("\n") if l.strip()]
    if not lines:
        return False
    first = lines[0].strip()
    if first.startswith("#"):
        for line in lines[1:]:
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                return stripped[0].islower()
        return False
    return bool(first) and first[0].islower()


def _merge_orphan_entries(entries: list[dict], config: dict) -> list[dict]:
    """Merge duplicate and orphan entries caused by Marker page-boundary splits.

    Two passes:
    1. Group-merge: entries with same (toc_title, entry_title) where later fragments
       start with lowercase/mid-word text get folded into the first occurrence.
       Handles Marker duplicating headings across page boundaries.
    2. Hungry+orphan: entries with metadata but no description absorb the next
       orphan continuation entry (different title, starts lowercase).
    """
    if not config or len(entries) < 2:
        return entries

    # Pass 1: merge same-title fragments into first occurrence
    primary = {}  # (toc_title, entry_title) -> index in merged list
    merged = []
    fold_count = 0

    for entry in entries:
        title = entry.get("entry_title")
        toc_title = entry["toc_entry"]["title"]
        key = (toc_title, title) if title else None

        if key and key in primary:
            if _is_orphan_continuation(entry["content"]):
                # Strip the duplicate heading, append content to primary
                orphan_content = entry["content"]
                orphan_content = re.sub(r"^#{1,4}\s+.+\n?", "", orphan_content).strip()
                if orphan_content:
                    merged[primary[key]]["content"] += "\n\n" + orphan_content
                    fold_count += 1
                continue

        idx = len(merged)
        merged.append(dict(entry))
        if key and key not in primary:
            primary[key] = idx

    # Pass 2: hungry + orphan (different titles, consecutive)
    if len(merged) < 2:
        if fold_count:
            print(f"  Orphan merges: {fold_count} fragments folded", flush=True)
        return merged

    result = []
    hungry_count = 0
    i = 0
    while i < len(merged):
        entry = dict(merged[i])

        while i + 1 < len(merged):
            next_entry = merged[i + 1]
            if entry["toc_entry"] != next_entry["toc_entry"]:
                break
            if not _has_metadata_but_no_description(entry["content"], config):
                break
            if not _is_orphan_continuation(next_entry["content"]):
                break
            orphan_content = next_entry["content"]
            orphan_content = re.sub(r"^#{1,4}\s+.+\n?", "", orphan_content).strip()
            if orphan_content:
                entry["content"] = entry["content"] + "\n\n" + orphan_content
            hungry_count += 1
            i += 1

        result.append(entry)
        i += 1

    total = fold_count + hungry_count
    if total:
        print(f"  Orphan merges: {fold_count} fragments folded, {hungry_count} descriptions recovered", flush=True)
    return result


def build_entries(
    markdown: str,
    heading_chapter_map: dict[int, dict],
    known_entries: set[str],
    config: dict = None,
) -> list[dict]:
    """Parse Marker markdown into entries using heading-chapter map for ToC assignment.

    No page splitting. Marker's continuous markdown is parsed by headings.
    Each heading's chapter comes from heading_chapter_map (built from ToC state machine).
    known_entries whitelist prevents false headings in spell sections."""
    entries = []
    current_toc = None
    current_page = 0
    current_section = None
    current_entry = None
    current_content = []

    def flush():
        nonlocal current_content
        if current_content and current_toc:
            content = "\n".join(current_content).strip()
            if config:
                content = _clean_entry_content(content, config)
            min_content = config.get("ingestion", {}).get("min_entry_content", 10) if config else 10
            if content and len(content) > min_content:
                entries.append({
                    "toc_entry": current_toc,
                    "section_title": current_section,
                    "entry_title": current_entry,
                    "content": content,
                    "page_numbers": [current_page],
                })
        current_content = []

    lines = markdown.split("\n")
    char_pos = 0

    for line in lines:
        h_match = re.match(r"^(#{1,4})\s+(.+)", line)

        if h_match:
            level = len(h_match.group(1))
            heading = h_match.group(2).strip()
            clean_heading = re.sub(r"\*+", "", heading).strip()
            match_name = re.sub(r"\s*\([\w/,\s]+\)\s*$", "", clean_heading).strip()

            # Update chapter from heading-chapter map
            if char_pos in heading_chapter_map:
                hc = heading_chapter_map[char_pos]
                current_toc = hc["toc_entry"]
                current_page = hc["page"]

            if level <= 2:
                # Skip duplicate section headings from Marker page-boundary re-renders
                # e.g. "## Priest Spells" repeated at top of new page mid-entry
                if current_section and clean_heading.lower() == current_section.lower():
                    pass  # same section — keep accumulating current entry
                elif current_entry and current_content and config and _has_metadata_but_no_description("\n".join(current_content), config):
                    # Current entry has metadata but no description yet — the description
                    # likely follows this section heading (Marker inserted it mid-entry).
                    # Update section but don't flush the entry.
                    current_section = clean_heading
                else:
                    flush()
                    current_section = clean_heading
                    current_entry = None
                    current_content = [line]
            else:
                if known_entries and match_name.lower() not in known_entries:
                    current_content.append(line)
                else:
                    flush()
                    current_entry = match_name
                    current_content = [line]
        else:
            stripped = line.strip()
            if re.match(r"^!\[.*\]\(.*\)$", stripped):
                pass
            elif config and stripped and _should_strip_line(stripped, config):
                pass
            else:
                current_content.append(line)

        char_pos += len(line) + 1

    flush()

    if config:
        entries = _merge_orphan_entries(entries, config)

    print(f"  Entries: {len(entries)}", flush=True)
    return entries


# ── Step 5: Extract known entry names from indexes ───────────────

def extract_known_entries(pdf: PDFCache, toc_data: dict, config: dict) -> set[str]:
    """Get valid entry names from excluded index sections.

    Only extracts lines that are actual index entries (name + page number),
    not category headers or other text that lacks a page reference.
    Uses _extract_toc_line for parsing — no regex on content."""
    excluded = [s for s in toc_data["sections"] if s["is_excluded"]]
    if not excluded:
        return set()

    ingestion = config.get("ingestion", {})
    min_idx = ingestion.get("min_index_entry_length", 3)
    max_idx = ingestion.get("max_index_entry_length", 50)

    _log(f"  Known entries: scanning {len(excluded)} excluded sections, {pdf.total_pages} pages")
    names = set()
    for section in excluded:
        for page_idx in range(pdf.total_pages):
            printed = pdf.page_printed.get(page_idx, page_idx)
            if not (section["page_start"] <= printed <= section["page_end"]):
                continue
            text = pdf.page_texts[page_idx]
            for line in text.split("\n"):
                parsed = _extract_toc_line(line)
                if not parsed:
                    continue
                title = parsed[0]
                # Strip trailing parenthetical (school/type annotations)
                paren = title.rfind("(")
                if paren > 0:
                    title = title[:paren].strip()
                if title and min_idx <= len(title) <= max_idx and title[0].isupper():
                    names.add(title.lower())
    if names:
        print(f"  Known entries: {len(names)} from index sections", flush=True)
    return names


# ── Step 6: Collect sub-headings per ToC section ─────────────────

def collect_sub_headings(entries: list[dict], toc_sections: list[dict], config: dict) -> None:
    """Collect entry titles per ToC section for query routing."""
    max_subs = config.get("ingestion", {}).get("max_sub_headings_per_section", 50)
    section_headings = {}
    for entry in entries:
        title = entry["toc_entry"]["title"]
        if title not in section_headings:
            section_headings[title] = []
        et = entry.get("entry_title") or entry.get("section_title")
        if et and et not in section_headings[title]:
            section_headings[title].append(et)

    for section in toc_sections:
        if section["is_excluded"]:
            continue
        section["sub_headings"] = section_headings.get(section["title"], [])[:max_subs]

    total = sum(len(s["sub_headings"]) for s in toc_sections if not s["is_excluded"])
    print(f"  Sub-headings: {total} collected", flush=True)


# ── Step 7: Chunk entries ────────────────────────────────────────

def chunk_entries(entries: list[dict], config: dict) -> list[dict]:
    chunking = config.get("chunking", {})
    max_chars = chunking.get("max_chars", 800)
    overlap = chunking.get("overlap", 200)

    chunks = []
    for entry in entries:
        content = entry["content"]
        toc = entry["toc_entry"]
        page_str = ",".join(str(p) for p in entry["page_numbers"])

        if len(content) <= max_chars:
            chunks.append({
                "toc_entry": toc, "section_title": entry["section_title"],
                "entry_title": entry["entry_title"], "content": content,
                "page_numbers": page_str, "chunk_type": "content",
            })
        else:
            paragraphs = content.split("\n\n")
            current = ""
            for para in paragraphs:
                if len(current) + len(para) + 2 > max_chars and current:
                    chunks.append({
                        "toc_entry": toc, "section_title": entry["section_title"],
                        "entry_title": entry["entry_title"],
                        "content": current.strip(), "page_numbers": page_str,
                        "chunk_type": "content",
                    })
                    overlap_text = current.strip()[-overlap:] if overlap > 0 else ""
                    current = overlap_text + "\n\n" + para if overlap_text else para
                else:
                    current = current + "\n\n" + para if current else para
            if current.strip():
                chunks.append({
                    "toc_entry": toc, "section_title": entry["section_title"],
                    "entry_title": entry["entry_title"],
                    "content": current.strip(), "page_numbers": page_str,
                    "chunk_type": "content",
                })

    print(f"  Chunks: {len(chunks)}", flush=True)
    return chunks


# ── Step 8: Store ────────────────────────────────────────────────

def store(filepath: Path, toc_data: dict, chunks: list[dict],
          known_entries: set[str] | None = None,
          game_system: str | None = None, content_type: str | None = None) -> None:
    conn = duckdb.connect(DB_PATH)
    init_schema(conn)
    now = datetime.now(timezone.utc)
    toc_sections = toc_data["sections"]

    conn.execute("DELETE FROM documents_tabletop_rules.chunks WHERE source_file = ?", [filepath.name])
    conn.execute("DELETE FROM documents_tabletop_rules.toc WHERE source_file = ?", [filepath.name])
    conn.execute("DELETE FROM documents_tabletop_rules.files WHERE source_file = ?", [filepath.name])
    conn.execute("DELETE FROM documents_tabletop_rules.known_entries WHERE source_file = ?", [filepath.name])

    max_toc_id = conn.execute(
        "SELECT COALESCE(MAX(toc_id), 0) FROM documents_tabletop_rules.toc"
    ).fetchone()[0]

    toc_id_map = {}
    for i, entry in enumerate(toc_sections):
        toc_id = max_toc_id + i + 1
        toc_id_map[entry["title"]] = toc_id
        conn.execute(
            """INSERT INTO documents_tabletop_rules.toc
               (toc_id, source_file, title, page_start, page_end,
                is_excluded, sub_headings, tables)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [toc_id, filepath.name, entry["title"], entry["page_start"],
             entry["page_end"], entry["is_excluded"],
             "; ".join(entry.get("sub_headings", [])),
             "; ".join(entry.get("tables", []))],
        )

    max_chunk_id = conn.execute(
        "SELECT COALESCE(MAX(chunk_id), 0) FROM documents_tabletop_rules.chunks"
    ).fetchone()[0]

    for i, chunk in enumerate(chunks):
        toc = chunk["toc_entry"]
        toc_id = toc_id_map.get(toc["title"])
        conn.execute(
            """INSERT INTO documents_tabletop_rules.chunks
               (chunk_id, source_file, toc_id, section_title, entry_title,
                content, page_numbers, char_count, chunk_type, parsed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [max_chunk_id + i + 1, filepath.name, toc_id, chunk.get("section_title"),
             chunk.get("entry_title"), chunk["content"], chunk["page_numbers"],
             len(chunk["content"]), chunk.get("chunk_type", "content"), now],
        )

    conn.execute(
        """INSERT INTO documents_tabletop_rules.files
           (source_file, document_title, game_system, content_type,
            total_chunks, total_toc_entries, parsed_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [filepath.name, filepath.stem, game_system, content_type,
         len(chunks), len(toc_sections), now],
    )

    if known_entries:
        for name in known_entries:
            conn.execute(
                "INSERT INTO documents_tabletop_rules.known_entries (source_file, entry_name) VALUES (?, ?)",
                [filepath.name, name],
            )

    conn.close()
    print(f"  Stored: {len(toc_sections)} ToC, {len(chunks)} chunks, {len(known_entries or [])} known entries", flush=True)


# ── Pipeline ─────────────────────────────────────────────────────

def _log(msg: str) -> None:
    """Print with flush for real-time monitoring in Docker."""
    print(msg, flush=True)


def parse_pdf(filepath: Path, game_system: str | None = None,
              content_type: str | None = None) -> None:
    import time
    start = time.time()
    step_start = start

    def step(msg: str) -> None:
        nonlocal step_start
        now = time.time()
        elapsed = now - step_start
        _log(f"  [{elapsed:.1f}s] {msg}")
        step_start = now

    _log(f"\nParsing {filepath.name} ({filepath.stat().st_size / 1024 / 1024:.1f} MB)")

    config = load_config(filepath)

    # 1. Read PDF once — cache page texts and page numbers
    pdf = PDFCache(filepath, config)
    step("PDF cache ready")

    # 2. Parse ToC
    toc_data = parse_toc(pdf, config)
    step("ToC parsed")

    # 3. Marker extraction (continuous markdown, no page splitting)
    _log("  Marker: extracting...")
    markdown = extract_marker_markdown(filepath)
    step(f"Marker: {len(markdown):,} chars")

    # 4. Detect and strip watermarks from markdown
    watermarks = _detect_watermarks(pdf)
    if watermarks:
        lines = [l for l in markdown.split("\n") if l.strip() not in watermarks]
        markdown = "\n".join(lines)
    step("Watermarks stripped")

    # 5. Map headings to chapters via ToC state machine
    heading_chapter_map = build_heading_chapter_map(markdown, toc_data["sections"], pdf)
    step("Heading-chapter map built")

    # 6. Known entry names from indexes
    known_entries = extract_known_entries(pdf, toc_data, config)
    step(f"Known entries: {len(known_entries)}")

    # 7. Build entries from Marker headings with chapter assignments
    entries = build_entries(markdown, heading_chapter_map, known_entries, config)
    step(f"Entries built: {len(entries)}")

    # 8. Sub-headings for query routing
    collect_sub_headings(entries, toc_data["sections"], config)
    step("Sub-headings collected")

    # 9. Chunk
    chunks = chunk_entries(entries, config)
    step(f"Chunked: {len(chunks)}")

    # 10. Store
    store(filepath, toc_data, chunks, known_entries, game_system, content_type)
    step("Stored")

    _log(f"  Total: {time.time() - start:.1f}s")


def run(game_system: str | None = None, content_type: str | None = None,
        directory: Path | None = None) -> None:
    doc_dir = directory or DOCUMENTS_DIR
    files = sorted(doc_dir.glob("*.pdf"))
    if not files:
        print(f"No PDFs in {doc_dir}", flush=True)
        return
    print(f"Ingesting {len(files)} PDFs ({sum(f.stat().st_size for f in files) / 1024 / 1024:.1f} MB)", flush=True)
    for f in files:
        parse_pdf(f, game_system=game_system, content_type=content_type)
    print(f"\nDone: {len(files)} files", flush=True)


if __name__ == "__main__":
    run(game_system="D&D 2e", content_type="rules")
