"""
ToC-first PDF ingestion for tabletop RPG rule books.

The Table of Contents is the structural authority. Every page is mapped to its
ToC section by reading the printed page number. Every chunk inherits its ToC
section. No guessing, no matching, no offset math.

Flow:
  1. Parse ToC → (title, page_number) pairs
  2. Read each PDF page → printed page number → ToC section
  3. Extract text per page (pymupdf) + full markdown (Marker)
  4. Group pages by ToC section → build entries within each section
  5. Chunk entries → store in DuckDB + ChromaDB
"""

import re
from pathlib import Path
from datetime import datetime, timezone

import duckdb
import fitz  # pymupdf
import yaml

DB_PATH = "/workspace/db/lakehouse.duckdb"
DOCUMENTS_DIR = Path("/workspace/documents/tabletop_rules/raw")
CONFIGS_DIR = Path("/workspace/documents/tabletop_rules/configs")


# ── Config ───────────────────────────────────────────────────────

def load_config(filepath: Path) -> dict:
    """Load YAML config for a PDF. Falls back to _default.yaml."""
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
        print(f"  Config: {book_path.name}")
    else:
        print(f"  Config: defaults (no {filepath.stem}.yaml)")

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
            parent_toc_id   INTEGER,
            depth           INTEGER DEFAULT 0,
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


# ── Step 1: Parse ToC ────────────────────────────────────────────

def parse_toc(filepath: Path, config: dict) -> dict:
    """Extract ToC entries with page numbers from the PDF.
    Returns dict with:
      'sections': list of chapter/appendix dicts (title, page_start, page_end, is_excluded)
      'tables': list of table dicts (title, page_number)
    Tables are parsed separately so they can be cross-referenced to their parent section."""
    toc_config = config.get("toc", {})
    chapter_patterns = toc_config.get("chapter_patterns", [])
    table_pattern = toc_config.get("table_pattern", "")
    scan_pages = toc_config.get("toc_scan_pages", 15)
    exclude_set = set(t.lower() for t in config.get("exclude_chapters", []))

    doc = fitz.open(str(filepath))
    sections = []
    tables = []
    seen = set()

    for page_idx in range(min(scan_pages, len(doc))):
        text = doc[page_idx].get_text("text")
        for line in text.split("\n"):
            stripped = line.strip()

            # Match chapters/appendices
            for pat in chapter_patterns:
                m = re.match(
                    r"(" + pat + r")\s*(?:\.[\s.]*){2,}\s*(\d+)\s*$",
                    stripped, re.IGNORECASE,
                )
                if m:
                    title = re.sub(r"\s*(?:\.[\s.]*){2,}.*", "", m.group(1)).strip()
                    page = int(m.group(2))
                    if title and title not in seen:
                        seen.add(title)
                        sections.append({
                            "title": title,
                            "page_start": page,
                            "is_excluded": title.lower() in exclude_set,
                            "sub_headings": [],
                            "tables": [],
                        })

            # Match tables
            if table_pattern:
                m = re.match(
                    r"(" + table_pattern + r")\s*(?:\.[\s.]*){2,}\s*(\d+)\s*$",
                    stripped, re.IGNORECASE,
                )
                if m:
                    title = re.sub(r"\s*(?:\.[\s.]*){2,}.*", "", m.group(1)).strip()
                    page = int(m.group(2))
                    if title and title not in seen:
                        seen.add(title)
                        tables.append({"title": title, "page_number": page})

    doc.close()

    # Sort sections by page and compute page_end
    sections.sort(key=lambda e: e["page_start"])
    for i, entry in enumerate(sections):
        if i + 1 < len(sections):
            entry["page_end"] = sections[i + 1]["page_start"] - 1
        else:
            entry["page_end"] = 9999

    # Assign each table to its parent ToC section by page number
    for table in tables:
        for section in sections:
            if section["page_start"] <= table["page_number"] <= section["page_end"]:
                section["tables"].append(table["title"])
                table["toc_section"] = section["title"]
                break

    included = sum(1 for e in sections if not e["is_excluded"])
    excluded = sum(1 for e in sections if e["is_excluded"])
    print(f"  ToC: {included} sections, {excluded} excluded, {len(tables)} tables")
    return {"sections": sections, "tables": tables}


# ── Step 2: Map pages to ToC sections ────────────────────────────

def read_page_number(page, page_idx: int, pattern: str = r"^\d{1,3}$") -> int:
    """Read printed page number from a PDF page. Falls back to page_idx."""
    text = page.get_text("text")
    for line in reversed(text.split("\n")):
        stripped = line.strip()
        if stripped and re.match(pattern, stripped):
            return int(re.search(r"\d+", stripped).group())
    for line in text.split("\n")[:5]:
        stripped = line.strip()
        if stripped and re.match(pattern, stripped):
            return int(re.search(r"\d+", stripped).group())
    return page_idx


def build_page_map(filepath: Path, toc_entries: list[dict], config: dict) -> dict:
    """Map each PDF page index to its ToC entry (or None if excluded/unmapped).
    Returns {page_idx: toc_entry_dict or None}."""
    page_pattern = config.get("toc", {}).get("page_number_pattern", r"^\d{1,3}$")

    # Build included ToC ranges only
    included = [e for e in toc_entries if not e["is_excluded"]]

    doc = fitz.open(str(filepath))
    page_map = {}

    for page_idx in range(len(doc)):
        printed_page = read_page_number(doc[page_idx], page_idx, page_pattern)

        # Find which ToC section this page belongs to
        matched = None
        for entry in included:
            if entry["page_start"] <= printed_page <= entry["page_end"]:
                matched = entry
                break

        page_map[page_idx] = matched

    doc.close()
    return page_map


# ── Text cleanup ─────────────────────────────────────────────────

def clean_text(text: str) -> str:
    """Clean up raw pymupdf text extraction artifacts.
    - Rejoin hyphenated words split across lines (larg-\\nest -> largest)
    - Remove isolated single/double character fragments from column bleed
    - Collapse multiple blank lines
    - Strip trailing whitespace per line
    """
    # Rejoin hyphenated line breaks: "word-\n continued" -> "wordcontinued"
    text = re.sub(r"(\w)-\s*\n\s*(\w)", r"\1\2", text)
    # Remove lines that are just 1-2 characters (column bleed artifacts like " M" or "S")
    lines = []
    for line in text.split("\n"):
        stripped = line.strip()
        if stripped and len(stripped) <= 2 and stripped.isalpha():
            continue
        # Remove isolated single chars within a line: "some text  M  more text" -> "some text more text"
        line = re.sub(r"  [A-Z]  ", " ", line)
        # Remove trailing isolated chars: "some text  M"
        line = re.sub(r"\s+[A-Z]\s*$", "", line)
        lines.append(line.rstrip())
    # Collapse multiple blank lines to single
    result = "\n".join(lines)
    result = re.sub(r"\n{3,}", "\n\n", result)
    return result


# ── Step 3: Extract text per page ────────────────────────────────

def detect_watermarks(filepath: Path, threshold: float = 0.3) -> set[str]:
    """Detect watermark text by finding lines that repeat on many pages.
    Any line appearing on more than threshold (default 30%) of pages is a watermark."""
    doc = fitz.open(str(filepath))
    total_pages = len(doc)
    line_counts = {}

    for page_idx in range(total_pages):
        text = doc[page_idx].get_text("text")
        seen_on_page = set()
        for line in text.split("\n"):
            stripped = line.strip()
            if stripped and len(stripped) > 2 and stripped not in seen_on_page:
                seen_on_page.add(stripped)
                line_counts[stripped] = line_counts.get(stripped, 0) + 1

    doc.close()

    min_count = int(total_pages * threshold)
    watermarks = {line for line, count in line_counts.items() if count >= min_count}
    if watermarks:
        print(f"  Watermarks: {len(watermarks)} patterns detected (>{threshold*100:.0f}% of pages)")
    return watermarks


def _extract_page_text_blocks(page) -> str:
    """Extract text from a PDF page using block-level layout analysis.
    Sorts blocks by column (left then right) and vertical position
    to properly handle two-column layouts and tables."""
    blocks = page.get_text("blocks")
    page_width = page.rect.width
    mid_x = page_width / 2

    # Separate text blocks into left and right columns
    left_blocks = []
    right_blocks = []
    for b in blocks:
        if b[6] != 0:  # skip image blocks
            continue
        center_x = (b[0] + b[2]) / 2
        if center_x < mid_x:
            left_blocks.append(b)
        else:
            right_blocks.append(b)

    # Sort each column by vertical position
    left_blocks.sort(key=lambda b: b[1])
    right_blocks.sort(key=lambda b: b[1])

    # Interleave: left column first, then right column
    # This preserves proper reading order for two-column layouts
    all_blocks = left_blocks + right_blocks

    text_parts = []
    for b in all_blocks:
        text_parts.append(b[4].strip())

    return "\n".join(text_parts)


def extract_pages(filepath: Path, page_map: dict, config: dict,
                  watermarks: set[str] | None = None) -> list[dict]:
    """Extract text from each non-excluded page using block-level layout.
    Handles two-column layouts and tables correctly.
    Strips page numbers and watermarks automatically.
    Returns list of {page_idx, page_number, toc_entry, text}."""
    page_pattern = config.get("toc", {}).get("page_number_pattern", r"^\d{1,3}$")
    if watermarks is None:
        watermarks = set()

    doc = fitz.open(str(filepath))
    pages = []

    for page_idx in range(len(doc)):
        toc_entry = page_map.get(page_idx)
        if toc_entry is None:
            continue

        page = doc[page_idx]
        printed_page = read_page_number(page, page_idx, page_pattern)

        # Use block-level extraction for proper column/table handling
        text = _extract_page_text_blocks(page)

        # Strip watermarks and page numbers
        lines = []
        for line in text.split("\n"):
            stripped = line.strip()
            if stripped and re.match(page_pattern, stripped):
                continue
            if stripped in watermarks:
                continue
            lines.append(line)

        raw = "\n".join(lines)
        pages.append({
            "page_idx": page_idx,
            "page_number": printed_page,
            "toc_entry": toc_entry,
            "text": clean_text(raw),
        })

    doc.close()
    print(f"  Pages: {len(pages)} content pages extracted")
    return pages


# ── Step 3b: Collect sub-headings per ToC section ────────────────

def collect_sub_headings(pages: list[dict], toc_sections: list[dict], config: dict) -> None:
    """Scan extracted pages and collect sub-headings found within each ToC section.
    Uses section-specific parsing rules. Modifies toc_sections in-place."""
    section_pages = {}
    for page in pages:
        title = page["toc_entry"]["title"]
        if title not in section_pages:
            section_pages[title] = []
        section_pages[title].append(page)

    for section in toc_sections:
        if section["is_excluded"]:
            continue
        rules = _get_section_parsing(section["title"], config)
        pages_for_section = section_pages.get(section["title"], [])
        headings = []
        for page in pages_for_section:
            for line in page["text"].split("\n"):
                stripped = line.strip()
                if not stripped:
                    continue
                if _is_metadata_line(stripped, rules):
                    continue
                if _is_keep_with_entry(stripped, rules):
                    continue
                if _is_heading(stripped, rules) and stripped not in headings:
                    headings.append(stripped)

        section["sub_headings"] = headings[:50]

    total = sum(len(s["sub_headings"]) for s in toc_sections if not s["is_excluded"])
    print(f"  Sub-headings: {total} collected across sections")


# ── Step 4: Build entries within ToC sections ────────────────────

def _get_section_parsing(toc_title: str, config: dict) -> dict:
    """Get parsing rules for a specific ToC section.
    Checks section_parsing for a matching key (substring match on toc_title),
    falls back to parsing_defaults."""
    defaults = config.get("parsing_defaults", {})
    section_overrides = config.get("section_parsing", {})

    # Find matching section override
    toc_lower = toc_title.lower()
    for key, rules in section_overrides.items():
        if key.lower() in toc_lower:
            # Merge: section rules override defaults
            merged = dict(defaults)
            merged.update(rules)
            return merged

    return defaults


def _is_heading(stripped: str, rules: dict) -> bool:
    """Check if a line is a heading based on parsing rules."""
    hr = rules.get("heading_rules", {})
    min_len = hr.get("min_length", 3)
    max_len = hr.get("max_length", 50)

    if not (min_len <= len(stripped) <= max_len):
        return False
    if hr.get("must_start_upper", True) and not stripped[0].isupper():
        return False
    if hr.get("no_trailing_period", True) and stripped.endswith("."):
        return False
    if hr.get("no_trailing_comma", True) and stripped.endswith(","):
        return False
    if hr.get("no_colon", True) and ":" in stripped:
        return False
    if "\t" in stripped:
        return False

    min_alpha = hr.get("min_alpha_ratio", 0.7)
    alpha_count = sum(1 for c in stripped if c.isalpha() or c in " '-/()")
    if alpha_count < len(stripped) * min_alpha:
        return False

    return True


def _is_metadata_line(stripped: str, rules: dict) -> bool:
    """Check if a line is a metadata field (key:value) that should stay with its entry."""
    metadata_fields = rules.get("metadata_fields", [])
    for pattern in metadata_fields:
        if re.search(pattern, stripped, re.IGNORECASE):
            return True
    return False


def _is_keep_with_entry(stripped: str, rules: dict) -> bool:
    """Check if a line should be kept with the current entry (not treated as heading)."""
    keep_patterns = rules.get("keep_with_entry", [])
    for pattern in keep_patterns:
        if re.match(pattern, stripped, re.IGNORECASE):
            return True
    return False


def _is_sub_section(stripped: str, rules: dict) -> bool:
    """Check if a line is a sub-section delimiter (e.g. 'First-Level Spells')."""
    pattern = rules.get("sub_section_pattern")
    if pattern and re.match(pattern, stripped, re.IGNORECASE):
        return True
    return False


def _clean_entry_content(lines: list[str]) -> str:
    """Clean up an entry's content lines before storage.
    - Remove leading/trailing blank lines
    - Collapse multiple consecutive blank lines to one
    - Join short continuation lines to previous line
    - Strip extraneous whitespace"""
    # Remove leading/trailing blanks
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()

    cleaned = []
    for line in lines:
        if not line.strip():
            if cleaned and cleaned[-1] != "":
                cleaned.append("")
        else:
            cleaned.append(line.strip())

    return "\n".join(cleaned).strip()


def build_entries(pages: list[dict], config: dict) -> list[dict]:
    """Group pages by ToC section, detect headings using section-specific rules.
    Each entry has: toc_entry, section_title, entry_title, content, page_numbers."""
    entries = []

    # Group pages by ToC section
    sections = {}
    for page in pages:
        toc_title = page["toc_entry"]["title"]
        if toc_title not in sections:
            sections[toc_title] = []
        sections[toc_title].append(page)

    # Build set of ToC titles to exclude from heading detection
    toc_titles_lower = set()
    for page in pages:
        t = page["toc_entry"]["title"].lower()
        toc_titles_lower.add(t)
        # Also add just the name part after "Appendix N:" etc.
        name = re.sub(r"^(chapter|appendix)\s+\d+\s*:?\s*", "", t, flags=re.IGNORECASE).strip()
        if name:
            toc_titles_lower.add(name)

    for toc_title, section_pages in sections.items():
        toc_entry = section_pages[0]["toc_entry"]
        rules = _get_section_parsing(toc_title, config)
        last_was_metadata = False

        current_section = None
        current_entry_title = None
        current_content = []
        current_page_numbers = []

        def flush():
            nonlocal current_content, current_page_numbers
            if current_content:
                # Clean up: collapse blank lines, join continuation lines
                cleaned = _clean_entry_content(current_content)
                if cleaned and len(cleaned) > 20:
                    entries.append({
                        "toc_entry": toc_entry,
                        "section_title": current_section,
                        "entry_title": current_entry_title,
                        "content": cleaned,
                        "page_numbers": sorted(set(current_page_numbers)),
                    })
            current_content = []
            current_page_numbers = []

        for page in section_pages:
            text = page["text"]
            page_num = page["page_number"]

            for line in text.split("\n"):
                stripped = line.strip()
                if not stripped:
                    current_content.append("")
                    last_was_metadata = False
                    continue

                # If previous line was a metadata field and this line looks like
                # a continuation (short, doesn't start a new field), join it
                if last_was_metadata and not _is_metadata_line(stripped, rules) and not _is_heading(stripped, rules):
                    if len(stripped) < 40 and not stripped[0].isupper():
                        if current_content:
                            current_content[-1] = current_content[-1] + " " + stripped
                            last_was_metadata = False
                            continue

                # Skip ToC section titles that appear as text on the page
                if stripped.lower() in toc_titles_lower:
                    continue

                # Check in order: sub-section > metadata > keep_with_entry > heading > content
                if _is_sub_section(stripped, rules):
                    flush()
                    current_section = stripped
                    current_entry_title = None
                    current_content = [stripped]
                    current_page_numbers = [page_num]
                    last_was_metadata = False
                elif _is_metadata_line(stripped, rules):
                    current_content.append(stripped)
                    if page_num not in current_page_numbers:
                        current_page_numbers.append(page_num)
                    last_was_metadata = True
                elif _is_keep_with_entry(stripped, rules):
                    current_content.append(stripped)
                    if page_num not in current_page_numbers:
                        current_page_numbers.append(page_num)
                    last_was_metadata = False
                elif _is_heading(stripped, rules):
                    flush()
                    current_entry_title = stripped
                    current_content = [stripped]
                    current_page_numbers = [page_num]
                    last_was_metadata = False
                else:
                    current_content.append(stripped)
                    if page_num not in current_page_numbers:
                        current_page_numbers.append(page_num)
                    last_was_metadata = False

        flush()

    print(f"  Entries: {len(entries)} across {len(sections)} ToC sections")
    return entries


# ── Step 5: Chunk entries ────────────────────────────────────────

def chunk_entries(entries: list[dict], config: dict) -> list[dict]:
    """Split entries into chunks. Never crosses ToC section boundaries.
    Each chunk inherits all metadata from its entry."""
    chunking = config.get("chunking", {})
    max_chars = chunking.get("max_chars", 800)
    overlap = chunking.get("overlap", 200)

    chunks = []
    for entry in entries:
        content = entry["content"]
        toc = entry["toc_entry"]
        page_nums = entry["page_numbers"]
        page_str = ",".join(str(p) for p in page_nums)

        if len(content) <= max_chars:
            chunks.append({
                "toc_entry": toc,
                "section_title": entry["section_title"],
                "entry_title": entry["entry_title"],
                "content": content,
                "page_numbers": page_str,
                "chunk_type": "content",
            })
        else:
            paragraphs = content.split("\n\n")
            current = ""
            for para in paragraphs:
                if len(current) + len(para) + 2 > max_chars and current:
                    chunks.append({
                        "toc_entry": toc,
                        "section_title": entry["section_title"],
                        "entry_title": entry["entry_title"],
                        "content": current.strip(),
                        "page_numbers": page_str,
                        "chunk_type": "content",
                    })
                    overlap_text = current.strip()[-overlap:] if overlap > 0 else ""
                    current = overlap_text + "\n\n" + para if overlap_text else para
                else:
                    current = current + "\n\n" + para if current else para
            if current.strip():
                chunks.append({
                    "toc_entry": toc,
                    "section_title": entry["section_title"],
                    "entry_title": entry["entry_title"],
                    "content": current.strip(),
                    "page_numbers": page_str,
                    "chunk_type": "content",
                })

    print(f"  Chunks: {len(chunks)}")
    return chunks


# ── Step 6: Store ────────────────────────────────────────────────

def store(
    filepath: Path,
    toc_data: dict,
    chunks: list[dict],
    game_system: str | None = None,
    content_type: str | None = None,
) -> None:
    """Write ToC (with sub-headings and tables), chunks, and file metadata to DuckDB."""
    conn = duckdb.connect(DB_PATH)
    init_schema(conn)
    now = datetime.now(timezone.utc)
    toc_sections = toc_data["sections"]

    # Clear previous data for this file
    conn.execute("DELETE FROM documents_tabletop_rules.chunks WHERE source_file = ?", [filepath.name])
    conn.execute("DELETE FROM documents_tabletop_rules.toc WHERE source_file = ?", [filepath.name])
    conn.execute("DELETE FROM documents_tabletop_rules.files WHERE source_file = ?", [filepath.name])

    # Insert ToC sections with sub-headings and tables
    max_toc_id = conn.execute(
        "SELECT COALESCE(MAX(toc_id), 0) FROM documents_tabletop_rules.toc"
    ).fetchone()[0]

    toc_id_map = {}
    for i, entry in enumerate(toc_sections):
        toc_id = max_toc_id + i + 1
        toc_id_map[entry["title"]] = toc_id
        sub_headings_str = "; ".join(entry.get("sub_headings", []))
        tables_str = "; ".join(entry.get("tables", []))
        conn.execute(
            """INSERT INTO documents_tabletop_rules.toc
               (toc_id, source_file, title, page_start, page_end,
                is_excluded, sub_headings, tables)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [toc_id, filepath.name, entry["title"], entry["page_start"],
             entry["page_end"], entry["is_excluded"], sub_headings_str, tables_str],
        )

    # Insert chunks
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

    # Insert file metadata
    conn.execute(
        """INSERT INTO documents_tabletop_rules.files
           (source_file, document_title, game_system, content_type,
            total_chunks, total_toc_entries, parsed_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [filepath.name, filepath.stem, game_system, content_type,
         len(chunks), len(toc_sections), now],
    )

    conn.close()
    print(f"  Stored: {len(toc_sections)} ToC entries, {len(chunks)} chunks")


# ── Pipeline ─────────────────────────────────────────────────────

def parse_pdf(filepath: Path, game_system: str | None = None,
              content_type: str | None = None) -> None:
    """Full ingestion pipeline for a single PDF."""
    import time
    start = time.time()
    file_size_mb = filepath.stat().st_size / (1024 * 1024)
    print(f"\nParsing {filepath.name} ({file_size_mb:.1f} MB)")

    config = load_config(filepath)

    # 1. Parse ToC — sections and tables
    toc_data = parse_toc(filepath, config)

    # 2. Map pages to ToC sections
    page_map = build_page_map(filepath, toc_data["sections"], config)

    # 3. Detect watermarks and extract text per page
    watermarks = detect_watermarks(filepath)
    pages = extract_pages(filepath, page_map, config, watermarks)

    # 3b. Collect sub-headings within each ToC section
    collect_sub_headings(pages, toc_data["sections"], config)

    # 4. Build entries within ToC sections
    entries = build_entries(pages, config)

    # 5. Chunk
    chunks = chunk_entries(entries, config)

    # 6. Store
    store(filepath, toc_data, chunks, game_system, content_type)

    elapsed = time.time() - start
    print(f"  Done in {elapsed:.1f}s")


def run(
    game_system: str | None = None,
    content_type: str | None = None,
    directory: Path | None = None,
) -> None:
    """Ingest all PDFs in the raw documents directory."""
    doc_dir = directory or DOCUMENTS_DIR
    files = sorted(doc_dir.glob("*.pdf"))

    if not files:
        print(f"No PDF files found in {doc_dir}")
        return

    total_size_mb = sum(f.stat().st_size for f in files) / (1024 * 1024)
    print(f"Ingesting {len(files)} PDFs ({total_size_mb:.1f} MB total)")

    for f in files:
        parse_pdf(f, game_system=game_system, content_type=content_type)

    print(f"\nAll done: {len(files)} files ingested")


if __name__ == "__main__":
    run(game_system="D&D 2e", content_type="rules")
