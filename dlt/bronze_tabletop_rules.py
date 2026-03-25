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
        CREATE TABLE IF NOT EXISTS bronze_tabletop.watermarks (
            source_file       VARCHAR NOT NULL,
            watermark_text    VARCHAR NOT NULL,
            occurrence_count  INTEGER NOT NULL,
            PRIMARY KEY (source_file, watermark_text)
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

    for page_idx in range(total_pages):
        text = doc[page_idx].get_text("text")
        page_texts.append(text)

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
        page_printed[page_idx] = printed

    doc.close()
    return page_texts, page_printed, total_pages


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


def extract_toc(page_texts: list[str], config: dict) -> list[dict]:
    """Parse ToC from first N pages."""
    toc_config = config.get("toc", {})
    chapter_patterns = [re.compile(p, re.IGNORECASE) for p in toc_config.get("chapter_patterns", [])]
    table_pattern_str = toc_config.get("table_pattern", "")
    table_pattern = re.compile(table_pattern_str, re.IGNORECASE) if table_pattern_str else None
    scan_pages = toc_config.get("toc_scan_pages", 15)
    exclude_set = set(t.lower() for t in config.get("exclude_chapters", []))

    sections = []
    seen = set()

    for page_idx in range(min(scan_pages, len(page_texts))):
        for line in page_texts[page_idx].split("\n"):
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
                        })
                    break

    sections.sort(key=lambda e: e["page_start"])
    for i, entry in enumerate(sections):
        entry["page_end"] = sections[i + 1]["page_start"] - 1 if i + 1 < len(sections) else 9999

    return sections


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
                 known_entries: set[str], watermarks: dict[str, int]) -> None:
    """Write all raw extraction data to bronze_tabletop schema."""
    conn = duckdb.connect(DB_PATH)
    init_bronze_schema(conn)
    now = datetime.now(timezone.utc)
    sf = filepath.name

    # Delete old data for this file (idempotent re-ingestion)
    for table in ["files", "marker_extractions", "page_texts",
                   "toc_raw", "known_entries_raw", "watermarks"]:
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

    # Watermarks
    for text, count in watermarks.items():
        conn.execute(
            "INSERT INTO bronze_tabletop.watermarks VALUES (?, ?, ?)",
            [sf, text, count],
        )

    conn.close()
    _log(f"  Bronze stored: {len(page_texts)} pages, {len(toc_sections)} ToC, "
         f"{len(known_entries)} index entries, {len(watermarks)} watermarks")


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

    # 2. ToC
    toc_sections = extract_toc(page_texts, config)
    included = sum(1 for s in toc_sections if not s["is_excluded"])
    excluded = sum(1 for s in toc_sections if s["is_excluded"])
    step(f"ToC: {included} sections, {excluded} excluded")

    # 3. Marker full document (uses disk cache if available)
    _log("  Marker: extracting full document...")
    markdown = extract_marker_markdown(filepath)
    step(f"Marker doc: {len(markdown):,} chars")

    # 5. Known entries from indexes
    known_entries = extract_known_entries(page_texts, page_printed, toc_sections, config)
    step(f"Known entries: {len(known_entries)}")

    # 6. Watermarks
    watermarks = detect_watermarks(page_texts)
    step(f"Watermarks: {len(watermarks)}")

    # 7. Store everything
    store_bronze(filepath, config, page_texts, page_printed,
                 markdown, toc_sections, known_entries, watermarks)
    step("Stored")

    _log(f"  Bronze total: {time.time() - start:.1f}s")


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
    if len(sys.argv) > 1:
        extract_pdf(Path(sys.argv[1]))
    else:
        run()
