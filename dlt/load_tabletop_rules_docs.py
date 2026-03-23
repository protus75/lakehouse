"""
ToC-first PDF ingestion for tabletop RPG rule books.

- pymupdf: page numbers -> ToC chapter assignment (truth)
- Marker: text extraction as markdown (handles columns, tables, headings)
- Merge: tag each page's Marker content with its chapter from pymupdf

No heuristic heading detection. Marker's # headings are used directly.
"""

import re
from pathlib import Path
from datetime import datetime, timezone

import duckdb
import fitz  # pymupdf -- page numbers only

from dlt.lib.tabletop_cleanup import (
    _log,
    load_config,
    _extract_toc_line,
    _detect_watermarks,
    build_heading_chapter_map,
    build_entries,
    collect_sub_headings,
    chunk_entries,
)

DB_PATH = "/workspace/db/lakehouse.duckdb"
DOCUMENTS_DIR = Path("/workspace/documents/tabletop_rules/raw")
CONFIGS_DIR = Path("/workspace/documents/tabletop_rules/configs")


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
        _log(f"  PDF cache: {self.total_pages} pages read")


# ── Step 1: Parse ToC ────────────────────────────────────────────

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
    _log(f"  ToC: {included} sections, {excluded} excluded, {len(tables)} tables")
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
        _log(f"  Marker: using cached {cache_path.name}")
        md = cache_path.read_text(encoding="utf-8")
    else:
        from marker.converters.pdf import PdfConverter
        models = _get_marker_models()
        converter = PdfConverter(artifact_dict=models)
        rendered = converter(str(filepath))
        md = rendered.markdown
        cache_path.write_text(md, encoding="utf-8")
        _log(f"  Marker: cached to {cache_path.name}")

    # Strip image references
    md = re.sub(r"!\[.*?\]\(.*?\)", "", md)
    # Rejoin hyphenated words
    md = re.sub(r"(\w)-\s*\n\s*(\w)", r"\1\2", md)

    return md


# ── Step 5: Extract known entry names from indexes ───────────────

def extract_known_entries(pdf: PDFCache, toc_data: dict, config: dict) -> set[str]:
    """Get valid entry names from excluded index sections.

    Only extracts lines that are actual index entries (name + page number),
    not category headers or other text that lacks a page reference.
    Uses _extract_toc_line for parsing -- no regex on content."""
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
        _log(f"  Known entries: {len(names)} from index sections")
    return names


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
    _log(f"  Stored: {len(toc_sections)} ToC, {len(chunks)} chunks, {len(known_entries or [])} known entries")


# ── Pipeline ─────────────────────────────────────────────────────

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

    config = load_config(filepath, CONFIGS_DIR)

    # 1. Read PDF once -- cache page texts and page numbers
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
    watermarks = _detect_watermarks(pdf.page_texts, pdf.total_pages)
    if watermarks:
        lines = [l for l in markdown.split("\n") if l.strip() not in watermarks]
        markdown = "\n".join(lines)
    step("Watermarks stripped")

    # 5. Map headings to chapters via ToC state machine
    heading_chapter_map = build_heading_chapter_map(
        markdown, toc_data["sections"],
        pdf.page_texts, pdf.page_printed, pdf.total_pages, config,
    )
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
        _log(f"No PDFs in {doc_dir}")
        return
    _log(f"Ingesting {len(files)} PDFs ({sum(f.stat().st_size for f in files) / 1024 / 1024:.1f} MB)")
    for f in files:
        parse_pdf(f, game_system=game_system, content_type=content_type)
    _log(f"\nDone: {len(files)} files")


if __name__ == "__main__":
    run(game_system="D&D 2e", content_type="rules")
