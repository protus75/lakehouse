"""
dlt pipeline: loads tabletop RPG/board game PDFs into DuckDB (documents_tabletop_rules schema).

Two-pass hybrid PDF extraction:
  Pass 1: Marker (layout-aware markdown with multi-column support)
  Pass 2: VLM via Ollama (vision model renders problem pages as images for structured extraction)

Chapter-aligned chunking preserves book structure: chapter > section > entry.

Run from CLI:
  python -c "from dlt.load_tabletop_rules_docs import run; run(game_system='D&D 2e', content_type='rules')"

Or with custom worker count:
  python -c "from dlt.load_tabletop_rules_docs import run; run(game_system='D&D 2e', max_workers=8)"
"""

import re
import base64
from pathlib import Path
from datetime import datetime, timezone
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor, as_completed

import duckdb
import fitz  # pymupdf — used for page rendering to images for VLM pass
import requests

DB_PATH = "/workspace/db/lakehouse.duckdb"
DOCUMENTS_DIR = Path("/workspace/documents/tabletop_rules/raw")
OLLAMA_URL = "http://host.docker.internal:11434"
VLM_MODEL = "minicpm-v"


# ── Schema ───────────────────────────────────────────────────────

def init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create documents_tabletop_rules schema with chunks and metadata tables."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS documents_tabletop_rules")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS documents_tabletop_rules.chunks (
            chunk_id        INTEGER PRIMARY KEY,
            source_file     VARCHAR NOT NULL,
            chapter_title   VARCHAR,
            section_title   VARCHAR,
            content         VARCHAR NOT NULL,
            char_count      INTEGER NOT NULL,
            page_numbers    VARCHAR,
            parsed_at       TIMESTAMP NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS documents_tabletop_rules.files (
            source_file     VARCHAR PRIMARY KEY,
            document_title  VARCHAR,
            game_system     VARCHAR,
            content_type    VARCHAR,
            tags            VARCHAR,
            rules_version   VARCHAR,
            total_chunks    INTEGER NOT NULL,
            total_chars     INTEGER NOT NULL,
            parsed_at       TIMESTAMP NOT NULL
        )
    """)


# ── Pass 1: Marker extraction ───────────────────────────────────

_marker_models = None

def _get_marker_models():
    """Lazy-load Marker models once and reuse across files."""
    global _marker_models
    if _marker_models is None:
        from marker.models import create_model_dict
        _marker_models = create_model_dict()
    return _marker_models


def extract_with_marker(filepath: Path) -> str:
    """Use Marker for layout-aware PDF to markdown conversion.
    Marker handles multi-column layouts, reading order, and tables."""
    from marker.converters.pdf import PdfConverter

    models = _get_marker_models()
    converter = PdfConverter(artifact_dict=models)
    rendered = converter(str(filepath))
    return rendered.markdown


# ── Pass 2: VLM extraction for structured content ───────────────

STAT_BLOCK_PATTERNS = [
    r"(?:Range|Components|Duration|Casting Time|Area of Effect|Saving Throw)\s*:",
    r"(?:Power Score|PSP Cost|Initial Cost|Maintenance Cost)\s*:",
    r"(?:AC|THAC0|Hit Dice|No\. of Attacks|Damage/Attack|Movement)\s*:",
    r"(?:XP Value|GP Value|Charges)\s*:",
    r"(?:Sphere|School|Level)\s*:\s*\S",
]


def detect_incomplete_pages(markdown: str, filepath: Path) -> list[int]:
    """Identify pages that likely have structured content Marker may have
    mangled — stat blocks, ability tables, multi-column key:value pairs.
    Compares Marker output against pymupdf raw text per page to find
    pages where structured fields were dropped."""
    doc = fitz.open(str(filepath))
    incomplete_pages = []

    for page_num in range(len(doc)):
        page = doc[page_num]
        raw_text = page.get_text("text")

        has_stat_block = any(
            re.search(pat, raw_text, re.IGNORECASE)
            for pat in STAT_BLOCK_PATTERNS
        )
        if not has_stat_block:
            continue

        fields_in_raw = set()
        for pat in STAT_BLOCK_PATTERNS:
            for m in re.finditer(pat, raw_text, re.IGNORECASE):
                fields_in_raw.add(m.group().split(":")[0].strip().lower())

        fields_in_markdown = set()
        for pat in STAT_BLOCK_PATTERNS:
            for m in re.finditer(pat, markdown, re.IGNORECASE):
                fields_in_markdown.add(m.group().split(":")[0].strip().lower())

        missing = fields_in_raw - fields_in_markdown
        if missing:
            incomplete_pages.append(page_num)

    doc.close()
    return incomplete_pages


def render_page_to_base64(filepath: Path, page_num: int, dpi: int = 300) -> str:
    """Render a single PDF page to a base64-encoded PNG for VLM input."""
    doc = fitz.open(str(filepath))
    page = doc[page_num]
    mat = fitz.Matrix(dpi / 72, dpi / 72)
    pix = page.get_pixmap(matrix=mat)
    img_bytes = pix.tobytes("png")
    doc.close()
    return base64.b64encode(img_bytes).decode("utf-8")


VLM_PROMPT = """Extract ALL text from this page of a tabletop RPG rulebook.

Preserve the COMPLETE content with exact formatting for:
- Stat blocks (spells, psionic powers, magic abilities, monster entries, magic items)
- All key: value fields such as Range, Components, Duration, Casting Time,
  Area of Effect, Saving Throw, Sphere, School, Level, Power Score, PSP Cost,
  AC, THAC0, Hit Dice, XP Value, GP Value, and any similar structured fields
- Tables with all rows and columns
- Section headings and subheadings

Output clean markdown. Preserve every field exactly as it appears on the page.
Do not summarize, skip, or paraphrase any content."""


def extract_pages_with_vlm(
    filepath: Path,
    page_numbers: list[int],
) -> dict[int, str]:
    """Send rendered page images to a VLM via Ollama for structured extraction.
    Returns a dict mapping page_number -> extracted markdown text."""
    results = {}

    for page_num in page_numbers:
        img_b64 = render_page_to_base64(filepath, page_num)

        try:
            response = requests.post(
                f"{OLLAMA_URL}/api/generate",
                json={
                    "model": VLM_MODEL,
                    "prompt": VLM_PROMPT,
                    "images": [img_b64],
                    "stream": False,
                },
                timeout=120,
            )
            response.raise_for_status()
            results[page_num] = response.json()["response"]
            print(f"    VLM extracted page {page_num + 1}")
        except Exception as e:
            print(f"    VLM failed on page {page_num + 1}: {e}")

    return results


def merge_vlm_into_markdown(
    markdown: str,
    vlm_pages: dict[int, str],
    filepath: Path,
) -> str:
    """Merge VLM-extracted structured fields into the Marker markdown.
    For each VLM page, find stat-block fields that are in the VLM output
    but missing from the corresponding markdown section, and inject them."""
    if not vlm_pages:
        return markdown

    doc = fitz.open(str(filepath))

    for page_num, vlm_text in vlm_pages.items():
        vlm_fields = []
        for pat in STAT_BLOCK_PATTERNS:
            for m in re.finditer(pat, vlm_text, re.IGNORECASE):
                line_start = vlm_text.rfind("\n", 0, m.start()) + 1
                line_end = vlm_text.find("\n", m.end())
                if line_end == -1:
                    line_end = len(vlm_text)
                field_line = vlm_text[line_start:line_end].strip()
                if field_line and field_line.lower() not in markdown.lower():
                    vlm_fields.append(field_line)

        if not vlm_fields:
            continue

        page = doc[page_num]
        raw_text = page.get_text("text")
        headings = re.findall(r"^([A-Z][A-Za-z' ]{2,40})$", raw_text, re.MULTILINE)

        injected = False
        for heading in headings:
            heading_pat = re.compile(
                r"(#{1,4}\s*" + re.escape(heading) + r".*?\n)",
                re.IGNORECASE,
            )
            match = heading_pat.search(markdown)
            if match:
                insert_point = match.end()
                field_block = "\n".join(vlm_fields) + "\n\n"
                markdown = markdown[:insert_point] + field_block + markdown[insert_point:]
                injected = True
                break

        if not injected:
            page_marker = f"\n\n<!-- VLM supplement page {page_num + 1} -->\n"
            markdown += page_marker + "\n".join(vlm_fields) + "\n"

    doc.close()
    return markdown


# ── Chapter-aligned chunking ────────────────────────────────────

def extract_toc_entries(filepath: Path) -> dict:
    """Extract chapter and table titles from the PDF table of contents using pymupdf.
    Returns a dict with 'chapters' and 'tables' lists."""
    doc = fitz.open(str(filepath))
    chapters = []
    tables = []
    for page_num in range(min(15, len(doc))):
        text = doc[page_num].get_text("text")
        for match in re.finditer(
            r"(Chapter\s+\d+\s*:\s*[^\n.]+|Appendix\s+\d*\s*:?\s*[^\n.]+)",
            text,
            re.IGNORECASE,
        ):
            title = match.group(1).strip()
            title = re.sub(r"\s*\.{2,}.*", "", title).strip()
            if title and title not in chapters:
                chapters.append(title)
        for match in re.finditer(
            r"(Table\s+\d+\s*:\s*[^\n.]+)",
            text,
            re.IGNORECASE,
        ):
            title = match.group(1).strip()
            title = re.sub(r"\s*\.{2,}.*", "", title).strip()
            if title and title not in tables:
                tables.append(title)
    doc.close()
    print(f"    ToC: found {len(chapters)} chapters, {len(tables)} tables")
    return {"chapters": chapters, "tables": tables}


def _clean_heading(text: str) -> str:
    """Strip markdown bold markers and extra whitespace from a heading."""
    return re.sub(r"\*+", "", text).strip()


INDEX_HEADINGS = {
    "index",
    "alphabetical index",
    "general index",
    "spell index",
    "priest spell index",
    "wizard spell index",
    "spell list",
    "priest spell list",
    "wizard spell list",
}


def _match_toc_entry(heading: str, toc_list: list[str]) -> str | None:
    """Match a markdown heading against a list of ToC titles.
    Returns the matched title or None."""
    clean = _clean_heading(heading).lower()
    for entry in toc_list:
        entry_lower = entry.lower()
        # Extract just the name part after "Chapter N:" / "Table N:" / "Appendix N:"
        name = re.sub(
            r"^(chapter|appendix|table)\s+\d+\s*:?\s*", "", entry_lower
        ).strip()
        if name and (name in clean or clean in name):
            return entry
        if entry_lower in clean or clean in entry_lower:
            return entry
    return None


def _is_index_heading(heading: str) -> bool:
    """Check if a heading marks the start of a book index or spell index."""
    clean = _clean_heading(heading).lower()
    if clean in INDEX_HEADINGS:
        return True
    return "index" in clean and ("spell" in clean or clean.endswith("index"))


def parse_book_structure(markdown: str, toc_entries: dict) -> list[dict]:
    """Parse markdown into a hierarchical book structure using ToC entries
    extracted from the PDF for accurate chapter and table assignment.
    Stops processing when it hits the Index section.
    Returns a flat list of entries with chapter/section/entry context."""
    toc_chapters = toc_entries["chapters"]
    toc_tables = toc_entries["tables"]

    entries = []
    current_chapter = None
    current_section = None

    lines = markdown.split("\n")
    current_content = []
    current_entry_title = None
    in_index = False

    def flush_entry():
        if current_content:
            content = "\n".join(current_content).strip()
            if content:
                entries.append({
                    "chapter_title": current_chapter,
                    "section_title": current_section,
                    "entry_title": current_entry_title,
                    "content": content,
                })

    for line in lines:
        h1 = re.match(r"^#\s+(.+)", line)
        h2 = re.match(r"^##\s+(.+)", line)
        h3_h4 = re.match(r"^#{3,4}\s+(.+)", line)

        heading_match = h1 or h2 or h3_h4
        if heading_match:
            heading_text = heading_match.group(1).strip()

            # Stop processing at the Index
            if _is_index_heading(heading_text):
                flush_entry()
                in_index = True
                print(f"    Skipping index section: '{_clean_heading(heading_text)}'")
                break

            # Check if this heading matches a named table from the ToC
            table_match = _match_toc_entry(heading_text, toc_tables)

        if in_index:
            continue

        if h1:
            flush_entry()
            heading_text = h1.group(1).strip()
            matched = _match_toc_entry(heading_text, toc_chapters)
            if matched:
                current_chapter = matched
                current_section = None
                current_entry_title = None
            elif table_match:
                current_entry_title = table_match
            else:
                current_section = _clean_heading(heading_text)
                current_entry_title = None
            current_content = [line]
        elif h2:
            flush_entry()
            heading_text = h2.group(1).strip()
            matched = _match_toc_entry(heading_text, toc_chapters)
            if matched:
                current_chapter = matched
                current_section = None
                current_entry_title = None
            elif table_match:
                current_entry_title = table_match
            else:
                current_section = _clean_heading(heading_text)
                current_entry_title = None
            current_content = [line]
        elif h3_h4:
            flush_entry()
            heading_text = h3_h4.group(1).strip()
            matched = _match_toc_entry(heading_text, toc_chapters)
            if matched:
                current_chapter = matched
                current_section = None
                current_entry_title = None
            elif table_match:
                current_entry_title = table_match
            else:
                current_entry_title = _clean_heading(heading_text)
            current_content = [line]
        else:
            current_content.append(line)

    if not in_index:
        flush_entry()
    return entries


def chunk_entries(
    entries: list[dict],
    max_chars: int = 800,
    overlap: int = 200,
) -> list[dict]:
    """Create chunks from parsed book entries.

    Rules:
    - Each entry becomes one chunk if it fits within max_chars
    - Large entries are split by paragraphs with overlap, but never cross
      into a different entry/section/chapter
    - Every chunk carries its chapter_title and section_title for context
    """
    chunks = []

    for entry in entries:
        content = entry["content"]
        chapter = entry["chapter_title"]
        section = entry["section_title"]
        title = entry["entry_title"] or entry["section_title"] or entry["chapter_title"]

        if len(content) <= max_chars:
            chunks.append({
                "chapter_title": chapter,
                "section_title": section,
                "entry_title": title,
                "content": content,
            })
        else:
            paragraphs = content.split("\n\n")
            sub_chunks = []
            current = ""

            for para in paragraphs:
                if len(current) + len(para) + 2 > max_chars and current:
                    sub_chunks.append(current.strip())
                    overlap_text = current.strip()[-overlap:] if overlap > 0 else ""
                    current = overlap_text + "\n\n" + para if overlap_text else para
                else:
                    current = current + "\n\n" + para if current else para

            if current.strip():
                sub_chunks.append(current.strip())

            for sc in sub_chunks:
                chunks.append({
                    "chapter_title": chapter,
                    "section_title": section,
                    "entry_title": title,
                    "content": sc,
                })

    return chunks


# ── PDF parsing pipeline ────────────────────────────────────────

def parse_pdf(filepath: Path, use_vlm: bool = True) -> tuple[str, list[dict]] | None:
    """Parse a single PDF using two-pass hybrid extraction.

    Pass 1: Marker for layout-aware markdown
    Pass 2: VLM via Ollama for pages with incomplete structured content
    Then: chapter-aligned chunking
    """
    import time
    try:
        start = time.time()
        file_size_mb = filepath.stat().st_size / (1024 * 1024)
        print(f"  Parsing {filepath.name} ({file_size_mb:.1f} MB)...")

        print(f"    Pass 1: Marker extraction...")
        markdown = extract_with_marker(filepath)
        print(f"    Pass 1 complete: {len(markdown):,} chars")

        if use_vlm:
            print(f"    Pass 2: Detecting pages needing VLM...")
            incomplete = detect_incomplete_pages(markdown, filepath)
            if incomplete:
                print(f"    Pass 2: VLM extracting {len(incomplete)} pages...")
                vlm_results = extract_pages_with_vlm(filepath, incomplete)
                markdown = merge_vlm_into_markdown(markdown, vlm_results, filepath)
                print(f"    Pass 2 complete: supplemented {len(vlm_results)} pages")
            else:
                print(f"    Pass 2: No incomplete pages detected, skipping VLM")

        print(f"    Extracting table of contents...")
        toc_entries = extract_toc_entries(filepath)
        entries = parse_book_structure(markdown, toc_entries)
        chunks = chunk_entries(entries)

        elapsed = time.time() - start
        print(f"    Done in {elapsed:.1f}s -> {len(chunks)} chunks")
        return (filepath.name, chunks)
    except Exception as e:
        print(f"  ERROR parsing {filepath.name}: {e}")
        import traceback
        traceback.print_exc()
        return None


# ── Database writes ─────────────────────────────────────────────

def ingest_file(
    filepath: Path,
    conn: duckdb.DuckDBPyConnection,
    chunks: list[dict],
    document_title: str | None = None,
    game_system: str | None = None,
    content_type: str | None = None,
    tags: str | None = None,
    rules_version: str | None = None,
) -> int:
    """Store pre-parsed chunks in DuckDB with metadata."""
    now = datetime.now(timezone.utc)

    conn.execute(
        "DELETE FROM documents_tabletop_rules.chunks WHERE source_file = ?",
        [filepath.name],
    )
    conn.execute(
        "DELETE FROM documents_tabletop_rules.files WHERE source_file = ?",
        [filepath.name],
    )

    max_id = conn.execute(
        "SELECT COALESCE(MAX(chunk_id), 0) FROM documents_tabletop_rules.chunks"
    ).fetchone()[0]

    for i, chunk in enumerate(chunks):
        conn.execute(
            """INSERT INTO documents_tabletop_rules.chunks
               (chunk_id, source_file, chapter_title, section_title,
                content, char_count, parsed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [
                max_id + i + 1,
                filepath.name,
                chunk.get("chapter_title"),
                chunk.get("section_title"),
                chunk["content"],
                len(chunk["content"]),
                now,
            ],
        )

    total_chars = sum(len(c["content"]) for c in chunks)
    conn.execute(
        """INSERT INTO documents_tabletop_rules.files
           (source_file, document_title, game_system, content_type, tags, rules_version,
            total_chunks, total_chars, parsed_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            filepath.name,
            document_title or filepath.stem,
            game_system,
            content_type,
            tags,
            rules_version,
            len(chunks),
            total_chars,
            now,
        ],
    )

    print(f"  {filepath.name}: {len(chunks)} chunks, {total_chars:,} chars")
    return len(chunks)


def ingest_all(
    directory: Path | None = None,
    game_system: str | None = None,
    content_type: str | None = None,
    tags: str | None = None,
    max_workers: int | None = None,
    use_vlm: bool = True,
) -> None:
    """Parse all PDFs in documents/tabletop_rules/raw directory.

    Args:
        directory: Override directory to scan
        game_system: e.g., "D&D 2e", "Pathfinder 2e"
        content_type: e.g., "rules", "module", "campaign"
        tags: comma-separated tags for categorization
        max_workers: Number of cores to use (default: all available)
        use_vlm: Enable Pass 2 VLM extraction for structured content
    """
    import time
    overall_start = time.time()

    doc_dir = directory or DOCUMENTS_DIR
    files = sorted(doc_dir.glob("*.pdf"))

    if not files:
        print(f"No PDF files found in {doc_dir}")
        return

    if max_workers is None:
        max_workers = mp.cpu_count()

    total_size_mb = sum(f.stat().st_size for f in files) / (1024 * 1024)
    print(f"Parsing {len(files)} PDFs ({total_size_mb:.1f} MB total) using {max_workers} threads...")
    print(f"VLM pass: {'enabled' if use_vlm else 'disabled'}")
    print()

    parsed_results = {}
    parse_start = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(parse_pdf, f, use_vlm): f for f in files
        }

        for future in as_completed(future_to_file):
            result = future.result()
            if result:
                filename, chunks = result
                parsed_results[filename] = chunks

    parse_elapsed = time.time() - parse_start
    print(f"\nParsing complete: {parse_elapsed:.1f}s")
    print()

    conn = duckdb.connect(DB_PATH)
    init_schema(conn)

    total_chunks = 0
    print("Writing to database...")
    for i, f in enumerate(files, 1):
        if f.name in parsed_results:
            chunks = parsed_results[f.name]
            total_chunks += ingest_file(
                f,
                conn,
                chunks,
                game_system=game_system,
                content_type=content_type,
                tags=tags,
            )
        print(f"  {i}/{len(files)} files written")

    conn.close()
    overall_elapsed = time.time() - overall_start
    print(f"\nDone in {overall_elapsed:.1f}s total:")
    print(f"   {len(files)} files, {total_chunks} total chunks ingested")
    if len(files) > 0:
        print(f"   {total_chunks / overall_elapsed:.0f} chunks/sec")


def run(
    game_system: str | None = None,
    content_type: str | None = None,
    tags: str | None = None,
    max_workers: int | None = None,
    use_vlm: bool = True,
) -> None:
    """Entrypoint."""
    ingest_all(
        game_system=game_system,
        content_type=content_type,
        tags=tags,
        max_workers=max_workers,
        use_vlm=use_vlm,
    )


if __name__ == "__main__":
    run(game_system="D&D 2e", content_type="rules")
