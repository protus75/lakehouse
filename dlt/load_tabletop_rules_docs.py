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
        print(f"  Config: {book_path.name}")
    else:
        print(f"  Config: defaults")
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


# ── Step 1: Parse ToC ────────────────────────────────────────────

def parse_toc(filepath: Path, config: dict) -> dict:
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
            for pat in chapter_patterns:
                m = re.match(r"(" + pat + r")\s*(?:\.[\s.]*){2,}\s*(\d+)\s*$",
                             stripped, re.IGNORECASE)
                if m:
                    title = re.sub(r"\s*(?:\.[\s.]*){2,}.*", "", m.group(1)).strip()
                    page = int(m.group(2))
                    if title and title not in seen:
                        seen.add(title)
                        sections.append({
                            "title": title, "page_start": page,
                            "is_excluded": title.lower() in exclude_set,
                            "sub_headings": [], "tables": [],
                        })
            if table_pattern:
                m = re.match(r"(" + table_pattern + r")\s*(?:\.[\s.]*){2,}\s*(\d+)\s*$",
                             stripped, re.IGNORECASE)
                if m:
                    title = re.sub(r"\s*(?:\.[\s.]*){2,}.*", "", m.group(1)).strip()
                    page = int(m.group(2))
                    if title and title not in seen:
                        seen.add(title)
                        tables.append({"title": title, "page_number": page})
    doc.close()

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
    print(f"  ToC: {included} sections, {excluded} excluded, {len(tables)} tables")
    return {"sections": sections, "tables": tables}


# ── Step 2: Page number → chapter map (pymupdf) ─────────────────

def _read_page_number(page, page_idx: int, pattern: str) -> int:
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


def build_page_chapter_map(filepath: Path, toc_sections: list[dict], config: dict) -> dict:
    """Map PDF page index → ToC section dict (or None for excluded/unmapped)."""
    page_pattern = config.get("toc", {}).get("page_number_pattern", r"^\d{1,3}$")
    included = [s for s in toc_sections if not s["is_excluded"]]

    doc = fitz.open(str(filepath))
    page_map = {}
    for page_idx in range(len(doc)):
        printed = _read_page_number(doc[page_idx], page_idx, page_pattern)
        matched = None
        for entry in included:
            if entry["page_start"] <= printed <= entry["page_end"]:
                matched = entry
                break
        page_map[page_idx] = matched
    doc.close()
    return page_map


# ── Step 3: Marker extraction ───────────────────────────────────

_marker_models = None

def _get_marker_models():
    global _marker_models
    if _marker_models is None:
        from marker.models import create_model_dict
        _marker_models = create_model_dict()
    return _marker_models


def extract_marker_pages(filepath: Path) -> dict[int, str]:
    """Run Marker with pagination to get per-page markdown.
    Returns dict mapping page_index -> markdown content."""
    from marker.converters.pdf import PdfConverter

    models = _get_marker_models()
    converter = PdfConverter(
        artifact_dict=models,
        config={"paginate_output": True},
    )
    rendered = converter(str(filepath))
    md = rendered.markdown

    # Marker inserts {page_number} markers at page boundaries
    # Split on these markers and build a page_index -> content map
    pages = {}
    parts = re.split(r"\{(\d+)\}", md)
    # parts = [pre_text, page_num, content, page_num, content, ...]
    for i in range(1, len(parts), 2):
        page_idx = int(parts[i])
        content = parts[i + 1].strip() if i + 1 < len(parts) else ""
        if content:
            pages[page_idx] = content

    # Fallback: if no page markers found, treat as single page
    if not pages:
        pages[0] = md

    return pages


# ── Step 4: Merge Marker pages with chapter assignments ─────────

def _detect_watermarks(pages: dict[int, str], threshold: float = 0.3) -> set[str]:
    """Detect repeated lines across many pages."""
    line_counts = {}
    total = len(pages)
    for page_md in pages.values():
        seen = set()
        for line in page_md.split("\n"):
            stripped = line.strip()
            if stripped and len(stripped) > 2 and stripped not in seen:
                seen.add(stripped)
                line_counts[stripped] = line_counts.get(stripped, 0) + 1
    min_count = max(int(total * threshold), 3)
    watermarks = {line for line, count in line_counts.items() if count >= min_count}
    if watermarks:
        print(f"  Watermarks: {len(watermarks)} patterns detected")
    return watermarks


def merge_pages_with_chapters(
    marker_pages: dict[int, str],
    page_map: dict,
    config: dict,
) -> list[dict]:
    """Combine Marker markdown pages with pymupdf chapter assignments.
    marker_pages is {page_index: markdown}, page_map is {page_index: toc_entry}.
    Returns list of {toc_entry, page_number, markdown} for included pages."""

    watermarks = _detect_watermarks(marker_pages)
    page_pattern = config.get("toc", {}).get("page_number_pattern", r"^\d{1,3}$")

    merged = []
    for page_idx in sorted(marker_pages.keys()):
        page_md = marker_pages[page_idx]
        toc_entry = page_map.get(page_idx)
        if toc_entry is None:
            continue

        lines = []
        for line in page_md.split("\n"):
            if line.strip() in watermarks:
                continue
            if line.strip() and re.match(page_pattern, line.strip()):
                continue
            lines.append(line)

        text = "\n".join(lines)
        # Remove Marker image references
        text = re.sub(r"!\[.*?\]\(.*?\)", "", text)
        # Rejoin hyphenated words
        text = re.sub(r"(\w)-\s*\n\s*(\w)", r"\1\2", text)

        merged.append({
            "toc_entry": toc_entry,
            "page_number": page_idx,
            "markdown": text.strip(),
        })

    print(f"  Merged: {len(merged)} pages with chapters")
    return merged


# ── Step 5: Build entries from Marker headings ───────────────────

def _should_strip_line(stripped: str, config: dict) -> bool:
    """Check if a line should be stripped from entry content based on config patterns.
    Used to remove school/type annotations, tags, etc. that are metadata not content."""
    strip_patterns = config.get("strip_content_patterns", [])
    for pattern in strip_patterns:
        if re.match(pattern, stripped, re.IGNORECASE):
            return True
    return False


def build_entries(merged_pages: list[dict], known_entries: set[str], config: dict = None) -> list[dict]:
    """Parse Marker markdown headings into entries.
    Marker already marks headings with #. We just split on them.
    known_entries whitelist filters false headings in spell sections."""
    entries = []

    # Group pages by ToC section
    sections = {}
    for page in merged_pages:
        title = page["toc_entry"]["title"]
        if title not in sections:
            sections[title] = []
        sections[title].append(page)

    for toc_title, section_pages in sections.items():
        toc_entry = section_pages[0]["toc_entry"]
        current_section = None
        current_entry = None
        current_content = []
        current_pages = []

        def flush():
            nonlocal current_content, current_pages
            if current_content:
                content = "\n".join(current_content).strip()
                if content and len(content) > 10:
                    entries.append({
                        "toc_entry": toc_entry,
                        "section_title": current_section,
                        "entry_title": current_entry,
                        "content": content,
                        "page_numbers": sorted(set(current_pages)),
                    })
            current_content = []
            current_pages = []

        for page in section_pages:
            page_num = page["page_number"]

            for line in page["markdown"].split("\n"):
                # Detect Marker headings (# ## ### ####)
                h_match = re.match(r"^(#{1,4})\s+(.+)", line)

                if h_match:
                    level = len(h_match.group(1))
                    heading = h_match.group(2).strip()
                    # Strip bold markers from heading
                    clean_heading = re.sub(r"\*+", "", heading).strip()

                    if level <= 2:
                        # Section heading (## level)
                        flush()
                        current_section = clean_heading
                        current_entry = None
                        current_content = [line]
                        current_pages = [page_num]
                    else:
                        # Entry heading (### or ####)
                        # Strip trailing school/type in parens for matching
                        # e.g. "Endure Cold/Endure Heat (Alteration)" -> "Endure Cold/Endure Heat"
                        match_name = re.sub(r"\s*\([\w/,\s]+\)\s*$", "", clean_heading).strip()

                        if known_entries and match_name.lower() not in known_entries:
                            # Not a known entry — keep as content
                            current_content.append(line)
                            if page_num not in current_pages:
                                current_pages.append(page_num)
                        else:
                            flush()
                            current_entry = match_name
                            current_content = [line]
                            current_pages = [page_num]
                else:
                    stripped = line.strip()
                    # Skip Marker image references (always)
                    if re.match(r"^!\[.*\]\(.*\)$", stripped):
                        continue
                    # Skip lines matching config strip patterns
                    if config and _should_strip_line(stripped, config):
                        continue
                    current_content.append(line)
                    if page_num not in current_pages:
                        current_pages.append(page_num)

        flush()

    print(f"  Entries: {len(entries)} across {len(sections)} sections")

    # Post-process: fix page-boundary splits
    # If an entry has metadata fields but very little description,
    # and the NEXT entry starts with lowercase continuation text,
    # merge the next entry's content into this one
    META_INDICATORS = ["sphere:", "range:", "casting time:", "duration:", "components:"]
    i = 0
    merged_count = 0
    while i < len(entries) - 1:
        curr = entries[i]
        nxt = entries[i + 1]
        curr_content = curr["content"]
        nxt_content = nxt["content"]

        # Check if current entry has metadata but short total content
        has_meta = sum(1 for m in META_INDICATORS if m in curr_content.lower()) >= 2
        # Strip heading lines to check actual description length
        desc_lines = [l for l in curr_content.split("\n")
                      if l.strip() and not l.startswith("#")
                      and not any(m in l.lower() for m in META_INDICATORS)
                      and not re.match(r"^\([\w/,\s]+\)", l.strip())
                      and l.strip().lower() != "reversible"]
        desc_len = sum(len(l) for l in desc_lines)

        if has_meta and desc_len < 100:
            # Check if next entry is an orphan (no title, or starts with lowercase)
            nxt_lines = [l for l in nxt_content.split("\n") if l.strip()]
            nxt_first = nxt_lines[0].strip() if nxt_lines else ""
            is_orphan = (
                (not nxt["entry_title"])
                or (nxt_first and nxt_first[0].islower())
                or (nxt["entry_title"] == curr["entry_title"])
                or (nxt["toc_entry"]["title"] == curr["toc_entry"]["title"]
                    and not nxt_first.startswith("#")
                    and nxt_first and nxt_first[0].islower())
            )

            if is_orphan:
                # Merge next into current
                curr["content"] = curr_content + "\n\n" + nxt_content
                curr["page_numbers"] = sorted(set(curr["page_numbers"] + nxt["page_numbers"]))
                entries.pop(i + 1)
                merged_count += 1
                continue  # Re-check this entry with the next one

        i += 1

    if merged_count:
        print(f"  Fixed {merged_count} page-boundary splits")

    return entries


# ── Step 6: Extract known entry names from indexes ───────────────

def extract_known_entries(filepath: Path, toc_data: dict, config: dict) -> set[str]:
    """Get valid entry names from excluded index sections."""
    page_pattern = config.get("toc", {}).get("page_number_pattern", r"^\d{1,3}$")
    excluded = [s for s in toc_data["sections"] if s["is_excluded"]]
    if not excluded:
        return set()

    doc = fitz.open(str(filepath))
    names = set()
    for section in excluded:
        for page_idx in range(len(doc)):
            printed = _read_page_number(doc[page_idx], page_idx, page_pattern)
            if not (section["page_start"] <= printed <= section["page_end"]):
                continue
            text = doc[page_idx].get_text("text")
            for line in text.split("\n"):
                stripped = line.strip()
                if not stripped or len(stripped) < 3:
                    continue
                if re.match(page_pattern, stripped):
                    continue
                clean = re.sub(r"\s*(?:\.[\s.]*){2,}.*", "", stripped).strip()
                clean = re.sub(r"\s+\d+\s*$", "", clean).strip()
                if clean and 3 <= len(clean) <= 50 and clean[0].isupper():
                    names.add(clean.lower())
    doc.close()
    if names:
        print(f"  Known entries: {len(names)} from index sections")
    return names


# ── Step 7: Collect sub-headings per ToC section ─────────────────

def collect_sub_headings(merged_pages: list[dict], toc_sections: list[dict]) -> None:
    """Extract Marker ## and ### headings per ToC section for query routing."""
    section_pages = {}
    for page in merged_pages:
        title = page["toc_entry"]["title"]
        if title not in section_pages:
            section_pages[title] = []
        section_pages[title].append(page)

    for section in toc_sections:
        if section["is_excluded"]:
            continue
        headings = []
        for page in section_pages.get(section["title"], []):
            for line in page["markdown"].split("\n"):
                m = re.match(r"^#{1,4}\s+(.+)", line)
                if m:
                    clean = re.sub(r"\*+", "", m.group(1)).strip()
                    if clean and clean not in headings:
                        headings.append(clean)
        section["sub_headings"] = headings[:50]

    total = sum(len(s["sub_headings"]) for s in toc_sections if not s["is_excluded"])
    print(f"  Sub-headings: {total} collected")


# ── Step 8: Chunk entries ────────────────────────────────────────

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

    print(f"  Chunks: {len(chunks)}")
    return chunks


# ── Step 9: Store ────────────────────────────────────────────────

def store(filepath: Path, toc_data: dict, chunks: list[dict],
          game_system: str | None = None, content_type: str | None = None) -> None:
    conn = duckdb.connect(DB_PATH)
    init_schema(conn)
    now = datetime.now(timezone.utc)
    toc_sections = toc_data["sections"]

    conn.execute("DELETE FROM documents_tabletop_rules.chunks WHERE source_file = ?", [filepath.name])
    conn.execute("DELETE FROM documents_tabletop_rules.toc WHERE source_file = ?", [filepath.name])
    conn.execute("DELETE FROM documents_tabletop_rules.files WHERE source_file = ?", [filepath.name])

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

    conn.close()
    print(f"  Stored: {len(toc_sections)} ToC, {len(chunks)} chunks")


# ── Pipeline ─────────────────────────────────────────────────────

def parse_pdf(filepath: Path, game_system: str | None = None,
              content_type: str | None = None) -> None:
    import time
    start = time.time()
    print(f"\nParsing {filepath.name} ({filepath.stat().st_size / 1024 / 1024:.1f} MB)")

    config = load_config(filepath)

    # 1. Parse ToC
    toc_data = parse_toc(filepath, config)

    # 2. Page numbers → chapter map (pymupdf)
    page_map = build_page_chapter_map(filepath, toc_data["sections"], config)

    # 3. Marker extraction (per-page markdown)
    print(f"  Marker: extracting...")
    marker_pages = extract_marker_pages(filepath)
    print(f"  Marker: {len(marker_pages)} pages (page indices {min(marker_pages.keys())}-{max(marker_pages.keys())})")

    # 4. Merge Marker pages with chapter assignments
    merged = merge_pages_with_chapters(marker_pages, page_map, config)

    # 5. Known entry names from indexes
    known_entries = extract_known_entries(filepath, toc_data, config)

    # 6. Sub-headings for query routing
    collect_sub_headings(merged, toc_data["sections"])

    # 7. Build entries from Marker headings
    entries = build_entries(merged, known_entries, config)

    # 8. Chunk
    chunks = chunk_entries(entries, config)

    # 9. Store
    store(filepath, toc_data, chunks, game_system, content_type)

    print(f"  Done in {time.time() - start:.1f}s")


def run(game_system: str | None = None, content_type: str | None = None,
        directory: Path | None = None) -> None:
    doc_dir = directory or DOCUMENTS_DIR
    files = sorted(doc_dir.glob("*.pdf"))
    if not files:
        print(f"No PDFs in {doc_dir}")
        return
    print(f"Ingesting {len(files)} PDFs ({sum(f.stat().st_size for f in files) / 1024 / 1024:.1f} MB)")
    for f in files:
        parse_pdf(f, game_system=game_system, content_type=content_type)
    print(f"\nDone: {len(files)} files")


if __name__ == "__main__":
    run(game_system="D&D 2e", content_type="rules")
