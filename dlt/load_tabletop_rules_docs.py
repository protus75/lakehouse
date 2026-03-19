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
import yaml

DB_PATH = "/workspace/db/lakehouse.duckdb"
DOCUMENTS_DIR = Path("/workspace/documents/tabletop_rules/raw")
CONFIGS_DIR = Path("/workspace/documents/tabletop_rules/configs")
OLLAMA_URL = "http://host.docker.internal:11434"
VLM_MODEL = "minicpm-v"


# ── Config loading ───────────────────────────────────────────────

def load_book_config(filepath: Path) -> dict:
    """Load the YAML config for a specific PDF.
    Looks for configs/{pdf_stem}.yaml, falls back to configs/_default.yaml.
    Merges book-specific config over defaults."""
    default_path = CONFIGS_DIR / "_default.yaml"
    book_path = CONFIGS_DIR / f"{filepath.stem}.yaml"

    config = {}
    if default_path.exists():
        with open(default_path) as f:
            config = yaml.safe_load(f) or {}

    if book_path.exists():
        with open(book_path) as f:
            book_config = yaml.safe_load(f) or {}
        config = _deep_merge(config, book_config)
        print(f"    Config loaded: {book_path.name}")
    else:
        print(f"    Config: using defaults (no {filepath.stem}.yaml found)")

    return config


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override dict into base dict. Override wins."""
    result = dict(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


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
            entry_title     VARCHAR,
            content         VARCHAR NOT NULL,
            char_count      INTEGER NOT NULL,
            chunk_type      VARCHAR DEFAULT 'content',
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


PAGE_SEPARATOR = "\n\n" + "-" * 48 + "\n\n"


def extract_with_marker(filepath: Path) -> list[str]:
    """Use Marker for layout-aware PDF to markdown conversion.
    Returns a list of markdown strings, one per page."""
    from marker.converters.pdf import PdfConverter

    models = _get_marker_models()
    converter = PdfConverter(
        artifact_dict=models,
        config={"paginate_output": True},
    )
    rendered = converter(str(filepath))
    pages = rendered.markdown.split(PAGE_SEPARATOR)
    return [p.strip() for p in pages if p.strip()]


# ── Pass 2: VLM extraction for structured content ───────────────

def detect_incomplete_pages(markdown: str, filepath: Path, config: dict) -> list[int]:
    """Identify pages that likely have structured content Marker may have
    mangled — stat blocks, ability tables, multi-column key:value pairs.
    Compares Marker output against pymupdf raw text per page to find
    pages where structured fields were dropped."""
    patterns = config.get("vlm_detection_patterns", [])
    if not patterns:
        return []

    doc = fitz.open(str(filepath))
    incomplete_pages = []

    for page_num in range(len(doc)):
        page = doc[page_num]
        raw_text = page.get_text("text")

        has_stat_block = any(
            re.search(pat, raw_text, re.IGNORECASE) for pat in patterns
        )
        if not has_stat_block:
            continue

        fields_in_raw = set()
        for pat in patterns:
            for m in re.finditer(pat, raw_text, re.IGNORECASE):
                fields_in_raw.add(m.group().split(":")[0].strip().lower())

        fields_in_markdown = set()
        for pat in patterns:
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
    config: dict,
) -> str:
    """Merge VLM-extracted structured fields into the Marker markdown.
    For each VLM page, find stat-block fields that are in the VLM output
    but missing from the corresponding markdown section, and inject them."""
    if not vlm_pages:
        return markdown

    patterns = config.get("vlm_detection_patterns", [])
    doc = fitz.open(str(filepath))

    for page_num, vlm_text in vlm_pages.items():
        vlm_fields = []
        for pat in patterns:
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


# ── Chapter-aligned chunking (page-number based) ────────────────

def extract_toc_entries(filepath: Path, config: dict) -> dict:
    """Extract chapter/appendix titles with page numbers and table titles
    from the PDF table of contents using pymupdf.
    Uses config['toc'] for patterns and scan range."""
    toc_config = config.get("toc", {})
    chapter_patterns = toc_config.get("chapter_patterns", [r"(?:Chapter|Appendix)\s+\d*\s*:?\s*[A-Za-z].*"])
    table_pattern = toc_config.get("table_pattern", r"Table\s+\d+\s*:?\s*[A-Za-z].*")
    scan_pages = toc_config.get("toc_scan_pages", 15)

    doc = fitz.open(str(filepath))
    chapters = []
    tables = []
    seen_chapters = set()
    seen_tables = set()

    for page_num in range(min(scan_pages, len(doc))):
        text = doc[page_num].get_text("text")
        for line in text.split("\n"):
            for chap_pat in chapter_patterns:
                m = re.match(
                    r"(" + chap_pat + r")\s*(?:\.[\s.]*){2,}\s*(\d+)\s*$",
                    line.strip(),
                    re.IGNORECASE,
                )
                if m:
                    title = m.group(1).strip()
                    title = re.sub(r"\s*(?:\.[\s.]*){2,}.*", "", title).strip()
                    pg = int(m.group(2))
                    if title not in seen_chapters:
                        seen_chapters.add(title)
                        chapters.append((title, pg))

            if table_pattern:
                m = re.match(
                    r"(" + table_pattern + r")\s*(?:\.[\s.]*){2,}\s*(\d+)\s*$",
                    line.strip(),
                    re.IGNORECASE,
                )
                if m:
                    title = m.group(1).strip()
                    title = re.sub(r"\s*(?:\.[\s.]*){2,}.*", "", title).strip()
                    if title not in seen_tables:
                        seen_tables.add(title)
                        tables.append(title)

    doc.close()
    chapters.sort(key=lambda x: x[1])
    print(f"    ToC: found {len(chapters)} chapters/appendices, {len(tables)} tables")
    for title, pg in chapters:
        print(f"      page {pg:>3d}: {title}")
    return {"chapters": chapters, "tables": tables}


def _clean_heading(text: str) -> str:
    """Strip markdown bold markers and extra whitespace from a heading."""
    return re.sub(r"\*+", "", text).strip()


def _is_index_heading(heading: str, config: dict) -> bool:
    """Check if a heading marks the start of a book index or spell index."""
    index_headings = set(h.lower() for h in config.get("index_headings", ["index"]))
    clean = _clean_heading(heading).lower()
    if clean in index_headings:
        return True
    return "index" in clean and clean.endswith("index")


def _chapter_for_page(page_num: int, chapter_pages: list[tuple[str, int]]) -> str | None:
    """Given a page number, return which chapter it belongs to.
    chapter_pages is sorted by page number ascending."""
    current = None
    for title, start_page in chapter_pages:
        if start_page > page_num:
            break
        current = title
    return current


def _build_page_to_chapter(
    filepath: Path,
    chapter_pages: list[tuple[str, int]],
    config: dict,
) -> dict[int, str]:
    """Build a mapping of PDF page index to chapter title.
    Reads the printed page number directly from each page's text content,
    then maps it to the chapter range from the ToC."""
    page_num_pattern = config.get("toc", {}).get("page_number_pattern", r"^\d{1,3}$")
    doc = fitz.open(str(filepath))
    mapping = {}

    for page_idx in range(len(doc)):
        printed_page = _read_page_number(doc[page_idx], page_idx, page_num_pattern)
        mapping[page_idx] = _chapter_for_page(printed_page, chapter_pages)

    doc.close()
    return mapping


def _read_page_number(page, page_idx: int, page_num_pattern: str = r"^\d{1,3}$") -> int:
    """Read the printed page number from a PDF page's text.
    Searches near the end of the text first (bottom of page), then the top.
    Falls back to the PDF page index if no printed number is found."""
    text = page.get_text("text")
    lines = text.split("\n")

    # Search from the end — page numbers are typically near the bottom
    for line in reversed(lines):
        stripped = line.strip()
        if stripped and re.match(page_num_pattern, stripped):
            return int(re.search(r"\d+", stripped).group())

    # Fallback: also check the first few lines (some layouts put page number at top)
    for line in lines[:5]:
        stripped = line.strip()
        if stripped and re.match(page_num_pattern, stripped):
            return int(re.search(r"\d+", stripped).group())

    return page_idx


def parse_book_structure(
    pages: list[str],
    filepath: Path,
    toc_entries: dict,
    config: dict,
) -> list[dict]:
    """Parse per-page markdown into chapter-aligned entries.

    Each page's chapter is determined by reading its printed page number
    directly from the PDF and looking it up in the ToC chapter ranges.
    No guessing, no regex matching, no offset math.

    Stops processing at Index headings.
    Returns a flat list of entries with chapter/section/entry context."""
    chapter_pages = toc_entries["chapters"]
    toc_tables = toc_entries["tables"]
    table_names_lower = {
        re.sub(r"^table\s+\d+\s*:?\s*", "", t, flags=re.IGNORECASE).strip().lower()
        for t in toc_tables
    }

    # Build page-to-chapter mapping from actual printed page numbers
    page_chapter_map = _build_page_to_chapter(filepath, chapter_pages, config)

    entries = []
    current_section = None
    current_entry_title = None
    current_content = []
    current_chapter = None
    hit_index = False

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

    for page_idx, page_md in enumerate(pages):
        if hit_index:
            break

        # Get this page's chapter directly from the page number
        chapter = page_chapter_map.get(page_idx)
        if chapter:
            current_chapter = chapter

        for line in page_md.split("\n"):
            h_match = re.match(r"^(#{1,4})\s+(.+)", line)

            if h_match:
                heading_text = h_match.group(2).strip()
                heading_level = len(h_match.group(1))

                if _is_index_heading(heading_text, config):
                    flush_entry()
                    print(f"    Skipping index section: '{_clean_heading(heading_text)}'")
                    hit_index = True
                    break

                flush_entry()

                clean = _clean_heading(heading_text)
                clean_lower = clean.lower()

                is_table = clean_lower in table_names_lower
                if is_table:
                    for t in toc_tables:
                        name = re.sub(r"^table\s+\d+\s*:?\s*", "", t, flags=re.IGNORECASE).strip().lower()
                        if name == clean_lower:
                            current_entry_title = t
                            break
                elif heading_level <= 2:
                    current_section = clean
                    current_entry_title = None
                else:
                    current_entry_title = clean

                current_content = [line]
            else:
                current_content.append(line)

    if not hit_index:
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


# ── Enrichment: LLM helper ──────────────────────────────────────

def call_llm(
    prompt: str,
    model: str = "llama3:70b",
    timeout: int = 180,
    max_retries: int = 2,
) -> str | None:
    """Call Ollama LLM with retry. Returns response text or None on failure."""
    for attempt in range(max_retries + 1):
        try:
            response = requests.post(
                f"{OLLAMA_URL}/api/generate",
                json={"model": model, "prompt": prompt, "stream": False},
                timeout=timeout,
            )
            response.raise_for_status()
            return response.json()["response"]
        except Exception as e:
            if attempt < max_retries:
                print(f"      LLM retry {attempt + 1}/{max_retries}: {e}")
            else:
                print(f"      LLM failed after {max_retries + 1} attempts: {e}")
                return None


# ── Enrichment: Spell detection (no LLM) ────────────────────────

def detect_spell_entries(entries: list[dict], config: dict) -> list[dict]:
    """Identify spell entries and parse their stat block metadata using config.
    Returns only entries that are spells, augmented with spell_meta dict."""
    entry_types = config.get("entry_types", {})
    spell_config = entry_types.get("spell")
    if not spell_config:
        return []

    detect_fields = [r"\b" + f for f in spell_config.get("detect_fields", [])]
    detect_min = spell_config.get("detect_min_fields", 3)
    class_fields = [r"\b" + f for f in spell_config.get("detect_class_fields", [])]
    chapter_keywords = spell_config.get("chapter_keywords", [])
    meta_patterns = {k: re.compile(r"\b" + v, re.IGNORECASE) for k, v in spell_config.get("metadata", {}).items()}
    class_rules = spell_config.get("class_rules", [])
    level_patterns = spell_config.get("level_patterns", {})

    spells = []
    for entry in entries:
        content = entry.get("content", "")
        chapter = (entry.get("chapter_title") or "").lower()

        field_count = sum(1 for pat in detect_fields if re.search(pat, content, re.IGNORECASE))
        has_class_field = any(re.search(pat, content, re.IGNORECASE) for pat in class_fields)

        is_keyword_chapter = any(kw.lower() in chapter for kw in chapter_keywords)
        is_spell = (field_count >= detect_min and has_class_field) or (is_keyword_chapter and field_count >= 2)

        if not is_spell:
            continue

        meta = {}
        for key, pattern in meta_patterns.items():
            m = pattern.search(content)
            if m:
                meta[key] = m.group(1).strip()

        # Determine entry class from config rules
        meta["spell_type"] = "Unknown"
        for rule in class_rules:
            if "chapter_contains" in rule and rule["chapter_contains"].lower() in chapter:
                meta["spell_type"] = rule["value"]
                break
            if "has_field" in rule and meta.get(rule["has_field"]):
                meta["spell_type"] = rule["value"]
                break
            if "default" in rule:
                meta["spell_type"] = rule["default"]

        # Extract level from section title
        section = (entry.get("section_title") or "").lower()
        for word, num in level_patterns.items():
            if word.lower() in section:
                meta["level"] = num
                break

        spell_entry = dict(entry)
        spell_entry["is_spell"] = True
        spell_entry["spell_meta"] = meta
        spell_entry["spell_name"] = entry.get("entry_title") or entry.get("section_title") or "Unknown"
        spells.append(spell_entry)

    return spells


# ── Enrichment: Clean table extraction (no LLM) ─────────────────

def extract_clean_tables(entries: list[dict], toc_tables: list[str]) -> list[dict]:
    """Find entries matching named ToC tables and store as table-typed chunks."""
    table_names_lower = {}
    for t in toc_tables:
        name = re.sub(r"^table\s+\d+\s*:?\s*", "", t, flags=re.IGNORECASE).strip().lower()
        if name:
            table_names_lower[name] = t

    table_chunks = []
    for entry in entries:
        title = _clean_heading(entry.get("entry_title") or "").lower()
        if title in table_names_lower:
            table_chunks.append({
                "chapter_title": entry.get("chapter_title"),
                "section_title": entry.get("section_title"),
                "entry_title": table_names_lower[title],
                "content": entry["content"],
                "chunk_type": "table",
            })

    return table_chunks


# ── Enrichment: Parse Appendix 5/6 indexes (no LLM) ─────────────

def parse_appendix_indexes(entries: list[dict], config: dict) -> list[dict]:
    """Parse appendix content into cross-reference chunks based on config rules."""
    cross_ref_config = config.get("cross_references", {})
    appendix_rules = cross_ref_config.get("appendix_indexes", [])
    if not appendix_rules:
        return []

    index_chunks = []
    for entry in entries:
        chapter = (entry.get("chapter_title") or "").lower()
        content = entry["content"]
        section = entry.get("section_title") or entry.get("entry_title")
        if not section:
            continue

        clean_section = _clean_heading(section)

        for rule in appendix_rules:
            keyword = rule.get("chapter_contains", "").lower()
            if keyword and keyword in chapter:
                label = rule["label_template"].format(section=clean_section)
                index_chunks.append({
                    "chapter_title": entry.get("chapter_title"),
                    "section_title": clean_section,
                    "entry_title": label,
                    "content": content,
                    "chunk_type": "cross_reference",
                })
                break

    return index_chunks


# ── Enrichment: Section summaries (LLM) ─────────────────────────

def generate_section_summaries(
    entries: list[dict],
    config: dict,
    model: str = "llama3:70b",
    batch_max_chars: int = 3000,
) -> list[dict]:
    """Generate LLM summaries for each named ToC subsection using config prompt."""
    from collections import OrderedDict

    prompt_template = config.get("prompts", {}).get("section_summary", "")
    if not prompt_template:
        return []

    book_title = config.get("book", {}).get("title", "this book")
    game_system = config.get("book", {}).get("game_system", "")

    groups = OrderedDict()
    for entry in entries:
        key = (entry.get("chapter_title") or "", entry.get("section_title") or "")
        if not key[1]:
            continue
        if key not in groups:
            groups[key] = []
        groups[key].append(entry["content"])

    summaries = []
    total = len(groups)
    print(f"      Generating summaries for {total} sections...")

    for i, ((chapter, section), contents) in enumerate(groups.items()):
        combined = "\n\n".join(contents)[:batch_max_chars]

        prompt = prompt_template.format(
            book_title=book_title,
            game_system=game_system,
            section=section,
            chapter=chapter,
            content=combined,
        )

        result = call_llm(prompt, model=model)
        if result:
            summaries.append({
                "chapter_title": chapter,
                "section_title": section,
                "entry_title": f"Summary: {section}",
                "content": f"Summary of '{section}' ({chapter}):\n{result.strip()}",
                "chunk_type": "summary",
            })
            if (i + 1) % 10 == 0:
                print(f"      {i + 1}/{total} section summaries generated")

    print(f"      {len(summaries)}/{total} section summaries completed")
    return summaries


# ── Enrichment: Spell summaries (LLM) ───────────────────────────

def generate_spell_summaries(
    spell_entries: list[dict],
    config: dict,
    model: str = "llama3:70b",
    batch_size: int = 5,
) -> list[dict]:
    """Generate structured spell summaries using config prompt template."""
    prompt_template = config.get("prompts", {}).get("entry_summary", "")
    if not prompt_template:
        return []

    game_system = config.get("book", {}).get("game_system", "")

    summaries = []
    total = len(spell_entries)
    print(f"      Generating summaries for {total} spells (batches of {batch_size})...")

    for batch_start in range(0, total, batch_size):
        batch = spell_entries[batch_start:batch_start + batch_size]

        spell_texts = []
        for s in batch:
            name = s.get("spell_name", "Unknown")
            content = s["content"][:600]
            spell_texts.append(f"SPELL: {name}\n{content}")

        prompt = prompt_template.format(
            game_system=game_system,
            spells="\n---\n".join(spell_texts),
        )

        result = call_llm(prompt, model=model, timeout=240)
        if result:
            # Parse individual spell summaries from batch response
            spell_blocks = re.split(r"(?=SPELL:\s)", result.strip())
            for block in spell_blocks:
                block = block.strip()
                if not block:
                    continue
                # Match back to a spell entry by name
                name_match = re.match(r"SPELL:\s*(.+?)(?:\n|$)", block)
                if not name_match:
                    continue
                summary_name = name_match.group(1).strip()

                # Find the matching spell entry
                matched_entry = None
                for s in batch:
                    if s["spell_name"].lower().rstrip("*").strip() == summary_name.lower().rstrip("*").strip():
                        matched_entry = s
                        break
                if not matched_entry:
                    # Fuzzy match: check if name is contained
                    for s in batch:
                        if summary_name.lower()[:20] in s["spell_name"].lower() or s["spell_name"].lower()[:20] in summary_name.lower():
                            matched_entry = s
                            break
                if not matched_entry and batch:
                    matched_entry = batch[0]

                if matched_entry:
                    summaries.append({
                        "chapter_title": matched_entry.get("chapter_title"),
                        "section_title": matched_entry.get("section_title"),
                        "entry_title": f"Summary: {matched_entry['spell_name']}",
                        "content": block,
                        "chunk_type": "summary",
                    })

        batch_num = batch_start // batch_size + 1
        total_batches = (total + batch_size - 1) // batch_size
        if batch_num % 5 == 0:
            print(f"      Batch {batch_num}/{total_batches} complete ({len(summaries)} summaries so far)")

    print(f"      {len(summaries)}/{total} spell summaries completed")
    return summaries


# ── Enrichment: Cross-reference indexes (no LLM) ────────────────

def build_generated_indexes(spell_entries: list[dict], config: dict) -> list[dict]:
    """Build cross-reference index chunks from config-defined groupings."""
    from collections import defaultdict

    cross_ref_config = config.get("cross_references", {})
    index_defs = cross_ref_config.get("generated_indexes", [])
    if not index_defs:
        return []

    chunks = []
    for index_def in index_defs:
        group_by = index_def.get("group_by", [])
        label_template = index_def.get("label_template", "")
        chapter_template = index_def.get("chapter_template", "Cross-Reference")

        if "alpha" in group_by:
            # Alphabetical index
            by_letter = defaultdict(list)
            for s in spell_entries:
                name = s["spell_name"]
                meta = s.get("spell_meta", {})
                letter = name[0].upper() if name else "?"
                spell_type = meta.get("spell_type", "")
                level = meta.get("level", "?")
                by_letter[letter].append(f"- {name} ({spell_type}, Level {level})")

            for letter in sorted(by_letter.keys()):
                entries_list = sorted(by_letter[letter])
                chunks.append({
                    "chapter_title": chapter_template,
                    "section_title": f"Spells: {letter}",
                    "entry_title": label_template.format(letter=letter),
                    "content": f"Spells starting with {letter}:\n\n" + "\n".join(entries_list),
                    "chunk_type": "cross_reference",
                })
        else:
            # Group by metadata fields (e.g. class + level)
            groups = defaultdict(list)
            for s in spell_entries:
                meta = s.get("spell_meta", {})
                key_parts = []
                for field in group_by:
                    val = meta.get("spell_type" if field == "class" else field)
                    if val is None:
                        break
                    key_parts.append((field, val))
                if len(key_parts) == len(group_by):
                    groups[tuple(key_parts)].append(s["spell_name"])

            for key_tuple, names in sorted(groups.items()):
                names.sort()
                spell_list = "\n".join(f"- {name}" for name in names)
                fmt = {field: val for field, val in key_tuple}
                fmt["class"] = fmt.get("class", "")
                label = label_template.format(**fmt)
                chapter = chapter_template.format(**fmt)
                section = f"Level {fmt.get('level', '')}" if "level" in fmt else label
                chunks.append({
                    "chapter_title": chapter,
                    "section_title": section,
                    "entry_title": label,
                    "content": f"{label}\n\n{spell_list}",
                    "chunk_type": "cross_reference",
                })

    return chunks


# ── Enrichment: Pipeline orchestration ───────────────────────────

def enrich_chunks(
    entries: list[dict],
    chunks: list[dict],
    toc_entries: dict,
    config: dict,
    model: str = "llama3:70b",
) -> list[dict]:
    """Run full enrichment pipeline using config-driven detection and prompts.
    Returns all enrichment chunks to append to the main chunk list."""
    enriched = []

    print(f"    Pass 3a: Detecting entry types...")
    spell_entries = detect_spell_entries(entries, config)
    print(f"      Found {len(spell_entries)} spell entries")

    print(f"    Pass 3b: Extracting named tables...")
    table_chunks = extract_clean_tables(entries, toc_entries.get("tables", []))
    enriched.extend(table_chunks)
    print(f"      Extracted {len(table_chunks)} table chunks")

    print(f"    Pass 3c: Parsing appendix indexes...")
    appendix_chunks = parse_appendix_indexes(entries, config)
    enriched.extend(appendix_chunks)
    print(f"      Parsed {len(appendix_chunks)} appendix index chunks")

    print(f"    Pass 3d: Generating section summaries (LLM)...")
    section_summaries = generate_section_summaries(entries, config, model=model)
    enriched.extend(section_summaries)

    print(f"    Pass 3e: Generating spell summaries (LLM)...")
    spell_summaries = generate_spell_summaries(spell_entries, config, model=model)
    enriched.extend(spell_summaries)

    print(f"    Pass 3f: Building cross-reference indexes...")
    index_chunks = build_generated_indexes(spell_entries, config)
    enriched.extend(index_chunks)
    print(f"      Built {len(index_chunks)} index chunks")

    print(f"    Enrichment complete: {len(enriched)} chunks added")
    return enriched


# ── PDF parsing pipeline ────────────────────────────────────────

def parse_pdf(
    filepath: Path,
    use_vlm: bool = True,
    enrich: bool = True,
    llm_model: str = "llama3:70b",
) -> tuple[str, list[dict]] | None:
    """Parse a single PDF using multi-pass hybrid extraction + enrichment.

    Pass 1: Marker for layout-aware markdown
    Pass 2: VLM via Ollama for pages with incomplete structured content
    Pass 3: Enrichment — summaries, tables, cross-references (if enrich=True)
    """
    import time
    try:
        start = time.time()
        file_size_mb = filepath.stat().st_size / (1024 * 1024)
        print(f"  Parsing {filepath.name} ({file_size_mb:.1f} MB)...")

        config = load_book_config(filepath)
        chunking = config.get("chunking", {})

        print(f"    Pass 1: Marker extraction...")
        pages = extract_with_marker(filepath)
        total_chars = sum(len(p) for p in pages)
        print(f"    Pass 1 complete: {len(pages)} pages, {total_chars:,} chars")

        if use_vlm:
            merged = "\n\n".join(pages)
            print(f"    Pass 2: Detecting pages needing VLM...")
            incomplete = detect_incomplete_pages(merged, filepath, config)
            if incomplete:
                print(f"    Pass 2: VLM extracting {len(incomplete)} pages...")
                vlm_results = extract_pages_with_vlm(filepath, incomplete)
                merged = merge_vlm_into_markdown(merged, vlm_results, filepath, config)
                # Re-split isn't needed — VLM supplements go into merged text
                # but pages list stays as-is for chapter mapping
                print(f"    Pass 2 complete: supplemented {len(vlm_results)} pages")
            else:
                print(f"    Pass 2: No incomplete pages detected, skipping VLM")

        print(f"    Extracting table of contents...")
        toc_entries = extract_toc_entries(filepath, config)
        entries = parse_book_structure(pages, filepath, toc_entries, config)
        chunks = chunk_entries(
            entries,
            max_chars=chunking.get("max_chars", 800),
            overlap=chunking.get("overlap", 200),
        )
        print(f"    Content chunks: {len(chunks)}")

        if enrich:
            print(f"    Pass 3: Enrichment pipeline...")
            enriched = enrich_chunks(entries, chunks, toc_entries, config, model=llm_model)
            chunks.extend(enriched)

        elapsed = time.time() - start
        print(f"    Done in {elapsed:.1f}s -> {len(chunks)} total chunks")
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
               (chunk_id, source_file, chapter_title, section_title, entry_title,
                content, char_count, chunk_type, parsed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                max_id + i + 1,
                filepath.name,
                chunk.get("chapter_title"),
                chunk.get("section_title"),
                chunk.get("entry_title"),
                chunk["content"],
                len(chunk["content"]),
                chunk.get("chunk_type", "content"),
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
    enrich: bool = True,
) -> None:
    """Parse all PDFs in documents/tabletop_rules/raw directory.

    Args:
        directory: Override directory to scan
        game_system: e.g., "D&D 2e", "Pathfinder 2e"
        content_type: e.g., "rules", "module", "campaign"
        tags: comma-separated tags for categorization
        max_workers: Number of cores to use (default: all available)
        use_vlm: Enable Pass 2 VLM extraction for structured content
        enrich: Enable Pass 3 enrichment (summaries, tables, cross-references)
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
    print(f"Enrichment: {'enabled' if enrich else 'disabled'}")
    print()

    parsed_results = {}
    parse_start = time.time()
    # Run sequentially when enrichment is enabled (LLM calls aren't thread-safe)
    if enrich:
        for f in files:
            result = parse_pdf(f, use_vlm=use_vlm, enrich=enrich)
            if result:
                filename, chunks = result
                parsed_results[filename] = chunks
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {
                executor.submit(parse_pdf, f, use_vlm, False): f for f in files
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
    enrich: bool = True,
) -> None:
    """Entrypoint."""
    ingest_all(
        game_system=game_system,
        content_type=content_type,
        tags=tags,
        max_workers=max_workers,
        use_vlm=use_vlm,
        enrich=enrich,
    )


if __name__ == "__main__":
    run(game_system="D&D 2e", content_type="rules")
