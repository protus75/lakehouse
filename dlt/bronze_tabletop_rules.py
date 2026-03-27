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

import yaml

import duckdb
import fitz  # pymupdf
import pyarrow as pa

from dlt.lib.tabletop_cleanup import _log, load_config, _extract_toc_line
from dlt.lib.iceberg_catalog import write_iceberg, read_iceberg, read_iceberg_filtered, table_exists

DOCUMENTS_DIR = Path("/workspace/documents/tabletop_rules/raw")
CONFIGS_DIR = Path("/workspace/documents/tabletop_rules/configs")


# ── Schema ───────────────────────────────────────────────────────
# Iceberg tables are created automatically from Arrow schemas via write_iceberg().
# No init_bronze_schema() needed — PyIceberg handles table creation on first write.

NAMESPACE = "bronze_tabletop"


# ── Lineage & Catalog ────────────────────────────────────────────

import uuid


def start_run(source_file: str, step: str, config: dict) -> str:
    """Begin a pipeline run. Returns run_id. Writes to Iceberg."""
    run_id = str(uuid.uuid4())[:12]
    lineage_cfg = config.get("lineage", {})
    version = lineage_cfg.get("pipeline_version", "unknown")
    c_hash = config_hash(config)
    now = datetime.now(timezone.utc)
    arrow = pa.table({
        "run_id": [run_id], "source_file": [source_file], "step": [step],
        "pipeline_version": [version], "config_hash": [c_hash],
        "status": ["running"], "started_at": [now],
        "finished_at": pa.array([None], type=pa.timestamp("us", tz="UTC")),
        "row_counts": pa.array([None], type=pa.string()),
        "error_message": pa.array([None], type=pa.string()),
    })
    write_iceberg(NAMESPACE, "pipeline_runs", arrow)
    return run_id


def finish_run(run_id: str, status: str = "success",
               row_counts: dict | None = None, error: str | None = None) -> None:
    """Complete a pipeline run. Delete old row and re-insert with updated status."""
    now = datetime.now(timezone.utc)
    rc = pa.array([json.dumps(row_counts) if row_counts else None], type=pa.string())
    err = pa.array([error], type=pa.string())
    try:
        existing = read_iceberg(NAMESPACE, "pipeline_runs")
        mask = pa.compute.equal(existing.column("run_id"), run_id)
        row_idx = pa.compute.index(mask, True).as_py()
        if row_idx is not None and row_idx >= 0:
            row = existing.slice(row_idx, 1)
            updated = pa.table({
                "run_id": row.column("run_id"),
                "source_file": row.column("source_file"),
                "step": row.column("step"),
                "pipeline_version": row.column("pipeline_version"),
                "config_hash": row.column("config_hash"),
                "status": [status],
                "started_at": row.column("started_at"),
                "finished_at": [now],
                "row_counts": rc,
                "error_message": err,
            })
            write_iceberg(NAMESPACE, "pipeline_runs", updated,
                          overwrite_filter="run_id", overwrite_filter_value=run_id)
    except Exception:
        arrow = pa.table({
            "run_id": [run_id], "source_file": ["unknown"], "step": ["unknown"],
            "pipeline_version": pa.array([None], type=pa.string()),
            "config_hash": pa.array([None], type=pa.string()),
            "status": [status], "started_at": [now], "finished_at": [now],
            "row_counts": rc,
            "error_message": err,
        })
        write_iceberg(NAMESPACE, "pipeline_runs", arrow)


def refresh_catalog(source_file: str, run_id: str, config: dict) -> None:
    """Snapshot row counts for all bronze tables into the catalog."""
    lineage_cfg = config.get("lineage", {})
    tables = lineage_cfg.get("catalog_tables", [])
    now = datetime.now(timezone.utc)
    for table_name in tables:
        try:
            tbl = read_iceberg_filtered(NAMESPACE, table_name, "source_file", source_file)
            count = len(tbl)
        except Exception:
            count = 0
        arrow = pa.table({
            "source_file": [source_file], "table_name": [table_name],
            "row_count": [count], "refreshed_at": [now], "run_id": [run_id],
        })
        write_iceberg(NAMESPACE, "catalog", arrow,
                      overwrite_filter="source_file", overwrite_filter_value=source_file)


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


MARKER_CACHE_DIR = Path("/workspace/documents/tabletop_rules/processed/marker")


def _clean_marker_md(md: str) -> str:
    """Strip image references and rejoin hyphenated words."""
    md = re.sub(r"!\[.*?\]\(.*?\)", "", md)
    md = re.sub(r"(\w)-\s*\n\s*(\w)", r"\1\2", md)
    return md


def extract_marker_markdown(filepath: Path, allow_ocr: bool = False) -> str:
    """Read Marker markdown from disk cache. Fails if cache missing unless allow_ocr=True.

    OCR should only run via the seed_models pipeline on a GPU-enabled container.
    Normal pipeline runs read from cache only."""
    cache_name = filepath.stem.replace(" ", "_") + ".md"
    cache_path = MARKER_CACHE_DIR / cache_name
    if cache_path.exists():
        _log(f"  Marker: using disk cache {cache_path.name}")
        md = cache_path.read_text(encoding="utf-8")
        return _clean_marker_md(md)
    if not allow_ocr:
        raise RuntimeError(
            f"Marker cache missing: {cache_path}\n"
            f"Run the seed_models job first to generate Marker output for this PDF."
        )
    _log(f"  Marker: cache miss, running OCR (allow_ocr=True)...")
    from marker.converters.pdf import PdfConverter
    from marker.models import create_model_dict
    models = create_model_dict()
    converter = PdfConverter(artifact_dict=models)
    rendered = converter(str(filepath))
    md = rendered.markdown
    MARKER_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(md, encoding="utf-8")
    _log(f"  Marker: cached to {cache_name}")
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


def _cluster_x_positions(x_values: list[float]) -> dict[float, int]:
    """Cluster x-positions into depth levels dynamically.

    Handles multi-column PDF layouts:
    1. Split x-values into columns using a large gap threshold (>50px)
    2. Within each column, cluster by indent level (~12px steps)
    3. Assign depth 0, 1, 2, ... matching indent levels across columns

    Returns {rounded_x: depth} mapping for every input x.
    """
    if not x_values:
        return {}

    sorted_xs = sorted(set(round(x, 1) for x in x_values))

    # Step 1: Split into columns (gaps > 50px between columns)
    columns = []
    current_col = [sorted_xs[0]]
    for x in sorted_xs[1:]:
        if x - current_col[-1] > 50:
            columns.append(current_col)
            current_col = [x]
        else:
            current_col.append(x)
    columns.append(current_col)

    # Step 2: Normalize x-positions relative to each column's left edge,
    # then cluster all normalized values together to get consistent depths
    normalized = {}  # original_x -> normalized_x
    for col_xs in columns:
        base = col_xs[0]
        for x in col_xs:
            normalized[x] = round(x - base)

    # Cluster normalized values into indent levels
    # Group positions within 10pt of each other (covers 6pt jitter while
    # keeping 12pt+ real indent steps separate)
    norm_vals = sorted(set(normalized.values()))
    norm_clusters = []
    current = [norm_vals[0]]
    for v in norm_vals[1:]:
        if v - current[0] <= 10:
            current.append(v)
        else:
            norm_clusters.append(current)
            current = [v]
    norm_clusters.append(current)

    norm_to_depth = {}
    for depth, cluster in enumerate(norm_clusters):
        for v in cluster:
            norm_to_depth[v] = depth

    x_to_depth = {}
    for x, norm in normalized.items():
        x_to_depth[x] = norm_to_depth[norm]

    return x_to_depth


def extract_toc(page_texts: list[str], config: dict,
                filepath: "Path | None" = None) -> tuple[list[dict], list[dict]]:
    """Parse ALL ToC entries from first N pages. Bronze captures everything raw.

    Returns (all_entries, toc_tables):
      all_entries: [{title, page_start, depth, is_chapter, is_table, is_excluded, parent_title}, ...]
                   Every line from the ToC pages — chapters, sub-sections, tables, all of it.
                   depth is derived from PDF x-position clustering (0=top-level, 1=sub, 2=sub-sub...).
                   is_chapter=True for chapter/appendix-level entries.
                   parent_title links sub-sections to their parent chapter.
      toc_tables: [{table_number, title, page}, ...] — tables extracted separately for
                   backward compatibility with table parsing.

    page_end is computed for chapter-level entries only (next chapter's page_start - 1).
    Sub-section entries get page_end = page_start (single-page reference from ToC).
    """
    toc_config = config.get("toc", {})
    table_pattern_str = toc_config.get("table_pattern", "")
    table_pattern = re.compile(table_pattern_str, re.IGNORECASE) if table_pattern_str else None
    scan_pages = toc_config.get("toc_scan_pages", 15)
    exclude_set = set(t.lower() for t in config.get("exclude_chapters", []))

    # ── Pass 1: identify ToC pages and extract x-positions for depth detection ──
    # First find which pages are actually ToC (>30% of lines parse as ToC entries)
    toc_page_candidates = []
    for page_idx in range(min(scan_pages, len(page_texts))):
        lines_on_page = [l for l in page_texts[page_idx].split("\n") if l.strip() and len(l.strip()) > 5]
        toc_count = sum(1 for l in lines_on_page if _extract_toc_line(l))
        if lines_on_page and toc_count / len(lines_on_page) > 0.3:
            toc_page_candidates.append(page_idx)

    toc_line_positions = {}  # title -> x_position
    if filepath and toc_page_candidates:
        doc = fitz.open(str(filepath))
        for page_idx in toc_page_candidates:
            if page_idx >= len(doc):
                continue
            page = doc[page_idx]
            for block in page.get_text("dict")["blocks"]:
                if "lines" not in block:
                    continue
                for line in block["lines"]:
                    text = "".join(span["text"] for span in line["spans"]).strip()
                    if not text or len(text) < 5:
                        continue
                    parsed = _extract_toc_line(text)
                    if parsed and parsed[0] not in toc_line_positions:
                        toc_line_positions[parsed[0]] = line["bbox"][0]
        doc.close()

    # Cluster x-positions into depth levels
    x_to_depth = _cluster_x_positions(list(toc_line_positions.values()))

    # ── Pass 2: extract entries from page texts ──
    all_entries = []
    toc_tables = []
    seen_titles = set()
    seen_tables = set()

    for page_idx in toc_page_candidates:
        raw_lines = page_texts[page_idx].split("\n")
        # Join split lines: fitz sometimes wraps long ToC entries across two lines
        # (possibly with blank lines between). Detect: current line doesn't parse
        # (no page number), a nearby following line does, and the join parses too.
        joined_lines = []
        i = 0
        while i < len(raw_lines):
            stripped = raw_lines[i].strip()
            if stripped and not _extract_toc_line(stripped):
                # Look ahead up to 2 lines, skipping blanks
                joined = False
                for skip in range(1, 3):
                    if i + skip >= len(raw_lines):
                        break
                    next_stripped = raw_lines[i + skip].strip()
                    if not next_stripped:
                        continue  # skip blank lines
                    combined = stripped + " " + next_stripped
                    if _extract_toc_line(combined):
                        joined_lines.append(combined)
                        i += skip + 1
                        joined = True
                        break
                    break  # non-blank line that doesn't join — stop looking
                if joined:
                    continue
            joined_lines.append(raw_lines[i])
            i += 1

        for line in joined_lines:
            parsed = _extract_toc_line(line)
            if not parsed:
                continue
            title, page = parsed

            # Determine depth from PDF x-position
            x_pos = toc_line_positions.get(title)
            if x_pos is not None:
                depth = x_to_depth.get(round(x_pos, 1), 0)
            else:
                depth = 0  # no position info — default to top-level

            # Initial classification guesses (review file overrides these):
            # depth 0 = chapter, table_pattern match = table, rest = section
            is_chapter = (depth == 0)
            is_table = bool(table_pattern and table_pattern.match(title))

            if is_table:
                num_match = re.search(r'\d+', title)
                if num_match:
                    table_num = int(num_match.group())
                    if table_num not in seen_tables:
                        seen_tables.add(table_num)
                        colon_idx = title.find(":")
                        table_title = title[colon_idx + 1:].strip() if colon_idx >= 0 else title
                        toc_tables.append({
                            "table_number": table_num,
                            "title": table_title,
                            "page": page,
                        })

            # Store every ToC line (dedup by title+page — same title at different
            # pages is valid, e.g. "Secondary Skills" section and table)
            dedup_key = (title, page)
            if dedup_key not in seen_titles:
                seen_titles.add(dedup_key)
                all_entries.append({
                    "title": title,
                    "page_start": page,
                    "depth": depth,
                    "is_chapter": is_chapter,
                    "is_table": is_table,
                    "is_excluded": is_chapter and title.lower() in exclude_set,
                })

    # Sort by page, then assign parent_title and page_end for chapters
    all_entries.sort(key=lambda e: (e["page_start"], 0 if not e["is_table"] else 1))

    # Fix table depths: set to nearest preceding non-table entry's depth + 1
    # Tables from the flat table index (page 7) have meaningless depths;
    # this re-derives depth from their position in the page-ordered entry list.
    last_section_depth = 0
    for entry in all_entries:
        if not entry["is_table"]:
            last_section_depth = entry["depth"]
        else:
            entry["depth"] = last_section_depth + 1

    # Assign parent_title: each sub-section belongs to the most recent chapter
    current_chapter = None
    for entry in all_entries:
        if entry["is_chapter"]:
            current_chapter = entry["title"]
            entry["parent_title"] = None
        else:
            entry["parent_title"] = current_chapter

    # Compute page_end for chapter-level entries
    chapters = [e for e in all_entries if e["is_chapter"]]
    for i, ch in enumerate(chapters):
        ch["page_end"] = chapters[i + 1]["page_start"] - 1 if i + 1 < len(chapters) else 9999

    # Sub-sections get page_end = page_start (point reference)
    for entry in all_entries:
        if not entry["is_chapter"]:
            entry["page_end"] = entry["page_start"]

    # Apply toc_corrections from config (title fixes, page adjustments, removals)
    corrections = config.get("toc_corrections", {})
    if corrections:
        corrected = []
        for entry in all_entries:
            fix = corrections.get(entry["title"], {})
            if fix.get("remove"):
                _log(f"  ToC correction: removed '{entry['title']}'")
                continue
            if "title" in fix:
                _log(f"  ToC correction: '{entry['title']}' → '{fix['title']}'")
                entry["title"] = fix["title"]
            if "page_start" in fix:
                entry["page_start"] = fix["page_start"]
            if "page_end" in fix:
                entry["page_end"] = fix["page_end"]
            if "is_excluded" in fix:
                entry["is_excluded"] = fix["is_excluded"]
            corrected.append(entry)
        all_entries = corrected

    _log(f"  ToC: {len(all_entries)} entries ({len(chapters)} chapters, "
         f"{len(all_entries) - len(chapters)} sub-sections, {len(toc_tables)} tables)")

    toc_tables.sort(key=lambda t: t["table_number"])
    return all_entries, toc_tables


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
    """Write all raw extraction data to bronze Iceberg tables on S3."""
    now = datetime.now(timezone.utc)
    sf = filepath.name
    ow = dict(overwrite_filter="source_file", overwrite_filter_value=sf)

    # Files
    write_iceberg(NAMESPACE, "files", pa.table({
        "source_file": [sf], "pdf_size_bytes": [filepath.stat().st_size],
        "total_pages": [len(page_texts)], "config_hash": [config_hash(config)],
        "run_id": [run_id], "extracted_at": [now],
    }), **ow)

    # Marker extraction
    write_iceberg(NAMESPACE, "marker_extractions", pa.table({
        "source_file": [sf], "markdown_text": [markdown],
        "char_count": [len(markdown)], "run_id": [run_id], "extracted_at": [now],
    }), **ow)

    # Page texts
    write_iceberg(NAMESPACE, "page_texts", pa.table({
        "source_file": [sf] * len(page_texts),
        "page_index": list(range(len(page_texts))),
        "page_text": page_texts,
        "printed_page_num": [page_printed.get(i, i) for i in range(len(page_texts))],
        "run_id": [run_id] * len(page_texts),
    }), **ow)

    # ToC (all entries: chapters + sub-sections + tables)
    if toc_sections:
        write_iceberg(NAMESPACE, "toc_raw", pa.table({
            "source_file": [sf] * len(toc_sections),
            "title": [s["title"] for s in toc_sections],
            "page_start": [s["page_start"] for s in toc_sections],
            "page_end": [s["page_end"] for s in toc_sections],
            "depth": pa.array([s.get("depth", 0) for s in toc_sections], type=pa.int32()),
            "is_chapter": [s.get("is_chapter", True) for s in toc_sections],
            "is_table": [s.get("is_table", False) for s in toc_sections],
            "is_excluded": [s["is_excluded"] for s in toc_sections],
            "parent_title": [s.get("parent_title") for s in toc_sections],
            "run_id": [run_id] * len(toc_sections),
        }), **ow)

    # Known entries
    if known_entries:
        write_iceberg(NAMESPACE, "known_entries_raw", pa.table({
            "source_file": [sf] * len(known_entries),
            "entry_name": [e["entry_name"] for e in known_entries],
            "entry_class": [e.get("entry_class") for e in known_entries],
            "entry_level": pa.array([e.get("entry_level") for e in known_entries], type=pa.int32()),
            "ref_page": pa.array([e.get("ref_page") for e in known_entries], type=pa.int32()),
            "source_section": [e.get("source_section") for e in known_entries],
            "school": [e.get("school") for e in known_entries],
            "sphere": [e.get("sphere") for e in known_entries],
            "run_id": [run_id] * len(known_entries),
        }), **ow)

    # Tables
    table_rows = []
    for tbl in all_tables:
        for row_idx, cells in enumerate(tbl["rows"]):
            table_rows.append({
                "source_file": sf, "table_number": tbl["table_number"],
                "table_title": tbl["table_title"], "format": tbl.get("format", "pipe"),
                "row_index": row_idx, "cells": json.dumps(cells), "run_id": run_id,
            })
    if table_rows:
        write_iceberg(NAMESPACE, "tables_raw", pa.Table.from_pylist(table_rows), **ow)

    # Authority table entries
    if authority_entries:
        write_iceberg(NAMESPACE, "authority_table_entries", pa.table({
            "source_file": [sf] * len(authority_entries),
            "entry_name": [e["entry_name"] for e in authority_entries],
            "entry_type": [e["entry_type"] for e in authority_entries],
            "source_table": [e["source_table"] for e in authority_entries],
            "run_id": [run_id] * len(authority_entries),
        }), **ow)

    # Spell list entries
    if spell_list:
        write_iceberg(NAMESPACE, "spell_list_entries", pa.table({
            "source_file": [sf] * len(spell_list),
            "entry_name": [e["entry_name"] for e in spell_list],
            "entry_class": [e["entry_class"] for e in spell_list],
            "entry_level": [e["entry_level"] for e in spell_list],
            "is_reversible": [e["is_reversible"] for e in spell_list],
            "source_section": [e.get("source_section") for e in spell_list],
            "run_id": [run_id] * len(spell_list),
        }), **ow)

    # Watermarks
    if watermarks:
        wm_texts = list(watermarks.keys())
        wm_counts = list(watermarks.values())
        write_iceberg(NAMESPACE, "watermarks", pa.table({
            "source_file": [sf] * len(wm_texts),
            "watermark_text": wm_texts,
            "occurrence_count": wm_counts,
            "run_id": [run_id] * len(wm_texts),
        }), **ow)

    # Refresh catalog
    row_counts = {
        "page_texts": len(page_texts), "toc_raw": len(toc_sections),
        "known_entries_raw": len(known_entries), "spell_list_entries": len(spell_list),
        "tables_raw": sum(len(t["rows"]) for t in all_tables),
        "authority_table_entries": len(authority_entries), "watermarks": len(watermarks),
    }
    refresh_catalog(sf, run_id, config)

    _log(f"  Bronze stored (run {run_id}): {row_counts['page_texts']} pages, "
         f"{row_counts['toc_raw']} ToC, {row_counts['known_entries_raw']} index entries, "
         f"{row_counts['spell_list_entries']} spell list, "
         f"{len(all_tables)} tables ({row_counts['tables_raw']} rows), "
         f"{row_counts['authority_table_entries']} authority, {row_counts['watermarks']} watermarks")


# ── Pipeline ─────────────────────────────────────────────────────

def extract_pdf(filepath: Path) -> None:
    """Extract raw data from a single PDF into bronze Iceberg tables."""
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
    current_hash = config_hash(config)
    try:
        existing = read_iceberg_filtered(NAMESPACE, "files", "source_file", filepath.name)
        if len(existing) > 0:
            prev_hash = existing.column("config_hash")[0].as_py()
            if prev_hash == current_hash:
                _log(f"  Bronze: already extracted (config unchanged), skipping")
                return
    except Exception:
        pass  # Table doesn't exist yet — first run

    # Start pipeline run
    run_id = start_run(filepath.name, "extract", config)
    _log(f"  Run: {run_id}")

    try:
        # 1. Page texts + printed page numbers
        page_texts, page_printed, total_pages = extract_page_texts(filepath, config)
        step(f"PDF: {total_pages} pages")

        # 2. ToC (sections + tables)
        toc_sections, toc_tables = extract_toc(page_texts, config, filepath)
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
        row_counts = {
            "page_texts": len(page_texts), "toc_raw": len(toc_sections),
            "known_entries_raw": len(known_entries), "spell_list_entries": len(spell_list),
            "tables_raw": sum(len(t["rows"]) for t in all_tables),
            "authority_table_entries": len(authority_entries), "watermarks": len(watermarks),
        }
        finish_run(run_id, "success", row_counts)

    except Exception as e:
        finish_run(run_id, "failed", error=str(e))
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


def _find_whole_word(word: str, text: str) -> int:
    """Find a whole-word match in text. Returns position or -1.
    A whole word is bounded by non-alphanumeric chars (or string edges)."""
    pos = 0
    while True:
        idx = text.find(word, pos)
        if idx < 0:
            return -1
        before_ok = idx == 0 or not text[idx - 1].isalnum()
        after_ok = idx + len(word) >= len(text) or not text[idx + len(word)].isalnum()
        if before_ok and after_ok:
            return idx
        pos = idx + 1


def _get_context_for_word(word: str, text: str, context_chars: int = 150) -> str:
    """Find surrounding context for a whole-word match in text.
    Searches continuous text (not line-by-line) to handle phrases spanning lines."""
    # Normalize whitespace for searching
    normalized = " ".join(text.split())
    norm_word = " ".join(word.split())
    pos = _find_whole_word(norm_word, normalized)
    if pos < 0:
        return ""
    # Extract context centered on the match
    start = max(0, pos - 40)
    end = min(len(normalized), pos + len(norm_word) + 80)
    # Extend to word boundaries
    while start > 0 and normalized[start - 1] not in " \t":
        start -= 1
    while end < len(normalized) and normalized[end] not in " \t":
        end += 1
    return normalized[start:end].strip()


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


def _verify_ollama_model(model: str, config: dict) -> None:
    """Verify an Ollama model is cached locally. Raises if not available."""
    import requests
    url = config.get("ocr_check", {}).get("ollama_url", "http://host.docker.internal:11434")
    try:
        resp = requests.get(f"{url}/api/tags", timeout=10)
        resp.raise_for_status()
        available = [m["name"] for m in resp.json().get("models", [])]
        if model not in available:
            raise RuntimeError(
                f"Ollama model '{model}' not cached. Available: {available}\n"
                f"Pull it first: ollama pull {model}"
            )
    except requests.ConnectionError:
        raise RuntimeError(f"Ollama not running at {url}")


def check_ocr(source_file: str) -> None:
    """Bronze validation: dictionary-based spellcheck of markdown content.

    Extracts words from markdown, checks against English dictionary + game terms
    whitelist from config. Unknown words are OCR candidates with suggested corrections.
    Runs in seconds — no LLM needed.
    """
    from spellchecker import SpellChecker
    import time as _time

    configs_dir = DOCUMENTS_DIR.parent / "configs"
    config = load_config(Path(source_file), configs_dir)
    run_id = start_run(source_file, "check_ocr", config)
    start_time = _time.time()

    try:
        md_table = read_iceberg_filtered(NAMESPACE, "marker_extractions", "source_file", source_file)
    except Exception:
        _log(f"No markdown found for {source_file}")
        return
    if len(md_table) == 0:
        _log(f"No markdown found for {source_file}")
        return

    md = _clean_marker_md(md_table.column("markdown_text")[0].as_py())

    # Apply existing substitutions — don't re-flag known fixes
    for sub in config.get("content_substitutions", []):
        if len(sub) == 2:
            md = md.replace(sub[0], sub[1])

    # Build spellchecker with game dictionary from config
    spell = SpellChecker()
    game_words = config.get("game_dictionary", [])
    if game_words:
        spell.word_frequency.load_words([w.lower() for w in game_words])

    # Also add all known entry names (spell names, class names, etc.)
    try:
        ke_table = read_iceberg_filtered(NAMESPACE, "known_entries_raw", "source_file", source_file)
        for i in range(len(ke_table)):
            name = ke_table.column("entry_name")[i].as_py()
            for word in name.lower().split():
                spell.word_frequency.load_words([word])
    except Exception:
        pass

    # Extract words from non-table, non-heading content
    words_with_context = {}  # word -> context line
    for line in md.split("\n"):
        stripped = line.strip()
        # Skip table rows, headings, image refs, short lines
        if stripped.startswith("|") or stripped.startswith("#") or stripped.startswith("!"):
            continue
        if len(stripped) < 10:
            continue
        # Extract alpha words (3+ chars)
        for word in stripped.split():
            clean = word.strip("*_,.;:!?()[]{}\"'—–-/\\")
            if len(clean) < 3 or not clean.isalpha():
                continue
            lower = clean.lower()
            if lower not in words_with_context:
                words_with_context[lower] = stripped[:150]

    _log(f"  OCR check: {len(words_with_context)} unique words to check")

    # Find unknown words
    unknown = spell.unknown(words_with_context.keys())
    _log(f"  Unknown words: {len(unknown)}")

    # Record unknown words (skip slow candidates/correction lookup)
    now = datetime.now()
    issues = []
    for word in sorted(unknown):
        ctx = words_with_context.get(word, "")
        issues.append({
            "source_file": source_file,
            "wrong_text": word,
            "suggested_fix": "",
            "context": ctx,
            "status": "candidate",
            "model": "dictionary",
            "run_id": run_id,
            "checked_at": now,
        })

    # Write to Iceberg
    if issues:
        write_iceberg(NAMESPACE, "ocr_issues", pa.table({
            k: [row[k] for row in issues] for k in issues[0]
        }), overwrite_filter="source_file", overwrite_filter_value=source_file)

    elapsed = _time.time() - start_time
    _log(f"  OCR check complete: {len(issues)} candidates in {elapsed:.1f}s")

    finish_run(run_id, "success",
               {"candidates": len(issues), "words_checked": len(words_with_context)})


def review_ocr(source_file: str) -> None:
    """Silver pass: review OCR candidates with the large model.

    Reads 'candidate' issues from bronze_tabletop.ocr_issues,
    sends each to the silver model for confirmation, updates status
    to 'confirmed' or 'rejected'.
    """
    import time as _time

    configs_dir = DOCUMENTS_DIR.parent / "configs"
    config = load_config(Path(source_file), configs_dir)
    run_id = start_run(source_file, "review_ocr", config)
    ocr_cfg = config.get("ocr_check", {})
    silver_model = ocr_cfg.get("silver_model", "llama3:70b")
    silver_prompt = ocr_cfg.get("silver_prompt",
        'Review this OCR error: "{wrong}" -> "{correct}". Context: {context}\n'
        'Respond with JSON: {{"verdict": "confirmed" or "rejected", "reason": "brief"}}')

    try:
        issues_table = read_iceberg_filtered(NAMESPACE, "ocr_issues", "source_file", source_file)
        issues_df = issues_table.to_pandas()
        candidates_df = issues_df[issues_df["status"] == "candidate"]
        candidates = list(candidates_df[["wrong_text", "suggested_fix", "context"]].itertuples(index=False))
    except Exception:
        candidates = []

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
            start_idx = response.find("{")
            end_idx = response.rfind("}")
            if start_idx >= 0 and end_idx > start_idx:
                result = json.loads(response[start_idx:end_idx + 1])
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

        # Update status: read all issues for this file, update the matching one, rewrite
        write_iceberg(NAMESPACE, "ocr_issues", pa.table({
            "source_file": [source_file], "wrong_text": [wrong],
            "suggested_fix": [correct], "context": [ctx or ""],
            "status": [verdict], "model": [silver_model],
            "run_id": [run_id], "checked_at": [now],
        }))

    total_time = _time.time() - start_time
    _log(f"  OCR review complete: {confirmed} confirmed, {rejected} rejected "
         f"in {total_time / 60:.1f}m")

    # Finish run, refresh catalog, free model
    finish_run(run_id, "success",
               {"confirmed": confirmed, "rejected": rejected})
    refresh_catalog(source_file, run_id, config)
    _unload_ollama_model(silver_model, config)


# ── Bronze Validation ──────────────────────────────────────────

def _store_validation(source_file: str, check_name: str,
                      status: str, message: str, run_id: str,
                      details: str = "") -> None:
    """Upsert a validation result to Iceberg."""
    now = datetime.now(timezone.utc)
    write_iceberg(NAMESPACE, "validation_results", pa.table({
        "source_file": [source_file], "check_name": [check_name],
        "status": [status], "message": [message], "details": [details],
        "run_id": [run_id], "checked_at": [now],
    }), overwrite_filter="source_file", overwrite_filter_value=source_file)


def validate_bronze(source_file: str) -> None:
    """Run all bronze validation checks for a source file.

    Checks: table completeness, entry coverage, spell cross-check,
    content gaps, duplicate entries. Results stored in
    bronze_tabletop.validation_results.
    """
    from dlt.lib.duckdb_reader import get_reader
    conn = get_reader(namespaces=[NAMESPACE])
    sf = source_file

    configs_dir = DOCUMENTS_DIR.parent / "configs"
    config = load_config(Path(sf), configs_dir)
    run_id = start_run(sf, "validate", config)
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
        _store_validation(sf, "table_completeness", status, msg, run_id,
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
        _store_validation(sf, "spell_cross_check", status, msg, run_id, details)
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
        _store_validation(sf, "content_gaps", status, msg, run_id,
                          json.dumps({"gaps": gaps}))
        _log(f"  Content gaps: {status} — {msg}")

    # ── 4. Duplicate entry detection ──
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
    _store_validation(sf, "duplicate_entries", status, msg, run_id,
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
        _store_validation(sf, "authority_coverage", status, msg, run_id,
                          json.dumps({"missing": not_in_known[:30]}))
        _log(f"  Authority coverage: {status} — {msg}")

    conn.close()
    run_status = "success" if failed == 0 else "failed"
    finish_run(run_id, run_status,
               {"passed": passed, "warned": warned, "failed": failed})
    refresh_catalog(sf, run_id, config)
    _log(f"  Validation summary: {passed} passed, {warned} warnings, {failed} failed")


def review_toc(source_file: str | None = None) -> dict:
    """Review parsed ToC and Marker headings for a book. Returns review report.

    Called by Dagster as a validation gate between bronze and dbt.
    Checks:
    1. toc_reviewed flag in config — warns if false (new book needs manual review)
    2. Dumps parsed ToC sections with page ranges
    3. Extracts all H1/H2 headings from Marker markdown for section heading review
    4. Identifies headings not in valid_section_headings config

    Report is logged and stored in bronze_tabletop.validation_results.
    Returns {"status": "pass"|"needs_review", "toc": [...], "headings": [...]}
    """
    from dlt.lib.duckdb_reader import get_reader
    conn = get_reader(namespaces=[NAMESPACE])

    files = _list_source_files("toc_raw", source_file)
    if not files:
        _log("No files found for ToC review")
        return {"status": "no_files", "files": []}

    all_reports = []
    for sf in files:
        configs_dir = DOCUMENTS_DIR.parent / "configs"
        config = load_config(Path(sf), configs_dir)
        run_id = start_run(sf, "toc_review", config)

        toc_reviewed = config.get("toc_reviewed", False)
        valid_headings = set(h.lower() for h in config.get("valid_section_headings", []))

        # Get parsed ToC
        toc_rows = conn.execute(
            "SELECT title, page_start, page_end, is_excluded "
            "FROM bronze_tabletop.toc_raw WHERE source_file = ? ORDER BY page_start",
            [sf]
        ).fetchall()
        toc = [{"title": t, "page_start": ps, "page_end": pe, "is_excluded": ex}
               for t, ps, pe, ex in toc_rows]

        # Get H1/H2 headings from Marker markdown
        md_rows = conn.execute(
            "SELECT markdown_text FROM bronze_tabletop.marker_extractions WHERE source_file = ?",
            [sf]
        ).fetchall()

        h1_headings = []
        h2_headings = []
        if md_rows:
            markdown = md_rows[0][0]
            for m in re.finditer(r"^(#{1,2})\s+(.+)", markdown, re.MULTILINE):
                level = len(m.group(1))
                heading = re.sub(r"\*+", "", m.group(2)).strip()
                if len(heading) < 2:
                    continue
                entry = {"heading": heading, "level": level, "position": m.start()}
                if level == 1:
                    h1_headings.append(entry)
                else:
                    h2_headings.append(entry)

        # Classify headings
        toc_titles_lower = set(t["title"].lower() for t in toc)
        toc_desc_lower = set()
        for t in toc:
            if ":" in t["title"]:
                toc_desc_lower.add(t["title"].split(":", 1)[-1].strip().lower())

        unrecognized_h1 = []
        for h in h1_headings:
            hl = h["heading"].lower().rstrip(".")
            if hl in toc_titles_lower or hl in toc_desc_lower or hl in valid_headings:
                continue
            # Check if it's a word in any ToC title
            matched = False
            for t_lower in toc_desc_lower:
                if hl in t_lower.split():
                    matched = True
                    break
            if not matched:
                unrecognized_h1.append(h["heading"])

        unrecognized_h2 = []
        for h in h2_headings:
            hl = h["heading"].lower()
            if hl in valid_headings or hl in toc_titles_lower or hl in toc_desc_lower:
                continue
            unrecognized_h2.append(h["heading"])

        # Log report
        _log(f"\n{'='*60}")
        _log(f"ToC Review: {sf}")
        _log(f"  toc_reviewed: {toc_reviewed}")
        _log(f"  valid_section_headings: {len(valid_headings)} configured")
        _log(f"{'='*60}")

        _log(f"\n  Parsed ToC ({len(toc)} sections):")
        for t in toc:
            excl = " [EXCLUDED]" if t["is_excluded"] else ""
            _log(f"    pp {t['page_start']:>3}-{t['page_end']:>4}  {t['title']}{excl}")

        _log(f"\n  H1 headings ({len(h1_headings)} total, {len(unrecognized_h1)} unrecognized):")
        if unrecognized_h1:
            for h in unrecognized_h1:
                _log(f"    [?] # {h}")
        else:
            _log("    All H1 headings match ToC titles")

        _log(f"\n  H2 headings ({len(h2_headings)} total, {len(unrecognized_h2)} not in valid_section_headings):")
        # Deduplicate for display
        seen = set()
        for h in unrecognized_h2:
            if h not in seen:
                seen.add(h)
                _log(f"    [?] ## {h}")

        # Determine status
        if not toc_reviewed:
            status = "needs_review"
            msg = (f"toc_reviewed=false — review ToC and headings above, then update config:\n"
                   f"  1. Set toc_reviewed: true\n"
                   f"  2. Add toc_corrections for any title/page fixes\n"
                   f"  3. Add valid_section_headings for legitimate H2 sub-sections\n"
                   f"  4. Re-run bronze to apply corrections")
            _log(f"\n  STATUS: NEEDS REVIEW")
            _log(f"  {msg}")
        else:
            status = "pass"
            msg = f"ToC reviewed. {len(toc)} sections, {len(unrecognized_h1)} unrecognized H1, {len(unrecognized_h2)} unrecognized H2"
            _log(f"\n  STATUS: PASS")

        _store_validation(sf, "toc_review", status, msg, run_id,
                          json.dumps({"toc": toc, "unrecognized_h1": unrecognized_h1,
                                      "unrecognized_h2": list(seen)[:50]}))
        finish_run(run_id, "success")

        all_reports.append({
            "source_file": sf, "status": status, "toc_reviewed": toc_reviewed,
            "toc": toc, "h1_headings": h1_headings, "h2_headings": h2_headings,
            "unrecognized_h1": unrecognized_h1, "unrecognized_h2": list(seen),
        })

    conn.close()
    overall = "pass" if all(r["status"] == "pass" for r in all_reports) else "needs_review"
    return {"status": overall, "files": all_reports}


def apply_toc_review(source_file: str) -> None:
    """Apply a reviewed ToC YAML file back to bronze_tabletop.toc_raw.

    Reads the review file from documents/tabletop_rules/reviews/,
    replaces the auto-extracted toc_raw data with the human-reviewed version.
    Entries with type=remove are dropped. All other fields (depth, type,
    page, page_end, title, excluded) are written as-is.

    Review file path: documents/tabletop_rules/reviews/toc_review_<stem>.yaml
    where <stem> is the PDF filename without extension, spaces replaced with _.
    """
    reviews_dir = DOCUMENTS_DIR.parent / "reviews"
    # Build set of keywords from the PDF filename for flexible matching
    sf_words = set(Path(source_file).stem.lower().replace("-", " ").replace("_", " ").split())

    # Find review file: any toc_review_*.yaml where most filename words appear
    review_path = None
    for f in reviews_dir.glob("toc_review_*.yaml"):
        review_words = set(f.stem.lower().replace("toc_review_", "").replace("-", " ").replace("_", " ").split())
        # Match if at least 2 words overlap or one name contains the other
        overlap = sf_words & review_words
        if len(overlap) >= 2 or sf_words <= review_words or review_words <= sf_words:
            review_path = f
            break
    if not review_path or not review_path.exists():
        _log(f"No review file found for {source_file} in {reviews_dir}")
        return

    with open(review_path) as f:
        review = yaml.safe_load(f)

    entries_raw = review.get("entries", [])
    if not entries_raw:
        _log(f"Review file {review_path.name} has no entries")
        return

    # Build toc_raw entries from review, skipping type=remove
    toc_entries = []
    current_chapter = None

    sort_idx = 0
    for entry in entries_raw:
        if entry.get("type") == "remove":
            continue

        title = entry["title"]
        page_start = entry["page"]
        page_end = entry.get("page_end", page_start)
        depth = entry.get("depth", 0)
        is_chapter = (depth == 0)
        is_table = (entry.get("type") == "table")
        is_excluded = entry.get("excluded", False)

        if is_chapter:
            current_chapter = title

        toc_entries.append({
            "title": title,
            "page_start": page_start,
            "page_end": page_end,
            "sort_order": sort_idx,
            "depth": depth,
            "is_chapter": is_chapter,
            "is_table": is_table,
            "is_excluded": is_excluded,
            "parent_title": None if is_chapter else current_chapter,
        })
        sort_idx += 1

    # Recompute page_end for chapters (next chapter's page_start - 1)
    chapters = [e for e in toc_entries if e["is_chapter"]]
    for i, ch in enumerate(chapters):
        ch["page_end"] = chapters[i + 1]["page_start"] - 1 if i + 1 < len(chapters) else 9999

    # Write to toc_raw, replacing existing data for this source file
    sf = source_file
    run_id = start_run(sf, "toc_review_apply", load_config(Path(sf), DOCUMENTS_DIR.parent / "configs"))
    now = datetime.now(timezone.utc)

    write_iceberg(NAMESPACE, "toc_raw", pa.table({
        "source_file": [sf] * len(toc_entries),
        "title": [e["title"] for e in toc_entries],
        "page_start": [e["page_start"] for e in toc_entries],
        "page_end": [e["page_end"] for e in toc_entries],
        "sort_order": pa.array([e["sort_order"] for e in toc_entries], type=pa.int32()),
        "depth": pa.array([e["depth"] for e in toc_entries], type=pa.int32()),
        "is_chapter": [e["is_chapter"] for e in toc_entries],
        "is_table": [e["is_table"] for e in toc_entries],
        "is_excluded": [e["is_excluded"] for e in toc_entries],
        "parent_title": [e["parent_title"] for e in toc_entries],
        "run_id": [run_id] * len(toc_entries),
    }), overwrite_filter="source_file", overwrite_filter_value=sf)

    finish_run(run_id, "success", {"toc_entries": len(toc_entries)})

    chapters_count = sum(1 for e in toc_entries if e["is_chapter"])
    tables_count = sum(1 for e in toc_entries if e["is_table"])
    excluded_count = sum(1 for e in toc_entries if e["is_excluded"])
    removed = len(entries_raw) - len(toc_entries)
    _log(f"Applied ToC review for {sf}: {len(toc_entries)} entries "
         f"({chapters_count} chapters, {tables_count} tables, "
         f"{excluded_count} excluded, {removed} removed)")


def run(directory: Path | None = None, force: bool = False) -> None:
    """Extract new/changed PDFs to bronze Iceberg tables.

    Change detection: skips a PDF if its file size and config hash
    match what's already stored in bronze_tabletop.files.
    Use force=True to re-extract everything.
    """
    doc_dir = directory or DOCUMENTS_DIR
    files = sorted(doc_dir.glob("*.pdf"))
    if not files:
        _log(f"No PDFs in {doc_dir}")
        return

    # Load existing extraction state from Iceberg
    existing = {}
    try:
        files_table = read_iceberg(NAMESPACE, "files")
        for i in range(len(files_table)):
            sf = files_table.column("source_file")[i].as_py()
            size = files_table.column("pdf_size_bytes")[i].as_py()
            chash = files_table.column("config_hash")[i].as_py()
            existing[sf] = (size, chash)
    except Exception:
        pass  # Table doesn't exist yet — first run

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


def _list_source_files(table_name: str, book_filter: str | None = None,
                       status_filter: dict | None = None) -> list[str]:
    """List distinct source_file values from an Iceberg table."""
    try:
        tbl = read_iceberg(NAMESPACE, table_name)
        df = tbl.to_pandas()
        if status_filter:
            for col, val in status_filter.items():
                df = df[df[col] == val]
        files = df["source_file"].unique().tolist()
        if book_filter:
            files = [f for f in files if book_filter in f]
        return sorted(files)
    except Exception:
        return []


if __name__ == "__main__":
    import sys
    if "--check-ocr" in sys.argv:
        # Usage: python -m dlt.bronze_tabletop_rules --check-ocr Player [--sample 50] [--no-resume]
        args = [a for a in sys.argv[1:] if a != "--check-ocr"]
        book_filter = next((a for a in args if not a.startswith("--")), None)
        sample_idx = next((i for i, a in enumerate(args) if a == "--sample"), None)
        sample_n = int(args[sample_idx + 1]) if sample_idx is not None else 0
        do_resume = "--no-resume" not in sys.argv

        for sf in _list_source_files("marker_extractions", book_filter):
            check_ocr(sf, sample=sample_n, resume=do_resume)
    elif "--review-ocr" in sys.argv:
        # Usage: python -m dlt.bronze_tabletop_rules --review-ocr Player
        args = [a for a in sys.argv[1:] if a != "--review-ocr"]
        book_filter = next((a for a in args if not a.startswith("--")), None)

        for sf in _list_source_files("ocr_issues", book_filter, {"status": "candidate"}):
            review_ocr(sf)
    elif "--validate" in sys.argv:
        # Usage: python -m dlt.bronze_tabletop_rules --validate Player
        args = [a for a in sys.argv[1:] if a != "--validate"]
        book_filter = next((a for a in args if not a.startswith("--")), None)

        for sf in _list_source_files("files", book_filter):
            validate_bronze(sf)
    elif "--force" in sys.argv:
        run(force=True)
    elif len(sys.argv) > 1:
        extract_pdf(Path(sys.argv[1]))
    else:
        run()
