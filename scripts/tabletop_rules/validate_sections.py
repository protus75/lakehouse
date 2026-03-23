"""Validate section-level ingestion quality.

Checks that each ToC section has reasonable content for its page range.
Catches misassigned chunks, missing sections, and content bleed.
Works for any book — no content-type-specific logic.

Run: docker exec lakehouse-workspace python scripts/tabletop_rules/validate_sections.py
"""

import duckdb
import yaml
from pathlib import Path

DB_PATH = "/workspace/db/lakehouse.duckdb"
CONFIGS_DIR = Path("/workspace/documents/tabletop_rules/configs")


def load_config(source_file: str) -> dict:
    stem = Path(source_file).stem
    default_path = CONFIGS_DIR / "_default.yaml"
    book_path = CONFIGS_DIR / f"{stem}.yaml"
    config = {}
    if default_path.exists():
        with open(default_path) as f:
            config = yaml.safe_load(f) or {}
    if book_path.exists():
        with open(book_path) as f:
            book = yaml.safe_load(f) or {}
        for k, v in book.items():
            if isinstance(v, dict) and isinstance(config.get(k), dict):
                config[k].update(v)
            else:
                config[k] = v
    return config


conn = duckdb.connect(DB_PATH, read_only=True)

source_files = conn.execute(
    "SELECT DISTINCT source_file FROM documents_tabletop_rules.files"
).fetchall()

for (sf,) in source_files:
    print(f"\n{'='*60}")
    print(f"  {sf}")
    print(f"{'='*60}")

    config = load_config(sf)

    # Get ToC sections with chunk stats
    sections = conn.execute("""
        SELECT t.toc_id, t.title, t.page_start, t.page_end, t.is_excluded,
               count(c.chunk_id) as chunk_count,
               count(distinct c.entry_title) as named_entries,
               sum(case when c.entry_title is null then 1 else 0 end) as unnamed_chunks,
               coalesce(sum(c.char_count), 0) as total_chars
        FROM documents_tabletop_rules.toc t
        LEFT JOIN documents_tabletop_rules.chunks c ON t.toc_id = c.toc_id
        WHERE t.source_file = ?
        GROUP BY t.toc_id, t.title, t.page_start, t.page_end, t.is_excluded
        ORDER BY t.page_start
    """, [sf]).fetchall()

    issues = []
    section_count = 0
    total_chunks = 0

    for s in sections:
        toc_id, title, p_start, p_end, excluded, chunks, named, unnamed, chars = s
        if excluded:
            continue

        section_count += 1
        total_chunks += chunks
        page_span = min(p_end, 999) - p_start + 1  # cap 9999 sentinel
        expected_min_chunks = max(1, page_span)  # at least 1 chunk per page

        label = f"{title} (p.{p_start}-{p_end})"

        # No chunks at all
        if chunks == 0:
            issues.append(("EMPTY_SECTION", label, "No chunks — section not parsed"))
            continue

        # Very few chunks for page span
        if chunks < page_span * 0.3 and page_span > 3:
            issues.append(("LOW_CHUNKS", label,
                           f"{chunks} chunks for {page_span} pages "
                           f"(expected ~{expected_min_chunks}+)"))

        # Way too many chunks for page span (content bleed from other sections)
        if chunks > page_span * 20 and page_span > 2:
            issues.append(("CHUNK_OVERFLOW", label,
                           f"{chunks} chunks for {page_span} pages — "
                           f"likely absorbing content from later sections"))

        # High ratio of unnamed chunks (no entry_title)
        if chunks > 5 and unnamed > chunks * 0.8:
            issues.append(("MOSTLY_UNNAMED", label,
                           f"{unnamed}/{chunks} chunks have no entry_title — "
                           f"heading detection not working"))

        # Very low char count per chunk
        avg_chars = chars / chunks if chunks else 0
        if chunks > 3 and avg_chars < 30:
            issues.append(("TINY_CHUNKS", label,
                           f"avg {avg_chars:.0f} chars/chunk — "
                           f"possible parsing artifacts"))

        # Print section summary
        status = "OK" if not any(i[1] == label for i in issues) else "!!"
        print(f"  [{status}] {label}: {chunks} chunks, "
              f"{named} named, {unnamed} unnamed, "
              f"{chars:,} chars", flush=True)

    # Report issues
    print(f"\n  Sections: {section_count} | Chunks: {total_chunks} | "
          f"Issues: {len(issues)}", flush=True)

    if issues:
        print()
        for category, label, detail in issues:
            print(f"  {category}: {label}")
            print(f"    {detail}")
    else:
        print("\n  All sections passed validation!")

conn.close()
