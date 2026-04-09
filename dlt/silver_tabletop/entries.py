"""silver_tabletop.silver_entries — cleaned, chapter-assigned entries from
pymupdf page texts.

Migrated from dbt python model. Now a plain function called by a Dagster
Python asset. Reads bronze via get_reader(), writes silver via
write_iceberg() per source_file. Streams over files so multi-book runs
never hold more than one file's worth of intermediate state.

Logic copied verbatim from the dbt model — the only changes are:
  - reads use get_reader() instead of dbt session
  - per-file write to iceberg via write_iceberg(overwrite_filter=...)
  - returns total row count instead of a DataFrame
"""
import sys
from pathlib import Path

sys.path.insert(0, "/workspace")

import pyarrow as pa

from dlt.lib.duckdb_reader import get_reader
from dlt.lib.iceberg_catalog import write_iceberg, list_tables
from dlt.lib.tabletop_cleanup import (
    load_config,
    build_entries_from_pages,
    collect_sub_headings,
    _detect_watermarks,
    strip_leading_title,
)
from dlt.lib.stable_keys import make_id


CONFIGS_DIR = Path("/workspace/documents/tabletop_rules/configs")
PDF_DIR = Path("/workspace/documents/tabletop_rules/raw")
NAMESPACE = "silver_tabletop"
TABLE = "silver_entries"


def _build_one_file(reader, sf: str) -> list[dict]:
    """Build silver_entries rows for a single source_file. Returns dict rows."""
    config = load_config(Path(sf), CONFIGS_DIR)

    # Page texts (primary content)
    pages_df = reader.execute(
        "SELECT page_index, page_text, printed_page_num "
        "FROM bronze_tabletop.page_texts WHERE source_file = ? ORDER BY page_index",
        [sf],
    ).fetchdf()
    if pages_df.empty:
        return []
    page_texts = dict(zip(
        pages_df["printed_page_num"].astype(int).tolist(),
        pages_df["page_text"].tolist(),
    ))
    total_pages = len(pages_df)

    # ToC (sort_order preserves book order within same page)
    toc_df = reader.execute(
        "SELECT title, page_start, page_end, is_excluded, is_chapter, is_table, "
        "parent_title, sort_order "
        "FROM bronze_tabletop.toc_raw WHERE source_file = ? ORDER BY sort_order",
        [sf],
    ).fetchdf()
    toc_all = []
    for _, row in toc_df.iterrows():
        toc_all.append({
            "title": row["title"],
            "page_start": int(row["page_start"]),
            "page_end": int(row["page_end"]) if row["page_end"] else 9999,
            "is_excluded": bool(row["is_excluded"]),
            "is_chapter": bool(row["is_chapter"]),
            "is_table": bool(row.get("is_table", False)),
            "parent_title": row.get("parent_title"),
            "sort_order": int(row["sort_order"]) if row.get("sort_order") is not None else 0,
            "sub_headings": [],
            "tables": [],
        })

    # Watermarks
    watermarks = _detect_watermarks(pages_df["page_text"].tolist(), total_pages)

    # Spell list
    sl_df = reader.execute(
        "SELECT entry_name, entry_class, entry_level "
        "FROM bronze_tabletop.spell_list_entries WHERE source_file = ?",
        [sf],
    ).fetchdf()
    spell_list = sl_df.to_dict("records") if not sl_df.empty else []

    # Authority entries
    ae_df = reader.execute(
        "SELECT entry_name, entry_type, source_table "
        "FROM bronze_tabletop.authority_table_entries WHERE source_file = ?",
        [sf],
    ).fetchdf()
    authority_entries = ae_df.to_dict("records") if not ae_df.empty else []

    # Cross-referenced spell metadata
    meta_df = reader.execute(
        "SELECT entry_name, school, sphere, ref_page "
        "FROM bronze_tabletop.known_entries_raw "
        "WHERE source_file = ? AND entry_class IS NOT NULL "
        "AND (school IS NOT NULL OR ref_page IS NOT NULL)",
        [sf],
    ).fetchdf()
    if not meta_df.empty:
        spell_meta: dict = {}
        for _, r in meta_df.iterrows():
            name = r["entry_name"].lower()
            ref_page = int(r["ref_page"]) if r.get("ref_page") else None
            if name not in spell_meta:
                spell_meta[name] = {
                    "school": r["school"],
                    "sphere": r.get("sphere"),
                    "ref_page": ref_page,
                }
            elif ref_page and not spell_meta[name].get("ref_page"):
                spell_meta[name]["ref_page"] = ref_page
        for s in spell_list:
            name = (s.get("entry_name") or "").lower()
            if name in spell_meta:
                s["school"] = spell_meta[name].get("school")
                s["sphere"] = spell_meta[name].get("sphere")
                s["ref_page"] = spell_meta[name].get("ref_page")

    # tables_raw for table entries
    tr_df = reader.execute(
        "SELECT toc_title, row_index, cells "
        "FROM bronze_tabletop.tables_raw WHERE source_file = ? "
        "ORDER BY toc_title, row_index",
        [sf],
    ).fetchdf()
    tables_raw = tr_df.to_dict("records") if not tr_df.empty else []

    # printed→0-based page index map (for VLM page rendering, when used)
    page_index_map = dict(zip(
        pages_df["printed_page_num"].astype(int).tolist(),
        pages_df["page_index"].astype(int).tolist(),
    ))
    pdf_path = PDF_DIR / sf

    # Font-switch table masks (Phase 4)
    page_text_masks: dict[int, list[tuple[int, int]]] = {}
    if "page_text_masks" in list_tables("bronze_tabletop"):
        mask_df = reader.execute(
            "SELECT printed_page_num, char_start, char_end "
            "FROM bronze_tabletop.page_text_masks WHERE source_file = ? "
            "ORDER BY printed_page_num, char_start",
            [sf],
        ).fetchdf()
        for _, r in mask_df.iterrows():
            pp = int(r["printed_page_num"])
            page_text_masks.setdefault(pp, []).append(
                (int(r["char_start"]), int(r["char_end"]))
            )
        print(
            f"  page_text_masks loaded for {sf}: {len(page_text_masks)} pages, "
            f"{sum(len(v) for v in page_text_masks.values())} ranges",
            flush=True,
        )

    # Build entries
    entries = build_entries_from_pages(
        toc_all, page_texts, spell_list, authority_entries, config, watermarks,
        tables_raw, pdf_path, page_index_map, page_text_masks,
    )
    collect_sub_headings(entries, toc_all, config)

    # Convert to row dicts
    field_names = config.get("metadata_field_names", [])
    min_desc = config.get("validation", {}).get("min_description_chars", 20)
    out_rows: list[dict] = []
    for entry in entries:
        toc_entry = entry["toc_entry"]
        content = entry["content"]
        entry_title = entry.get("entry_title")
        if entry_title:
            content = strip_leading_title(content, entry_title)

        has_metadata = any(
            f.lower() + ":" in content.lower() for f in field_names
        ) if field_names else False

        has_description = True
        if has_metadata:
            last_meta_pos = -1
            for f in field_names:
                idx = content.lower().rfind(f.lower() + ":")
                if idx > last_meta_pos:
                    last_meta_pos = idx
            if last_meta_pos >= 0:
                after = content[last_meta_pos:].split("\n", 1)
                has_description = len(after) >= 2 and len(after[1].strip()) >= min_desc

        row_data = {
            "source_file": sf,
            "toc_title": toc_entry["title"],
            "section_title": entry.get("section_title"),
            "entry_title": entry_title,
        }
        id_data = {**row_data, "content_prefix": content[:80]}
        out_rows.append({
            "entry_id": make_id("entry_id", id_data),
            "toc_id": make_id("toc_id", {
                "source_file": sf,
                "title": toc_entry["title"],
                "parent_title": toc_entry.get("parent_title") or "",
                "page_start": str(toc_entry.get("page_start", "")),
            }),
            **row_data,
            "content": content,
            "school": entry.get("school"),
            "sphere": entry.get("sphere"),
            "spell_class": entry.get("spell_class"),
            "spell_level": entry.get("spell_level"),
            "page_numbers": ",".join(str(p) for p in entry.get("page_numbers", [])),
            "char_count": len(content),
            "has_metadata": has_metadata,
            "has_description": has_description,
        })
    return out_rows


def build_silver_entries() -> int:
    """Build silver_tabletop.silver_entries from bronze. Streams per source_file.

    For the first file, uses overwrite_all=True to replace the table; for
    subsequent files, uses overwrite_filter='source_file' so each file owns
    its own rows without trampling other files'.

    Returns the total row count written.
    """
    reader = get_reader(["bronze_tabletop"])
    files_df = reader.execute(
        "SELECT source_file FROM bronze_tabletop.files ORDER BY source_file"
    ).fetchdf()
    if files_df.empty:
        print("silver_entries: no files in bronze, nothing to write", flush=True)
        return 0

    total_rows = 0
    first = True
    for sf in files_df["source_file"].tolist():
        rows = _build_one_file(reader, sf)
        if not rows:
            print(f"  {sf}: 0 entries", flush=True)
            continue
        arrow = pa.Table.from_pylist(rows)
        if first:
            write_iceberg(NAMESPACE, TABLE, arrow, overwrite_all=True)
            first = False
        else:
            write_iceberg(
                NAMESPACE, TABLE, arrow,
                overwrite_filter="source_file",
                overwrite_filter_value=sf,
            )
        total_rows += len(rows)
        print(f"  {sf}: {len(rows)} entries written", flush=True)

    return total_rows
