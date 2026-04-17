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

import pandas as pd
import pyarrow as pa

from dlt.lib.iceberg_catalog import write_iceberg, list_tables, read_iceberg_filtered, table_exists, read_iceberg
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

# Explicit arrow schema. Required because PyIceberg v2 cannot store columns
# whose pyarrow type is null() — and a column where every row is None gets
# inferred as null() unless we specify the type up front. The dbt python
# model never hit this because dbt's adapter generated the duckdb table
# schema from the model SQL/python signature, not from the data values.
SCHEMA = pa.schema([
    ("entry_id", pa.int64()),
    ("toc_id", pa.int64()),
    ("source_file", pa.string()),
    ("toc_title", pa.string()),
    ("section_title", pa.string()),
    ("entry_title", pa.string()),
    ("content", pa.string()),
    ("school", pa.string()),
    ("sphere", pa.string()),
    ("spell_class", pa.string()),
    ("spell_level", pa.int32()),
    ("page_numbers", pa.string()),
    ("char_count", pa.int64()),
    ("has_metadata", pa.bool_()),
    ("has_description", pa.bool_()),
])


def _arrow_to_df(arrow_table):
    """Convert Arrow table to pandas, replacing PyArrow nulls with Python None."""
    return arrow_table.to_pandas()


def _build_one_file(sf: str) -> list[dict]:
    """Build silver_entries rows for a single source_file. Returns dict rows.

    Reads all bronze data via PyIceberg (no DuckDB). The DuckDB iceberg
    extension causes heap corruption that manifests as SIGSEGV in later
    pure-Python code.
    """
    config = load_config(Path(sf), CONFIGS_DIR)

    pages_at = read_iceberg_filtered("bronze_tabletop", "page_texts", "source_file", sf)
    if len(pages_at) == 0:
        return []
    pages_df = _arrow_to_df(pages_at).sort_values("page_index")
    page_texts = dict(zip(
        pages_df["printed_page_num"].astype(int).tolist(),
        pages_df["page_text"].tolist(),
    ))
    total_pages = len(pages_df)

    toc_at = read_iceberg_filtered("bronze_tabletop", "toc_raw", "source_file", sf)
    toc_df = _arrow_to_df(toc_at).sort_values("sort_order")
    toc_all = []
    for _, row in toc_df.iterrows():
        toc_all.append({
            "title": row["title"],
            "page_start": int(row["page_start"]),
            "page_end": int(row["page_end"]) if pd.notna(row["page_end"]) else 9999,
            "is_excluded": bool(row["is_excluded"]),
            "is_chapter": bool(row["is_chapter"]),
            "is_table": bool(row.get("is_table", False)),
            "parent_title": row.get("parent_title") if pd.notna(row.get("parent_title")) else None,
            "sort_order": int(row["sort_order"]) if pd.notna(row.get("sort_order")) else 0,
            "sub_headings": [],
            "tables": [],
        })

    watermarks = _detect_watermarks(pages_df["page_text"].tolist(), total_pages)

    sl_at = read_iceberg_filtered("bronze_tabletop", "spell_list_entries", "source_file", sf)
    spell_list = _arrow_to_df(sl_at).to_dict("records") if len(sl_at) > 0 else []

    ae_at = read_iceberg_filtered("bronze_tabletop", "authority_table_entries", "source_file", sf)
    authority_entries = _arrow_to_df(ae_at).to_dict("records") if len(ae_at) > 0 else []

    meta_at = read_iceberg_filtered("bronze_tabletop", "known_entries_raw", "source_file", sf)
    meta_df = _arrow_to_df(meta_at) if len(meta_at) > 0 else pd.DataFrame()
    if not meta_df.empty:
        meta_df = meta_df[meta_df["entry_class"].notna() & (meta_df["school"].notna() | meta_df["ref_page"].notna())]
    if not meta_df.empty:
        def _val(v):
            return None if pd.isna(v) else v

        spell_meta: dict = {}
        for _, r in meta_df.iterrows():
            name = r["entry_name"].lower()
            ref_page = int(r["ref_page"]) if pd.notna(r.get("ref_page")) else None
            if name not in spell_meta:
                spell_meta[name] = {
                    "school": _val(r["school"]),
                    "sphere": _val(r.get("sphere")),
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

    tr_at = read_iceberg_filtered("bronze_tabletop", "tables_raw", "source_file", sf)
    tables_raw = _arrow_to_df(tr_at).sort_values(["toc_title", "row_index"]).to_dict("records") if len(tr_at) > 0 else []

    page_index_map = dict(zip(
        pages_df["printed_page_num"].astype(int).tolist(),
        pages_df["page_index"].astype(int).tolist(),
    ))
    pdf_path = PDF_DIR / sf

    page_text_masks: dict[int, list[tuple[int, int]]] = {}
    if table_exists("bronze_tabletop", "page_text_masks"):
        mask_at = read_iceberg_filtered("bronze_tabletop", "page_text_masks", "source_file", sf)
        if len(mask_at) > 0:
            mask_df = _arrow_to_df(mask_at).sort_values(["printed_page_num", "char_start"])
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

    # Force-release all Arrow/C memory before heavy Python processing.
    import gc
    gc.collect()
    print(f"  {sf}: starting build_entries_from_pages", flush=True)

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
    print("build_silver_entries: reading file list from bronze", flush=True)
    files_at = read_iceberg("bronze_tabletop", "files")
    files_df = _arrow_to_df(files_at).sort_values("source_file")
    if files_df.empty:
        print("silver_entries: no files in bronze, nothing to write", flush=True)
        return 0
    source_files = files_df["source_file"].tolist()
    print(f"build_silver_entries: {len(source_files)} files to process", flush=True)

    total_rows = 0
    first = True
    for sf in source_files:
        rows = _build_one_file(sf)
        if not rows:
            print(f"  {sf}: 0 entries", flush=True)
            continue
        arrow = pa.Table.from_pylist(rows, schema=SCHEMA)
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
