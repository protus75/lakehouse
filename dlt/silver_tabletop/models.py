"""Silver layer models — direct iceberg writes via get_reader() + write_iceberg().

Each function reads from bronze/silver iceberg tables, runs SQL or Python
logic, and writes the result to iceberg. No dbt, no duckdb file, no
intermediate formats.

Called by Dagster assets in dagster/lakehouse_assets/assets.py.
"""
import sys
from pathlib import Path

sys.path.insert(0, "/workspace")

import pandas as pd
import pyarrow as pa

from dlt.lib.duckdb_reader import get_reader
from dlt.lib.iceberg_catalog import write_iceberg
from dlt.lib.tabletop_cleanup import load_config, _detect_watermarks
from dlt.lib.stable_keys import make_id


def _val(v):
    """Convert pandas NA/NaN to Python None for safe truthiness checks."""
    return None if pd.isna(v) else v

CONFIGS_DIR = Path("/workspace/documents/tabletop_rules/configs")
NAMESPACE = "silver_tabletop"


def _execute_sql_to_iceberg(table_name: str, sql: str, namespaces: list[str] | None = None) -> int:
    """Run SQL against get_reader(), write result to iceberg. Returns row count."""
    reader = get_reader(namespaces or ["bronze_tabletop", "silver_tabletop"])
    arrow = reader.execute(sql).fetch_arrow_table()
    reader.close()
    if len(arrow) == 0:
        print(f"  {table_name}: 0 rows, skipping write", flush=True)
        return 0
    write_iceberg(NAMESPACE, table_name, arrow, overwrite_all=True)
    print(f"  {table_name}: {len(arrow)} rows", flush=True)
    return len(arrow)


def build_silver_files() -> int:
    """silver_files: file-level metadata aggregated from entries."""
    return _execute_sql_to_iceberg("silver_files", """
        SELECT
            e.source_file,
            f.pdf_size_bytes,
            f.total_pages,
            count(*) as total_entries,
            count(case when e.entry_title is not null then 1 end) as named_entries,
            sum(e.char_count) as total_chars,
            current_timestamp as processed_at
        FROM silver_tabletop.silver_entries e
        JOIN bronze_tabletop.files f ON e.source_file = f.source_file
        GROUP BY e.source_file, f.pdf_size_bytes, f.total_pages
    """)


def build_silver_known_entries() -> int:
    """silver_known_entries: deduplicated index entries with merged school/sphere."""
    return _execute_sql_to_iceberg("silver_known_entries", """
        WITH raw_entries AS (
            SELECT source_file, entry_name, entry_class, entry_level,
                   ref_page, source_section, school, sphere
            FROM bronze_tabletop.known_entries_raw
        ),
        index_entries AS (
            SELECT
                r.source_file, r.entry_name, r.entry_class, r.entry_level, r.ref_page,
                coalesce(r.school, s5.school) as school,
                coalesce(r.sphere, s6.sphere) as sphere
            FROM raw_entries r
            LEFT JOIN raw_entries s5
                ON r.source_file = s5.source_file AND r.entry_name = s5.entry_name
                AND s5.school IS NOT NULL
            LEFT JOIN raw_entries s6
                ON r.source_file = s6.source_file AND r.entry_name = s6.entry_name
                AND s6.sphere IS NOT NULL
            WHERE r.source_section LIKE '%Spell Index%'
               OR r.source_section LIKE '%General Index%'
               OR r.entry_class IS NOT NULL
        ),
        ranked AS (
            SELECT *,
                row_number() OVER (
                    PARTITION BY source_file, entry_name, entry_class
                    ORDER BY ref_page NULLS LAST, school NULLS LAST, sphere NULLS LAST
                ) AS rn
            FROM index_entries
        )
        SELECT source_file, entry_name, entry_class, entry_level,
               ref_page, school, sphere
        FROM ranked WHERE rn = 1
    """, ["bronze_tabletop"])


def build_silver_tables() -> int:
    """silver_tables: table rows with ToC linkage and HTML cleanup."""
    return _execute_sql_to_iceberg("silver_tables", """
        SELECT
            t.source_file, t.table_number, t.table_title, t.toc_title,
            s.toc_id, s.parent_title, s.sort_order,
            t.format, t.row_index,
            regexp_replace(
                regexp_replace(t.cells, '<[^>]+>', ' ', 'g'),
                '\\s+', ' ', 'g'
            ) as cells
        FROM bronze_tabletop.tables_raw t
        LEFT JOIN silver_tabletop.silver_toc_sections s
            ON t.source_file = s.source_file
            AND t.toc_title = s.title
            AND s.is_table = true
    """)


def build_silver_page_anchors() -> int:
    """silver_page_anchors: markdown position → page mapping."""
    from dlt.lib.tabletop_cleanup import _build_page_position_map

    reader = get_reader(["bronze_tabletop"])
    files_df = reader.execute("SELECT source_file FROM bronze_tabletop.files").fetchdf()
    all_rows = []

    for sf in files_df["source_file"].tolist():
        config = load_config(Path(sf), CONFIGS_DIR)
        marker_df = reader.execute(
            "SELECT markdown_text FROM bronze_tabletop.marker_extractions WHERE source_file = ?", [sf]
        ).fetchdf()
        if marker_df.empty:
            continue
        markdown = marker_df.iloc[0]["markdown_text"]

        pages_df = reader.execute(
            "SELECT page_index, page_text, printed_page_num FROM bronze_tabletop.page_texts "
            "WHERE source_file = ? ORDER BY page_index", [sf]
        ).fetchdf()
        page_texts = pages_df["page_text"].tolist()
        page_printed = dict(zip(pages_df["page_index"].tolist(), pages_df["printed_page_num"].tolist()))
        total_pages = len(page_texts)

        # Strip watermarks from markdown
        wm_df = reader.execute(
            "SELECT watermark_text FROM bronze_tabletop.watermarks WHERE source_file = ?", [sf]
        ).fetchdf()
        if not wm_df.empty:
            wm_set = set(wm_df["watermark_text"].tolist())
            lines = [l for l in markdown.split("\n") if l.strip() not in wm_set]
            markdown = "\n".join(lines)

        anchors = _build_page_position_map(markdown, page_texts, page_printed, total_pages, config)
        for md_pos, page_idx in anchors:
            printed = page_printed.get(page_idx, page_idx)
            all_rows.append({
                "source_file": sf, "markdown_position": md_pos,
                "page_index": page_idx, "printed_page_num": printed,
            })

    reader.close()
    if not all_rows:
        return 0
    write_iceberg(NAMESPACE, "silver_page_anchors", pa.Table.from_pylist(all_rows), overwrite_all=True)
    print(f"  silver_page_anchors: {len(all_rows)} rows", flush=True)
    return len(all_rows)


def build_silver_toc_sections() -> int:
    """silver_toc_sections: ToC entries with stable hash keys and sub_headings."""
    reader = get_reader(["bronze_tabletop", "silver_tabletop"])
    files_df = reader.execute("SELECT source_file FROM bronze_tabletop.files").fetchdf()
    all_rows = []

    for sf in files_df["source_file"].tolist():
        # Get latest run's ToC
        toc_df = reader.execute(
            "SELECT title, page_start, page_end, sort_order, depth, is_chapter, "
            "is_table, is_excluded, parent_title "
            "FROM bronze_tabletop.toc_raw WHERE source_file = ? ORDER BY sort_order", [sf]
        ).fetchdf()

        # Get sub_headings from silver_entries
        entries_df = reader.execute(
            "SELECT toc_title, entry_title "
            "FROM silver_tabletop.silver_entries "
            "WHERE source_file = ? AND entry_title IS NOT NULL "
            "ORDER BY toc_title, entry_title", [sf]
        ).fetchdf()

        # Build sub_headings lookup
        sub_headings = {}
        for _, r in entries_df.iterrows():
            sub_headings.setdefault(r["toc_title"], []).append(r["entry_title"])

        # Build parent_toc_id lookup
        toc_id_by_title = {}
        for _, row in toc_df.iterrows():
            title = row["title"]
            parent = _val(row["parent_title"]) or ""
            tid = make_id("toc_id", {
                "source_file": sf, "title": title,
                "parent_title": parent,
                "page_start": str(int(row["page_start"])),
            })
            toc_id_by_title[title] = tid

        for _, row in toc_df.iterrows():
            title = row["title"]
            parent = _val(row["parent_title"]) or ""
            tid = toc_id_by_title[title]
            parent_tid = toc_id_by_title.get(parent)
            subs = sub_headings.get(title, [])

            all_rows.append({
                "toc_id": tid,
                "parent_toc_id": parent_tid,
                "source_file": sf,
                "title": title,
                "page_start": int(row["page_start"]),
                "page_end": int(row["page_end"]) if row["page_end"] else 9999,
                "sort_order": int(row["sort_order"]),
                "depth": int(row["depth"]) if row["depth"] else 0,
                "is_chapter": bool(row["is_chapter"]),
                "is_table": bool(row.get("is_table", False)),
                "is_excluded": bool(row["is_excluded"]),
                "parent_title": parent or None,
                "sub_headings": "; ".join(subs) if subs else None,
                "tables": None,
            })

    reader.close()
    if not all_rows:
        return 0

    schema = pa.schema([
        ("toc_id", pa.int64()), ("parent_toc_id", pa.int64()),
        ("source_file", pa.string()), ("title", pa.string()),
        ("page_start", pa.int32()), ("page_end", pa.int32()),
        ("sort_order", pa.int32()), ("depth", pa.int32()),
        ("is_chapter", pa.bool_()), ("is_table", pa.bool_()), ("is_excluded", pa.bool_()),
        ("parent_title", pa.string()), ("sub_headings", pa.string()), ("tables", pa.string()),
    ])
    write_iceberg(NAMESPACE, "silver_toc_sections", pa.Table.from_pylist(all_rows, schema=schema), overwrite_all=True)
    print(f"  silver_toc_sections: {len(all_rows)} rows", flush=True)
    return len(all_rows)


def build_silver_spell_meta() -> int:
    """silver_spell_meta: structured spell metadata parsed from entry content."""
    reader = get_reader(["silver_tabletop"])
    files_df = reader.execute(
        "SELECT DISTINCT source_file FROM silver_tabletop.silver_entries"
    ).fetchdf()
    all_rows = []

    for sf in files_df["source_file"].tolist():
        config = load_config(Path(sf), CONFIGS_DIR)
        spells_df = reader.execute(
            "SELECT entry_id, source_file, content, school, sphere "
            "FROM silver_tabletop.silver_entries "
            "WHERE source_file = ? AND spell_level IS NOT NULL", [sf]
        ).fetchdf()

        field_names = config.get("metadata_field_names", [])

        for _, row in spells_df.iterrows():
            content = row["content"] or ""
            fields = {}
            for fname in field_names:
                key = fname.lower().replace(" ", "_")
                # Find "FieldName:" in content
                idx = content.lower().find(fname.lower() + ":")
                if idx >= 0:
                    rest = content[idx + len(fname) + 1:]
                    # Take until next field or newline
                    val = rest.split("\n")[0].strip()
                    fields[key] = val
                else:
                    fields[key] = ""

            # Extract material component from last lines
            material_text = ""
            lines = content.split("\n")
            for i in range(max(0, len(lines) - 8), len(lines)):
                line = lines[i].strip()
                if line.lower().startswith("the material component") or \
                   line.lower().startswith("the material") or \
                   line.lower().startswith("material component"):
                    material_text = "\n".join(l.strip() for l in lines[i:]).strip()
                    break

            all_rows.append({
                "entry_id": int(row["entry_id"]),
                "source_file": sf,
                "school": _val(row["school"]) or "",
                "sphere": _val(row["sphere"]) or "",
                "reversible": "",  # populated from content if present
                "range": fields.get("range", ""),
                "components": fields.get("components", fields.get("component", "")),
                "duration": fields.get("duration", ""),
                "casting_time": fields.get("casting_time", ""),
                "area_of_effect": fields.get("area_of_effect", ""),
                "saving_throw": fields.get("saving_throw", ""),
                "material_component_text": material_text,
            })

    reader.close()
    if not all_rows:
        return 0

    schema = pa.schema([
        ("entry_id", pa.int64()), ("source_file", pa.string()),
        ("school", pa.string()), ("sphere", pa.string()), ("reversible", pa.string()),
        ("range", pa.string()), ("components", pa.string()), ("duration", pa.string()),
        ("casting_time", pa.string()), ("area_of_effect", pa.string()),
        ("saving_throw", pa.string()), ("material_component_text", pa.string()),
    ])
    write_iceberg(NAMESPACE, "silver_spell_meta", pa.Table.from_pylist(all_rows, schema=schema), overwrite_all=True)
    print(f"  silver_spell_meta: {len(all_rows)} rows", flush=True)
    return len(all_rows)


def build_silver_spell_crosscheck() -> int:
    """silver_spell_crosscheck: cross-reference spell index vs spell list."""
    from rapidfuzz import fuzz

    reader = get_reader(["bronze_tabletop", "silver_tabletop"])

    files_df = reader.execute("SELECT DISTINCT source_file FROM bronze_tabletop.files").fetchdf()
    all_rows = []

    for sf in files_df["source_file"].tolist():
        config = load_config(Path(sf), CONFIGS_DIR)

        # Known entries from index (Appendix 7)
        known_df = reader.execute(
            "SELECT entry_name, entry_class, entry_level, ref_page, school, sphere "
            "FROM silver_tabletop.silver_known_entries WHERE source_file = ?", [sf]
        ).fetchdf()

        # Spell list entries (Appendix 1)
        spell_list_df = reader.execute(
            "SELECT entry_name, entry_class, entry_level, is_reversible "
            "FROM bronze_tabletop.spell_list_entries WHERE source_file = ?", [sf]
        ).fetchdf()

        # Build lookups
        spell_list_lookup = {}
        for _, r in spell_list_df.iterrows():
            key = (r["entry_name"].lower().strip(), r["entry_class"].lower().strip())
            spell_list_lookup[key] = r

        # Cross-check each known spell entry
        exclude_names = set(n.lower() for n in config.get("exclude_entry_names", []))
        for _, r in known_df.iterrows():
            if not _val(r["entry_class"]):
                continue
            name = r["entry_name"]
            cls = r["entry_class"]
            if name.lower() in exclude_names:
                continue

            key = (name.lower().strip(), cls.lower().strip())
            in_list = key in spell_list_lookup
            sl_row = spell_list_lookup.get(key, {})

            level_mismatch = False
            if in_list and pd.notna(r["entry_level"]):
                sl_level = sl_row.get("entry_level")
                if pd.notna(sl_level) and pd.notna(r["entry_level"]):
                    level_mismatch = int(sl_level) != int(r["entry_level"])

            all_rows.append({
                "source_file": sf,
                "entry_name": name,
                "entry_class": cls,
                "entry_level": int(r["entry_level"]) if pd.notna(r["entry_level"]) else None,
                "ref_page": int(r["ref_page"]) if pd.notna(r["ref_page"]) else None,
                "school": _val(r["school"]),
                "sphere": _val(r["sphere"]),
                "is_reversible": bool(sl_row.get("is_reversible", False)) if in_list else False,
                "in_spell_list": in_list,
                "in_school_index": pd.notna(r["school"]),
                "in_sphere_index": pd.notna(r["sphere"]),
                "level_mismatch": level_mismatch,
            })

    reader.close()
    if not all_rows:
        return 0

    schema = pa.schema([
        ("source_file", pa.string()), ("entry_name", pa.string()),
        ("entry_class", pa.string()), ("entry_level", pa.int32()),
        ("ref_page", pa.int32()), ("school", pa.string()), ("sphere", pa.string()),
        ("is_reversible", pa.bool_()), ("in_spell_list", pa.bool_()),
        ("in_school_index", pa.bool_()), ("in_sphere_index", pa.bool_()),
        ("level_mismatch", pa.bool_()),
    ])
    write_iceberg(NAMESPACE, "silver_spell_crosscheck",
                  pa.Table.from_pylist(all_rows, schema=schema), overwrite_all=True)
    print(f"  silver_spell_crosscheck: {len(all_rows)} rows", flush=True)
    return len(all_rows)


def build_silver_entry_descriptions() -> int:
    """silver_entry_descriptions: clean descriptions with metadata stripped."""
    reader = get_reader(["silver_tabletop"])
    files_df = reader.execute(
        "SELECT DISTINCT source_file FROM silver_tabletop.silver_entries"
    ).fetchdf()
    all_rows = []

    for sf in files_df["source_file"].tolist():
        config = load_config(Path(sf), CONFIGS_DIR)
        entries_df = reader.execute(
            "SELECT entry_id, source_file, content, spell_level "
            "FROM silver_tabletop.silver_entries WHERE source_file = ? AND content IS NOT NULL", [sf]
        ).fetchdf()

        field_names = config.get("metadata_field_names", [])

        for _, row in entries_df.iterrows():
            content = row["content"]
            is_spell = pd.notna(row["spell_level"])

            if is_spell and field_names:
                # Strip spell header (key-value metadata lines)
                lines = content.split("\n")
                desc_start = 0
                for i, line in enumerate(lines):
                    stripped = line.strip()
                    if any(stripped.lower().startswith(f.lower() + ":") for f in field_names):
                        desc_start = i + 1
                        continue
                    # Check continuation lines (indented or very short)
                    if i > 0 and desc_start == i and (line.startswith("  ") or len(stripped) < 5):
                        desc_start = i + 1
                        continue
                    break
                content = "\n".join(lines[desc_start:]).strip()

                # Strip material component footer
                result_lines = content.split("\n")
                for i in range(len(result_lines) - 1, max(len(result_lines) - 8, -1), -1):
                    line = result_lines[i].strip().lower()
                    if line.startswith("the material component") or \
                       line.startswith("the material") or \
                       line.startswith("material component"):
                        content = "\n".join(result_lines[:i]).strip()
                        break

            if not content.strip():
                continue

            all_rows.append({
                "entry_id": int(row["entry_id"]),
                "source_file": sf,
                "description_type": "original",
                "content": content.strip(),
            })

    reader.close()
    if not all_rows:
        return 0

    schema = pa.schema([
        ("entry_id", pa.int64()), ("source_file", pa.string()),
        ("description_type", pa.string()), ("content", pa.string()),
    ])
    write_iceberg(NAMESPACE, "silver_entry_descriptions",
                  pa.Table.from_pylist(all_rows, schema=schema), overwrite_all=True)
    print(f"  silver_entry_descriptions: {len(all_rows)} rows", flush=True)
    return len(all_rows)
