"""Gold layer models — direct iceberg writes."""
import sys
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, "/workspace")

import pandas as pd
import pyarrow as pa

from dlt.lib.duckdb_reader import get_reader
from dlt.lib.iceberg_catalog import write_iceberg
from dlt.lib.tabletop_cleanup import load_config, chunk_entries
from dlt.lib.stable_keys import make_id

CONFIGS_DIR = Path("/workspace/documents/tabletop_rules/configs")
NAMESPACE = "gold_tabletop"


def _sql_to_iceberg(table_name, sql, ns=None):
    reader = get_reader(ns or ["bronze_tabletop", "silver_tabletop", "gold_tabletop"])
    arrow = reader.execute(sql).fetch_arrow_table()
    reader.close()
    if len(arrow) == 0:
        return 0
    write_iceberg(NAMESPACE, table_name, arrow, overwrite_all=True)
    print(f"  {table_name}: {len(arrow)} rows", flush=True)
    return len(arrow)


def build_gold_toc():
    return _sql_to_iceberg("gold_toc",
        "SELECT toc_id, parent_toc_id, source_file, title, page_start, page_end, "
        "sort_order, depth, is_chapter, is_table, is_excluded, parent_title, "
        "sub_headings, tables FROM silver_tabletop.silver_toc_sections",
        ["silver_tabletop"])


def build_gold_tables():
    return _sql_to_iceberg("gold_tables",
        "SELECT source_file, table_number, table_title, toc_title, toc_id, "
        "parent_title, sort_order, format, row_index, cells "
        "FROM silver_tabletop.silver_tables",
        ["silver_tabletop"])


def build_gold_entries():
    return _sql_to_iceberg("gold_entries",
        "SELECT e.entry_id, t.source_file, t.toc_id, t.title as toc_title, "
        "e.section_title, e.entry_title, e.content, e.char_count, "
        "e.spell_class, e.spell_level, t.sort_order, t.depth, "
        "t.is_chapter, t.is_table, t.is_excluded "
        "FROM silver_tabletop.silver_toc_sections t "
        "LEFT JOIN silver_tabletop.silver_entries e "
        "ON e.source_file = t.source_file AND e.toc_id = t.toc_id "
        "WHERE t.is_excluded = false "
        "ORDER BY t.source_file, t.sort_order, e.entry_title",
        ["silver_tabletop"])


def _extract_field(content: str, field_name: str) -> str | None:
    """Extract a metadata field value from entry content (case-insensitive)."""
    field_lower = field_name.lower() + ":"
    for line in content.split("\n"):
        stripped = line.strip()
        if stripped.lower().startswith(field_lower):
            return stripped[len(field_name) + 1:].strip() or None
    return None


def _build_class_toc_ids(toc_df, class_names_lower: set) -> set:
    """Walk ToC by sort_order. When a class name is hit, mark it and all
    deeper entries after it as class, until next entry at same or shallower depth."""
    toc_sorted = toc_df.sort_values("sort_order")
    class_toc_ids = set()
    current_class_depth = None

    for _, row in toc_sorted.iterrows():
        title = row["title"]
        depth = int(row["depth"])

        if current_class_depth is not None:
            if depth > current_class_depth:
                class_toc_ids.add(int(row["toc_id"]))
                continue
            else:
                current_class_depth = None

        if title.lower().strip() in class_names_lower:
            class_toc_ids.add(int(row["toc_id"]))
            current_class_depth = depth

    return class_toc_ids


def build_gold_entry_index():
    """Gold entry index: cross-reference for structured queries with entry_type."""
    reader = get_reader(["bronze_tabletop", "silver_tabletop"])

    entries_df = reader.execute("SELECT * FROM silver_tabletop.silver_entries").fetchdf()
    toc_df = reader.execute("SELECT * FROM silver_tabletop.silver_toc_sections").fetchdf()
    crosscheck_df = reader.execute("SELECT * FROM silver_tabletop.silver_spell_crosscheck").fetchdf()

    # Build spell lookup from crosscheck (authoritative)
    spell_lookup = {}
    for _, row in crosscheck_df.iterrows():
        spell_lookup[(row["source_file"], row["entry_name"], row["entry_class"])] = {
            "spell_class": row["entry_class"],
            "spell_level": int(row["entry_level"]) if pd.notna(row["entry_level"]) else None,
            "school": row["school"],
            "sphere": row["sphere"],
            "is_reversible": row["is_reversible"],
            "ref_page": int(row["ref_page"]) if pd.notna(row["ref_page"]) else None,
        }

    # Load authority table entries for proficiency whitelist
    authority_df = reader.execute(
        "SELECT source_file, entry_name, entry_type FROM bronze_tabletop.authority_table_entries"
    ).fetchdf()
    reader.close()

    authority_whitelist = {}
    for _, arow in authority_df.iterrows():
        key = (arow["source_file"], arow["entry_type"])
        if key not in authority_whitelist:
            authority_whitelist[key] = set()
        authority_whitelist[key].add(arow["entry_name"].lower().strip())

    all_rows = []

    for sf in entries_df["source_file"].unique():
        config = load_config(Path(sf), CONFIGS_DIR)

        sf_toc = toc_df[toc_df["source_file"] == sf]

        # Build class toc_ids from config class_names
        class_names = config.get("class_names", [])
        class_names_lower = {n.lower().strip() for n in class_names}
        class_toc_ids = _build_class_toc_ids(sf_toc, class_names_lower)

        # Build table toc_ids from is_table flag
        table_toc_ids = set(int(r["toc_id"]) for _, r in sf_toc.iterrows() if r["is_table"])

        # Proficiency whitelist for this file
        prof_whitelist = authority_whitelist.get((sf, "proficiency"), set())

        sf_entries = entries_df[entries_df["source_file"] == sf]

        for _, row in sf_entries.iterrows():
            content = row["content"] or ""
            entry_title = row["entry_title"]
            entry_name = entry_title.lower().strip() if pd.notna(entry_title) and entry_title else ""
            toc_id = int(row["toc_id"])

            # Determine entry_type
            entry_type = "rule"

            spell_class_val = row.get("spell_class")
            if pd.notna(spell_class_val) and spell_class_val:
                entry_type = "spell"
            elif toc_id in table_toc_ids:
                entry_type = "table"
            elif toc_id in class_toc_ids:
                entry_type = "class"
            elif entry_name and entry_name in prof_whitelist:
                entry_type = "proficiency"

            if not entry_name:
                entry_type = "rule"

            spell_class = None
            spell_level = None
            school = row.get("school") if pd.notna(row.get("school")) else None
            sphere = row.get("sphere") if pd.notna(row.get("sphere")) else None
            is_reversible = None
            ref_page = None

            if entry_type == "spell" and entry_name:
                parsed_class = row.get("spell_class") if pd.notna(row.get("spell_class")) else None
                xcheck = None
                if parsed_class:
                    xcheck = spell_lookup.get((sf, entry_name, parsed_class))
                if not xcheck:
                    xcheck = spell_lookup.get((sf, entry_name, "wizard"))
                if not xcheck:
                    xcheck = spell_lookup.get((sf, entry_name, "priest"))

                if xcheck:
                    spell_class = xcheck["spell_class"]
                    spell_level = xcheck["spell_level"]
                    school = xcheck["school"] or school
                    sphere = xcheck["sphere"] or sphere
                    is_reversible = xcheck["is_reversible"]
                    ref_page = xcheck["ref_page"]
                else:
                    spell_class = parsed_class
                    spell_level = int(row["spell_level"]) if pd.notna(row.get("spell_level")) else None
                    if not spell_class:
                        entry_type = "rule"

            all_rows.append({
                "entry_id": int(row["entry_id"]),
                "source_file": sf,
                "entry_title": entry_title if pd.notna(entry_title) else None,
                "entry_type": entry_type,
                "spell_level": spell_level,
                "spell_class": spell_class,
                "school": school,
                "sphere": sphere,
                "is_reversible": is_reversible,
                "ref_page": ref_page,
                "components": _extract_field(content, "Components") or _extract_field(content, "Component"),
                "saving_throw": _extract_field(content, "Saving Throw"),
                "range_text": _extract_field(content, "Range"),
                "duration_text": _extract_field(content, "Duration"),
                "casting_time": _extract_field(content, "Casting Time"),
            })

    if not all_rows:
        return 0

    schema = pa.schema([
        ("entry_id", pa.int64()), ("source_file", pa.string()),
        ("entry_title", pa.string()), ("entry_type", pa.string()),
        ("spell_level", pa.int32()), ("spell_class", pa.string()),
        ("school", pa.string()), ("sphere", pa.string()),
        ("is_reversible", pa.bool_()), ("ref_page", pa.int32()),
        ("components", pa.string()), ("saving_throw", pa.string()),
        ("range_text", pa.string()), ("duration_text", pa.string()),
        ("casting_time", pa.string()),
    ])
    write_iceberg(NAMESPACE, "gold_entry_index",
                  pa.Table.from_pylist(all_rows, schema=schema), overwrite_all=True)
    print(f"  gold_entry_index: {len(all_rows)} rows", flush=True)
    return len(all_rows)


def build_gold_chunks():
    """Gold chunks: query-ready chunks from silver entries."""
    reader = get_reader(["silver_tabletop"])

    entries_df = reader.execute("SELECT * FROM silver_tabletop.silver_entries").fetchdf()
    toc_df = reader.execute("SELECT * FROM silver_tabletop.silver_toc_sections").fetchdf()
    reader.close()

    # Build toc lookup for chunk_entries (needs toc_entry dicts)
    toc_by_title = {}
    for _, row in toc_df.iterrows():
        toc_by_title[(row["source_file"], row["title"])] = {
            "title": row["title"],
            "toc_id": int(row["toc_id"]),
            "page_start": int(row["page_start"]),
            "page_end": int(row["page_end"]),
            "is_excluded": bool(row["is_excluded"]),
        }

    all_chunks = []
    chunk_index_by_entry = {}
    now = datetime.now(timezone.utc)

    for sf in entries_df["source_file"].unique():
        config = load_config(Path(sf), CONFIGS_DIR)
        sf_entries = entries_df[entries_df["source_file"] == sf]

        entry_list = []
        for _, row in sf_entries.iterrows():
            toc_key = (row["source_file"], row["toc_title"])
            toc_entry = toc_by_title.get(toc_key, {"title": row["toc_title"]})
            page_str = row["page_numbers"] if pd.notna(row["page_numbers"]) and row["page_numbers"] else ""
            pages = [int(p) for p in page_str.split(",") if p.strip()]

            entry_list.append({
                "toc_entry": toc_entry,
                "entry_id": int(row["entry_id"]),
                "section_title": row["section_title"] if pd.notna(row["section_title"]) else None,
                "entry_title": row["entry_title"] if pd.notna(row["entry_title"]) else None,
                "content": row["content"] if pd.notna(row["content"]) else "",
                "page_numbers": pages if pages else [0],
            })

        chunks = chunk_entries(entry_list, config)

        for chunk in chunks:
            toc = chunk["toc_entry"]
            toc_title = toc.get("title", "")
            entry_title = chunk.get("entry_title")

            entry_key = (sf, toc_title, entry_title)
            idx = chunk_index_by_entry.get(entry_key, 0)
            chunk_index_by_entry[entry_key] = idx + 1

            entry_id = chunk["entry_id"]
            chunk_id = make_id("chunk_id", {
                "source_file": sf, "toc_title": toc_title,
                "entry_title": entry_title, "chunk_index": idx,
            })

            all_chunks.append({
                "chunk_id": chunk_id,
                "entry_id": entry_id,
                "source_file": sf,
                "toc_id": toc.get("toc_id"),
                "section_title": chunk.get("section_title"),
                "entry_title": entry_title,
                "content": chunk["content"],
                "page_numbers": chunk["page_numbers"],
                "char_count": len(chunk["content"]),
                "chunk_type": chunk.get("chunk_type", "content"),
                "chunked_at": now,
            })

    if not all_chunks:
        return 0

    schema = pa.schema([
        ("chunk_id", pa.int64()), ("entry_id", pa.int64()),
        ("source_file", pa.string()), ("toc_id", pa.int64()),
        ("section_title", pa.string()), ("entry_title", pa.string()),
        ("content", pa.string()), ("page_numbers", pa.string()),
        ("char_count", pa.int64()), ("chunk_type", pa.string()),
        ("chunked_at", pa.timestamp("us", tz="UTC")),
    ])
    write_iceberg(NAMESPACE, "gold_chunks",
                  pa.Table.from_pylist(all_chunks, schema=schema), overwrite_all=True)
    print(f"  gold_chunks: {len(all_chunks)} rows", flush=True)
    return len(all_chunks)


def build_gold_entry_descriptions():
    return _sql_to_iceberg("gold_entry_descriptions",
        "SELECT d.entry_id, d.source_file, i.entry_type, d.content "
        "FROM silver_tabletop.silver_entry_descriptions d "
        "JOIN gold_tabletop.gold_entry_index i ON d.entry_id = i.entry_id",
        ["silver_tabletop", "gold_tabletop"])


def build_gold_files():
    return _sql_to_iceberg("gold_files",
        "SELECT c.source_file, sf.total_pages, count(*) as total_chunks, "
        "count(distinct t.toc_id) as total_toc_entries, "
        "current_timestamp as built_at "
        "FROM gold_tabletop.gold_chunks c "
        "JOIN silver_tabletop.silver_files sf ON c.source_file = sf.source_file "
        "JOIN gold_tabletop.gold_toc t ON c.toc_id = t.toc_id "
        "GROUP BY c.source_file, sf.total_pages",
        ["silver_tabletop", "gold_tabletop"])
