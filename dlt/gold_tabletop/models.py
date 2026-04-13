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
