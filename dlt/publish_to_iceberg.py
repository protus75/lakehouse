"""Publish dbt-materialized silver/gold tables to Iceberg on S3.

Run after `dbt build` completes. Reads tables from DuckDB (where dbt
materialized them) and writes to Iceberg via PyIceberg.

Usage:
    docker exec lakehouse-workspace bash -c "cd /workspace && python -u -m dlt.publish_to_iceberg"
"""
import sys
sys.path.insert(0, "/workspace")

from pathlib import Path

import duckdb

from dlt.lib.iceberg_catalog import write_iceberg
from dlt.lib.tabletop_cleanup import _log

DB_PATH = "/workspace/db/lakehouse.duckdb"

PUBLISH_MAP = {
    "silver_tabletop": [
        "silver_entries",
        "silver_page_anchors",
        "silver_known_entries",
        "silver_toc_sections",
        "silver_files",
        "silver_spell_crosscheck",
    ],
    "gold_tabletop": [
        "gold_chunks",
        "gold_entry_index",
        "gold_toc",
        "gold_files",
    ],
}


def publish() -> None:
    """Read silver/gold from DuckDB, write to Iceberg on S3."""
    conn = duckdb.connect(DB_PATH, read_only=True)

    for namespace, tables in PUBLISH_MAP.items():
        for table_name in tables:
            full_name = f"{namespace}.{table_name}"
            try:
                arrow = conn.execute(f"SELECT * FROM {full_name}").fetch_arrow_table()
                row_count = len(arrow)
                write_iceberg(namespace, table_name, arrow, overwrite_all=True)
                _log(f"  Published {full_name}: {row_count} rows")
            except Exception as e:
                _log(f"  SKIP {full_name}: {e}")

    conn.close()
    _log("Publish to Iceberg complete.")


if __name__ == "__main__":
    publish()
