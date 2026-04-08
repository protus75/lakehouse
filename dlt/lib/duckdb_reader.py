"""DuckDB read connection with Iceberg views pre-loaded.

Replaces all duckdb.connect(DB_PATH) calls in read consumers.
Views match existing schema names (bronze_tabletop.files, etc.)
so SQL queries don't need changes.

Config loaded from /workspace/config/lakehouse.yaml. Nothing hardcoded.
"""
from pathlib import Path

import duckdb
import yaml

from dlt.lib.iceberg_catalog import get_catalog, list_tables, ensure_namespace


CONFIG_PATH = Path("/workspace/config/lakehouse.yaml")


def _load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def get_reader(namespaces: list[str] | None = None) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection with Iceberg views for all namespaces.

    Args:
        namespaces: Which namespaces to create views for.
                    Defaults to all three (bronze, silver, gold).
    """
    if namespaces is None:
        cfg = _load_config()
        namespaces = list(cfg["namespaces"].values())

    conn = duckdb.connect()

    conn.execute("INSTALL iceberg; LOAD iceberg;")
    conn.execute("SET unsafe_enable_version_guessing=true;")

    catalog_cfg = _load_config()["catalog"]
    warehouse = catalog_cfg["warehouse"]
    catalog = get_catalog()

    for ns in namespaces:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {ns}")
        ensure_namespace(catalog, ns)
        for table_name in list_tables(ns):
            s3_path = f"{warehouse}/{ns}/{table_name}"
            conn.execute(f"""
                CREATE OR REPLACE VIEW {ns}.{table_name} AS
                SELECT * FROM iceberg_scan('{s3_path}')
            """)

    return conn
