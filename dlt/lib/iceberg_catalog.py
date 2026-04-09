"""Shared Iceberg catalog connection and table management.

All Iceberg operations go through this module. Never connect to the catalog
directly — call get_catalog(), write_iceberg(), or read_iceberg().

Config loaded from /workspace/config/lakehouse.yaml. Nothing hardcoded.
"""
import shutil
from functools import lru_cache
from pathlib import Path

import pyarrow as pa
import yaml
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import EqualTo


CONFIG_PATH = Path("/workspace/config/lakehouse.yaml")


@lru_cache
def _load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def get_catalog() -> SqlCatalog:
    """Return a PyIceberg SqlCatalog connected to PostgreSQL."""
    cfg = _load_config()["catalog"]
    return SqlCatalog(
        cfg["name"],
        **{k: v for k, v in cfg.items() if k not in ("name", "type")},
    )


def ensure_namespace(catalog: SqlCatalog, namespace: str) -> None:
    """Create namespace if it doesn't exist (idempotent)."""
    existing = [ns[0] for ns in catalog.list_namespaces()]
    if namespace not in existing:
        catalog.create_namespace(namespace)


def write_iceberg(
    namespace: str,
    table_name: str,
    arrow_table: pa.Table,
    overwrite_filter: str | None = None,
    overwrite_filter_value: str | None = None,
    overwrite_all: bool = False,
) -> None:
    """Write an Arrow table to Iceberg. Atomic — either both catalog entry
    and data files exist after the call, or neither does.

    If overwrite_all is True, drops and recreates the table (full replace).
    If overwrite_filter and overwrite_filter_value are set, deletes existing
    rows matching the filter before appending. This preserves the
    delete-and-insert pattern used by the bronze layer.

    The atomicity guarantee is enforced by wrapping create+append in a try
    block that cleans up both the catalog row and the data directory on
    failure. The catalog must never have an entry without matching on-disk
    metadata — that's the "stale entry" bug class this method exists to
    prevent.
    """
    catalog = get_catalog()
    ensure_namespace(catalog, namespace)
    full_name = f"{namespace}.{table_name}"
    cfg = _load_config()
    warehouse = Path(cfg["catalog"]["warehouse"])
    table_dir = warehouse / namespace / table_name

    def _drop_everything() -> None:
        """Drop catalog entry AND wipe on-disk data. Idempotent."""
        try:
            catalog.drop_table(full_name)
        except Exception:
            pass
        if table_dir.exists():
            shutil.rmtree(table_dir)

    if overwrite_all:
        _drop_everything()
        try:
            tbl = catalog.create_table(full_name, schema=arrow_table.schema)
            tbl.append(arrow_table)
        except Exception:
            # create or append failed — leave neither catalog nor data behind
            _drop_everything()
            raise
        return

    # Append / upsert path
    try:
        tbl = catalog.load_table(full_name)
        # Evolve schema if incoming table has new columns
        existing_names = {f.name for f in tbl.schema().fields}
        new_fields = [f for f in arrow_table.schema if f.name not in existing_names]
        if new_fields:
            from pyiceberg.io.pyarrow import visit_pyarrow, _ConvertToIceberg
            converter = _ConvertToIceberg()
            with tbl.update_schema() as update:
                for field in new_fields:
                    iceberg_type = visit_pyarrow(field.type, converter)
                    update.add_column(field.name, iceberg_type)
            tbl = catalog.load_table(full_name)
    except Exception:
        # Table doesn't exist yet (or its catalog entry is stale).
        # _drop_everything ensures we start clean before creating.
        _drop_everything()
        try:
            tbl = catalog.create_table(full_name, schema=arrow_table.schema)
        except Exception:
            _drop_everything()
            raise

    try:
        if overwrite_filter and overwrite_filter_value is not None:
            tbl.delete(EqualTo(overwrite_filter, overwrite_filter_value))
        tbl.append(arrow_table)
    except Exception:
        # Append failed mid-write. The table existed before; we can't roll
        # back the delete cleanly, so the safest action is to drop the
        # whole table and let the next run recreate it. This is consistent
        # with the lakehouse rule: never leave half-state behind.
        _drop_everything()
        raise


def read_iceberg(namespace: str, table_name: str) -> pa.Table:
    """Read an entire Iceberg table as Arrow."""
    catalog = get_catalog()
    tbl = catalog.load_table(f"{namespace}.{table_name}")
    return tbl.scan().to_arrow()


def read_iceberg_filtered(
    namespace: str,
    table_name: str,
    filter_col: str,
    filter_value: str,
) -> pa.Table:
    """Read filtered rows from an Iceberg table."""
    catalog = get_catalog()
    tbl = catalog.load_table(f"{namespace}.{table_name}")
    return tbl.scan(row_filter=EqualTo(filter_col, filter_value)).to_arrow()


def table_exists(namespace: str, table_name: str) -> bool:
    """Check if an Iceberg table exists."""
    catalog = get_catalog()
    try:
        catalog.load_table(f"{namespace}.{table_name}")
        return True
    except Exception:
        return False


def list_tables(namespace: str) -> list[str]:
    """List all table names in a namespace."""
    catalog = get_catalog()
    ensure_namespace(catalog, namespace)
    return [t[1] for t in catalog.list_tables(namespace)]


def list_namespaces() -> list[str]:
    """Return all namespaces present in the catalog."""
    catalog = get_catalog()
    return [ns[0] for ns in catalog.list_namespaces()]


def list_all_tables() -> dict[str, list[str]]:
    """Return {namespace: [table_name, ...]} for every namespace in the catalog.

    Use this anywhere code needs "what tables exist". Never hardcode the answer.
    """
    catalog = get_catalog()
    result: dict[str, list[str]] = {}
    for ns_tuple in catalog.list_namespaces():
        ns = ns_tuple[0]
        result[ns] = [t[1] for t in catalog.list_tables(ns)]
    return result
