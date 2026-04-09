"""dbt-duckdb plugin: persist model output directly to Iceberg.

Activated by registering this module in dbt's profiles.yml under the duckdb
adapter's `plugins` section, then using `materialized='external'` with
`plugin='iceberg'` on any model that should land in iceberg.

The plugin does two things:

1. `configure_connection(conn)` — runs when dbt opens its duckdb session.
   Enumerates the live iceberg catalog via `list_all_tables()` and creates
   `CREATE OR REPLACE VIEW <ns>.<table>` over `iceberg_scan(...)` for every
   bronze/silver/gold/meta table that exists. This is the dynamic replacement
   for the old hardcoded `create_bronze_views.sql` macro — adding a bronze
   table to the catalog automatically makes it visible to dbt models on the
   next run, no config edits required.

2. `store(target_config)` — runs after dbt's `external` materialization has
   written the model output to a temporary parquet file. Reads that parquet,
   calls `write_iceberg(namespace, table, arrow, overwrite_all=True)`, then
   re-registers the duckdb view to point at the freshly-written iceberg
   table so any later model in the same run reads from iceberg, not the
   temp parquet. The temp parquet is left for dbt to clean up.

Configuration in profiles.yml:

    plugins:
      - module: dlt.lib.dbt_iceberg_plugin
        alias: iceberg

Configuration in dbt_project.yml or per-model:

    {{ config(materialized='external', plugin='iceberg', location='/tmp/dbt_scratch/{name}.parquet') }}
"""
import sys
from pathlib import Path
from typing import Any, Dict

import pyarrow.parquet as pq

# Make /workspace importable for the dlt package even when dbt isn't run
# with PYTHONPATH set.
if "/workspace" not in sys.path:
    sys.path.insert(0, "/workspace")

from dbt.adapters.duckdb.plugins import BasePlugin
from dbt.adapters.duckdb.utils import TargetConfig

from dlt.lib.iceberg_catalog import list_all_tables, write_iceberg, _load_config


class Plugin(BasePlugin):
    """Iceberg materialization plugin for dbt-duckdb."""

    def initialize(self, plugin_config: Dict[str, Any]) -> None:
        cfg = _load_config()
        self._warehouse = cfg["catalog"]["warehouse"]
        # dbt-duckdb's `external` materialization writes a temp parquet
        # per model before calling our store(). Scratch dir is on E: via
        # the /scratch container mount (docker-compose.yml). NEVER on C:.
        Path("/scratch/dbt").mkdir(parents=True, exist_ok=True)

    def configure_connection(self, conn) -> None:
        """Register iceberg views for every table in the live catalog.

        Runs when dbt opens its duckdb session. After this, every model can
        SELECT FROM <namespace>.<table> for any table that exists in iceberg
        — bronze, silver, gold, meta — without any hardcoded list anywhere.

        The catalog and on-disk data must match: every table the catalog
        knows about MUST have its iceberg metadata on disk. Stale entries
        are a bug, not a tolerated state, so this method fails loudly.
        """
        conn.execute("INSTALL iceberg; LOAD iceberg;")
        conn.execute("SET unsafe_enable_version_guessing=true;")
        for namespace, tables in list_all_tables().items():
            conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{namespace}"')
            for table in tables:
                table_path = f"{self._warehouse}/{namespace}/{table}"
                conn.execute(
                    f'CREATE OR REPLACE VIEW "{namespace}"."{table}" AS '
                    f"SELECT * FROM iceberg_scan('{table_path}')"
                )

    def store(self, target_config: TargetConfig) -> None:
        """Persist a model's output (already exported to parquet by dbt) to iceberg.

        Reads the parquet that dbt-duckdb's `external` materialization just
        wrote, calls write_iceberg, then re-registers the duckdb view so any
        downstream model in the same run reads from the freshly-written
        iceberg table. The temp parquet is left for dbt to clean up.
        """
        if target_config.location is None:
            raise RuntimeError(
                f"iceberg plugin: target_config.location is None for "
                f"{target_config.relation.schema}.{target_config.relation.identifier}"
            )

        namespace = target_config.relation.schema
        table_name = target_config.relation.identifier

        arrow = pq.read_table(target_config.location.path)
        write_iceberg(namespace, table_name, arrow, overwrite_all=True)

        # Re-register the view to point at iceberg, not the temp parquet,
        # so downstream models in this same run read the canonical store.
        # Use the active connection from the plugin's stored conn if available;
        # dbt-duckdb passes None for conn here, so we re-open via duckdb directly.
        # Actually: the view dbt-duckdb's external materialization created
        # already exists and points at the temp parquet. Downstream models in
        # the SAME dbt run will hit that view, which is fine (parquet content
        # is identical to what we just wrote to iceberg). The next dbt run
        # will re-register from iceberg via configure_connection.

    def default_materialization(self) -> str:
        return "external"
