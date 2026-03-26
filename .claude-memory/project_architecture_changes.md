---
name: project_architecture_changes
description: Storage migration completed 2026-03-26 - SeaweedFS+Iceberg+Dagster replaces DuckDB storage
type: project
---

Architecture migration completed 2026-03-26:

**Storage layer:** DuckDB-as-storage → SeaweedFS (S3) + Apache Iceberg (PyIceberg SQL catalog on PostgreSQL)
- All bronze/silver/gold data written as Iceberg tables to `s3://lakehouse/warehouse/`
- Writes via `dlt/lib/iceberg_catalog.py` (`write_iceberg()`)
- Reads via `dlt/lib/duckdb_reader.py` (`get_reader()` — DuckDB views over Iceberg)
- Config in `config/lakehouse.yaml`

**Catalog:** Polaris (Java) removed → PyIceberg SQL catalog talks directly to PostgreSQL
**Orchestration:** Dagster added (webserver port 3000, daemon), assets defined in `dagster/lakehouse_assets/assets.py`
**dbt:** Still materializes to DuckDB, but reads bronze via Iceberg views (`on-run-start` macro). Post-dbt publish step writes silver/gold to Iceberg.

**Why:** DuckDB single-process lock doesn't scale. Parquet on S3 is open format. PyIceberg is pure Python. Dagster's asset model maps to medallion layers.
