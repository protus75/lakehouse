---
name: project_architecture_changes
description: Architecture changes from original spec - Unity Catalog replaces Polaris, Dagster replaces Airflow
type: project
---

User-directed architecture changes from original `ai/python_lakehouse_architecture.md` spec (2026-03-26):

**Changed components:**
- **PyIceberg SQL catalog** (PostgreSQL-backed) replaces Apache Polaris — no Java catalog server needed, pure Python
- **Dagster** replaces Apache Airflow — better Python-native orchestration, asset-based model fits batch analytics

**Unchanged from spec:**
- SeaweedFS (S3-compatible object storage)
- Apache Iceberg (table format)
- DuckDB (query engine only)
- dlt (ingestion)
- dbt (transformation)
- Parquet files on S3

**Why:** Polaris is Java-heavy with limited Python tooling. PyIceberg SQL catalog is pure Python, uses existing PostgreSQL, no extra server. Can upgrade to Unity Catalog later if governance needed. Dagster's asset-centric model maps directly to medallion layers.

**How to apply:** Remove Polaris containers from docker-compose, configure PyIceberg with SQL catalog on existing PostgreSQL, add Dagster, write Iceberg tables to SeaweedFS via PyIceberg, DuckDB reads via Iceberg REST or direct parquet.
