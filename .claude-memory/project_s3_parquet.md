---
name: project_s3_parquet
description: Lakehouse storage must be S3 + Parquet, not DuckDB proprietary files — DuckDB is query engine only
type: project
---

The lakehouse is designed for S3 + Parquet storage, NOT DuckDB proprietary database files. DuckDB should only be used as a query engine (reading parquet from S3), never as the storage layer.

**Why:** Multiple books, multiple projects, open format requirement. DuckDB's single-process lock and proprietary format don't scale. Parquet on S3 is the standard lakehouse pattern — any tool can read it, supports partitioning, versioning, and concurrent access.

**How to apply:** All bronze/silver/gold data should be written as parquet files to S3 (or MinIO for local dev). DuckDB queries use `read_parquet('s3://...')`. Pipeline metadata (runs, catalog) may use DuckDB as a lightweight metastore. Need to determine: MinIO setup, bucket structure, partition strategy.
