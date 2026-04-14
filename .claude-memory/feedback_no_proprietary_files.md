---
name: No proprietary file formats ever
description: Only parquet or JSON for data files. No duckdb files, no pickle, no proprietary formats.
type: feedback
---

No proprietary file formats for data storage. Parquet or JSON only. Ever.

Specifically banned:
- `.duckdb` files (the current `lakehouse.duckdb` staging file violates this)
- Pickle files (Dagster's PickledObjectFilesystemIOManager also violates this)
- Any vendor-locked binary format

DuckDB is fine as an in-memory query engine. It is NOT fine as a storage format.

**Why:** Proprietary files create vendor lock-in, can't be read by other tools, and add an unnecessary intermediary between the pipeline and the canonical store (iceberg, which uses parquet internally).

**How to apply:** Any pipeline step that needs to persist data writes to iceberg via `write_iceberg()` or to parquet/JSON files on disk.
