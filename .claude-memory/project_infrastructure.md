---
name: Infrastructure and architecture decisions
description: D drive requirement, S3+Parquet storage, architecture migration status, model seeding, Docker image split plan
type: project
---

## D drive requirement
All data, models, caches, storage on D drive — never C drive. Key locations:
- Docker bind mounts: `D:\source\lakehouse\lakehouse\` subdirectories
- Ollama models: `D:\ollama\models` (OLLAMA_MODELS env var)
- HuggingFace cache: `cache/huggingface`
- Datalab/Surya cache: `cache/datalab`
- DuckDB: `db/duckdb`
- PostgreSQL: `db/postgres`
- Claude Code temp: `D:\Claude\Temp` (CLAUDE_TMPDIR)

## Architecture migration (completed 2026-03-26)
DuckDB-as-storage → SeaweedFS (S3) + Apache Iceberg (PyIceberg SQL catalog on PostgreSQL).
- All data written as Iceberg tables to `s3://lakehouse/warehouse/`
- Writes via `write_iceberg()`, reads via `get_reader()` (DuckDB views over Iceberg)
- Polaris (Java) removed → PyIceberg SQL talks directly to PostgreSQL
- Dagster added for orchestration
- dbt still materializes to DuckDB, post-dbt publish writes to Iceberg

## Model seeding
No pipeline should download models. Dedicated seed pipeline only. All others run offline, fail if models missing.
- Daemon has `TRANSFORMERS_OFFLINE=1`, `HF_HUB_OFFLINE=1`
- Need to create `seed_models` Dagster asset/job (only one allowed online)

## Docker image split plan
Single Dockerfile → three (base/daemon/workspace) to fix 5-8 min rebuilds.
- **Dockerfile.base**: python:3.11-slim + shared packages (pyarrow, duckdb, boto3, pyiceberg, polars)
- **Dockerfile.daemon**: FROM base + dagster, dbt, pyspellchecker. NO PyTorch/Marker. ~30s rebuild.
- **Dockerfile.workspace**: FROM base + PyTorch, CUDA, marker-pdf, sentence-transformers, chromadb, streamlit. GPU-enabled.
- Split requirements into three files
- Update docker-compose.yml for separate build contexts
- Daemon NEVER imports marker/torch/transformers
