---
name: Infrastructure and architecture decisions
description: D/F drive layout, local filesystem Iceberg storage, architecture migration status, model seeding, Docker image split plan
type: project
---

## Drive layout
Data, models, caches on D and F drives — never C drive. Key locations:
- Docker bind mounts: `D:\source\lakehouse\lakehouse\` subdirectories
- Iceberg warehouse data: `F:\lakehouse\data` → `/lakehouse-data` in containers
- Ollama models: `D:\ollama\models` (OLLAMA_MODELS env var)
- HuggingFace cache: `cache/huggingface`
- Datalab/Surya cache: `cache/datalab`
- DuckDB: `db/duckdb`
- PostgreSQL: `db/postgres`
- Claude Code temp: `D:\Claude\Temp` (CLAUDE_TMPDIR)

## Architecture migration history
1. DuckDB-as-storage → SeaweedFS (S3) + Iceberg (2026-03-26)
2. SeaweedFS removed → local filesystem storage (2026-04-07)
- All data written as Iceberg tables to `/lakehouse-data/<namespace>/<table>/`
- Writes via `write_iceberg()`, reads via `get_reader()` (DuckDB views over Iceberg)
- PyIceberg SQL catalog talks directly to PostgreSQL
- Dagster for orchestration
- dbt still materializes to DuckDB, post-dbt publish writes to Iceberg
- Cloud S3 migration steps documented in `ai/python_lakehouse_architecture.md`

## Model seeding (done 2026-03-27)
`seed_models` Dagster job validates all model dependencies. Daemon has `TRANSFORMERS_OFFLINE=1`, `HF_HUB_OFFLINE=1`.
- `seed_ollama_models`: pulls llama3:70b, llama3:8b, minicpm-v from host Ollama API
- `seed_huggingface_models`: validates HF cache (all-MiniLM-L6-v2)
- `seed_marker_cache`: validates Marker PDF cache exists
- Model list in `config/lakehouse.yaml` under `models:`

## Docker images (done 2026-03-27)
Two images: base + workspace. All three services use workspace image.
- **Dockerfile.base**: python:3.11-slim + shared packages (pyarrow, duckdb, pyiceberg, polars, pyspellchecker)
- **Dockerfile.workspace**: FROM base + dagster, dbt, PyTorch, CUDA, marker-pdf, sentence-transformers, chromadb, streamlit
- Webserver, daemon, workspace all use `lakehouse-workspace:latest` with different commands
- Three-way split was tried and reverted — daemon couldn't import pymupdf/marker needed by bronze assets

## NVIDIA driver (2026-03-27)
Driver 595.97, CUDA 13.2, RTX 4090. PyTorch 2.11+cu130 working.
