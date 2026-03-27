---
name: reference_cache_locations
description: All cache locations that cause stale code/model issues — must clear on code changes
type: reference
---

## Caches that cause problems

### Python bytecode (__pycache__)
- **Host**: `dlt/__pycache__/`, `dlt/lib/__pycache__/`, `dagster/lakehouse_assets/__pycache__/`
- **Containers**: same paths under `/workspace/`
- **When to clear**: after ANY code change to .py files
- **How**: `find d:/source/lakehouse/lakehouse -name '__pycache__' -exec rm -rf {} +`
- **Why**: Docker volume mounts + Windows mtime can cause .pyc to appear newer than source, so Python uses stale bytecode

### Dagster grpc server module cache
- **Where**: in-memory in the `dagster api grpc` subprocess inside daemon/webserver
- **When to clear**: after code changes to assets.py or any imported module
- **How**: `docker restart lakehouse-dagster-daemon lakehouse-dagster-webserver`
- **Why**: grpc server loads modules once and caches them; restart forces reimport
- **IMPORTANT**: clear __pycache__ BEFORE restarting, or the grpc server will reimport stale .pyc

### Marker model cache (datalab)
- **Host**: `cache/datalab/` → mounted at `/root/.cache/datalab` in containers
- **Containers**: daemon + workspace both need this mount
- **When to clear**: never (1.34GB download), only if models are corrupt
- **Env var**: daemon needs no download env vars: `TRANSFORMERS_OFFLINE=1`, `HF_HUB_OFFLINE=1`

### HuggingFace model cache
- **Host**: `cache/huggingface/` → mounted at `/workspace/.cache/huggingface`
- **Env var**: `HF_HOME=/workspace/.cache/huggingface` required on ALL containers that use HF

### Marker OCR output cache
- **Host**: `cache/marker/` → mounted at `/workspace/cache/marker`
- **When to clear**: only when re-OCR is needed (e.g. new Marker version)

### dbt target/compiled
- **Host**: `dbt/lakehouse_mvp/target/`
- **When to clear**: after dbt model changes if seeing stale SQL
- **How**: `rm -rf dbt/lakehouse_mvp/target/`

## Reset sequence after code changes
1. Clear __pycache__: `find d:/source/lakehouse/lakehouse -name '__pycache__' -exec rm -rf {} +`
2. Restart Dagster: `docker restart lakehouse-dagster-daemon lakehouse-dagster-webserver`
3. Wait 10s for grpc servers to start
4. Then launch pipeline
