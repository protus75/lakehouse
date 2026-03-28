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

### Unified cache volume
- **Host**: `cache/` → mounted as single volume `/workspace/cache` on daemon + workspace
- **Why single mount**: Windows Docker Desktop nested bind mounts are flaky — grpc subprocesses can't see individual subdirectory mounts. Single parent mount fixes this.
- Contains: `marker/` (OCR output), `huggingface/` (HF models), `datalab/` (Marker models)
- **Env var**: `HF_HOME=/workspace/cache/huggingface` on all containers
- **Workspace also mounts**: `cache/datalab:/root/.cache/datalab` separately (Marker hardcodes `~/.cache/datalab`)
- **Daemon has**: `TRANSFORMERS_OFFLINE=1`, `HF_HUB_OFFLINE=1` — never downloads

### dbt target/compiled
- **Host**: `dbt/lakehouse_mvp/target/`
- **When to clear**: after dbt model changes if seeing stale SQL
- **How**: `rm -rf dbt/lakehouse_mvp/target/`

## Pre-run checks (before every pipeline run)
1. Kill stale processes: check `docker top` for lingering python/marker processes
2. Check RAM: `docker stats --no-stream` — containers should be <2GB total
3. Reclaim WSL2 RAM: `wsl -d docker-desktop sh -c "echo 3 > /proc/sys/vm/drop_caches"`
4. Check network: `docker stats --no-stream` — NET I/O should be minimal
5. Monitor stderr for "download" within first 5s of any run — kill immediately if found
6. Verify GPU usage: `curl http://localhost:11434/api/ps` — check size_vram > 0 for loaded model
7. Check Ollama throughput: first chunk should complete in <15s for llama3:8b on GPU

## Bronze skip logic — CRITICAL
Bronze checks `files` table for existing source_file + config_hash. If matched, it SKIPS extraction entirely. Changing bronze code without changing config or using `--force` means the new code NEVER RUNS. Always use `--force` when bronze extraction code changes.

## Reset sequence after code changes (ALL STEPS REQUIRED)
1. Clear __pycache__: `find d:/source/lakehouse/lakehouse -name '__pycache__' -exec rm -rf {} +`
2. Restart BOTH Dagster containers: `docker restart lakehouse-dagster-daemon lakehouse-dagster-webserver`
   - ALWAYS restart both — each has its own grpc server with cached modules
   - Restarting only one causes stale code in the other
3. Wait 15s for grpc servers to start
4. Then launch pipeline
