---
name: project_docker_image_split
description: Plan to split Docker images into lightweight daemon vs full workspace with GPU
type: project
---

## Problem
Single Dockerfile shared by all containers. Daemon downloads 530MB+ CUDA/PyTorch it never uses. Image rebuilds take 5-8 min and download GB of packages even for adding a 50KB utility.

## Plan: Three Dockerfiles

### 1. `Dockerfile.base` — shared foundation
- `python:3.11-slim` + apt packages (curl, git, build-essential)
- Shared lightweight packages: pyarrow, duckdb, pyyaml, requests, boto3, pyiceberg, polars
- Build as `lakehouse-base:latest`
- Rarely rebuilt

### 2. `Dockerfile.daemon` — orchestration only (FROM lakehouse-base)
- Dagster (dagster, dagster-webserver, dagster-postgres, dagster-dbt)
- dbt (dbt-core, dbt-duckdb)
- Lightweight tools: pyspellchecker, python-dotenv
- NO PyTorch, NO Marker, NO sentence-transformers, NO chromadb
- Used by: dagster-daemon, dagster-webserver
- Fast rebuild (~30s)

### 3. `Dockerfile.workspace` — full GPU stack (FROM lakehouse-base)
- Everything in base PLUS:
- PyTorch + CUDA (marker-pdf pulls these)
- marker-pdf, docling, pymupdf
- sentence-transformers, chromadb, langchain
- Streamlit, Jupyter, FastAPI
- Used by: workspace container (GPU-enabled)
- Slow rebuild but only when ML packages change

## Docker Compose Changes
```yaml
dagster-daemon:
  build:
    context: .
    dockerfile: Dockerfile.daemon

dagster-webserver:
  build:
    context: .
    dockerfile: Dockerfile.daemon

workspace:
  build:
    context: .
    dockerfile: Dockerfile.workspace
```

## Requirements Files
- `requirements-base.txt` — shared (pyarrow, duckdb, boto3, pyiceberg, polars, pyyaml, requests)
- `requirements-daemon.txt` — dagster, dbt, pyspellchecker
- `requirements-workspace.txt` — marker-pdf, torch, chromadb, sentence-transformers, streamlit, jupyter, langchain

## Migration Steps
1. Create the three Dockerfiles
2. Split requirements into three files
3. Update docker-compose.yml to reference correct Dockerfiles
4. Build base, then daemon, then workspace
5. Test: daemon starts without PyTorch, workspace has GPU access
6. Verify pipeline runs end-to-end via Dagster

## Rules
- Daemon NEVER imports marker, torch, transformers — fail fast if attempted
- Workspace is the only container with GPU reservation
- Adding a utility package to daemon = rebuild ~30s (not 8 min)
- Base image rebuild only when core data packages change

**Why:** Current single image wastes 5-8 min and 2GB+ bandwidth on every rebuild. Daemon doesn't need GPU packages.
**How to apply:** Implement before next pipeline work session.
