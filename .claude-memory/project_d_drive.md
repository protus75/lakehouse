---
name: All data on D drive
description: All project data, models, caches, and storage must be on D drive, never C drive
type: project
---

All data storage for this project must be on D drive, not the OS drive (C:).

**Why:** The user's project and all persistent data lives on D drive. C drive is reserved for the OS only.

**How to apply:** When adding any new storage paths (model caches, databases, volumes, temp files), always use D drive paths. Check for anything defaulting to C drive (e.g. `~/.ollama`, `~/.cache`, Docker named volumes) and redirect to D drive. Key locations:
- Docker bind mounts: `D:\source\lakehouse\lakehouse\` subdirectories
- Ollama models: `D:\ollama\models` (via OLLAMA_MODELS env var)
- HuggingFace cache: bind-mounted to `../cache/huggingface`
- Datalab/Surya cache: bind-mounted to `../cache/datalab`
- DuckDB: bind-mounted to `../db/duckdb`
- PostgreSQL: bind-mounted to `../db/postgres`
