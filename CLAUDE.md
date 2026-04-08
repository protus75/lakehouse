# Lakehouse Project — Claude Instructions

## Memory
- **Read `.claude-memory/MEMORY.md` at the start of every conversation.** It indexes all persistent memory files. Read every file it references before doing any work.
- **Memory lives in `.claude-memory/` in the project repo.** NEVER write memory files to `C:\Users\...\` or the system default path. Always use `.claude-memory/` relative to project root.

## Rules — MUST FOLLOW

### No hardcoded values — CRITICAL
NEVER hardcode thresholds, field names, patterns, pixel values, magic numbers, or string literals in code. Everything goes in YAML config (`config/lakehouse.yaml` or per-book configs). Write YAML config FIRST, then code that reads it.

### No regex for content parsing — CRITICAL
NEVER use regex for content parsing. Use string operations (split, startswith, find, `in`), ML-detected headings, or LLM for complex classification. Only acceptable for truly atomic patterns (extracting a number, fixed format).

### Dagster only — CRITICAL
NEVER run pipeline steps manually (`docker exec python -m dlt.*`, `dbt build`, `ollama pull`). Always use Dagster jobs/assets via http://localhost:3000. Only exception: small diagnostic/sample queries for debugging.

### Cache reset before EVERY pipeline run — CRITICAL
ALWAYS clear caches and restart Dagster before launching ANY pipeline. No exceptions, even if "only config changed." Steps:
1. Clear host pycache: `find d:/source/lakehouse/lakehouse -name '__pycache__' -exec rm -rf {} +`
2. Clear container pycache (volume mount doesn't always sync): `docker exec lakehouse-dagster-daemon bash -c 'find /workspace -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null'`
3. Restart BOTH Dagster containers: `docker restart lakehouse-dagster-daemon lakehouse-dagster-webserver`
4. Wait 15s for grpc servers
5. THEN launch pipeline
Skip this = stale code = wasted pipeline run = wasted user time.

### Zero validation errors — CRITICAL
ZERO errors before new features. Never say "good enough." Never commit with known bad data. Never set test severity to `warn` to suppress failures.

### Never discard data during extraction — CRITICAL
Capture EVERY field from ANY source. Store raw in bronze with ALL columns. Never reduce or simplify during extraction — that's for silver/gold.

### Always estimate and monitor task duration
For ANY task expected to take >15 seconds: give a time estimate up front before starting. For ANY command >1 minute: run in background, poll every 30s, compare output between polls, check disk I/O. If no output change after 60s and disk/network idle — process is hung, cancel immediately. NEVER go silent waiting on a long task. Docker processes crash silently — monitor all of them.

### No downloads without permission
Before pipeline runs: verify model cache volumes mounted. Monitor stderr for "Downloading" in first 5 seconds — kill immediately if found. Metered network.

### Never manually wipe data or catalog — CRITICAL
NEVER delete data files or drop catalog entries manually. The pipeline handles all cleanup via `write_iceberg(overwrite_all=True)`. If data appears corrupted, re-run the pipeline — it will drop catalog, wipe the table directory, and recreate. Manual wipes break the catalog→data link and require manual catalog cleanup to fix.

### No pip install in containers
NEVER `pip install` in a running container — it's lost on restart. Add to `docker/requirements.txt` and rebuild the image.

### Shell commands
- Use PowerShell syntax (`;` not `&&`) in instructions to the user
- No inline comments in copyable commands
- Always include `cd` or absolute paths — never assume directory
- Bash tool: run commands directly without `cd /path &&` prefix
- Bash tool: use relative paths matching permission rules (e.g. `python scripts/dagster.py`, NOT `python d:/source/.../scripts/dagster.py`)
- No `jq` — use `python -c "import json..."` instead

### Small failure counts → manual review, not code changes
When a test has only 1-3 failures, don't try to write a perfect detection rule. Dump the specific failing entries for the user to review, then add config overrides. Offer the data, not the fix.

### Finish before moving on
Complete the current task fully — run full validation, review results, fix issues. Don't present menus of options after partial results.

## Infrastructure

### Docker
- Workspace container: `lakehouse-workspace` (Python 3.11, GPU-enabled)
- Dagster: `lakehouse-dagster-webserver` (port 3000), `lakehouse-dagster-daemon`
- Compose file: `docker/docker-compose.yml`
- All code mounted from host via volumes (edits on host appear in container)

### Storage: Local Filesystem
- Host path: `F:/lakehouse/data` → container mount: `/lakehouse-data`
- All Iceberg data at `/lakehouse-data/<namespace>/<table>/`
- Volume mounted in all three Docker services (webserver, daemon, workspace)

### Catalog: PyIceberg SQL (PostgreSQL)
- PostgreSQL: `lakehouse-postgres` (port 5432, user=`iceberg`, db=`iceberg`)
- PyIceberg connects directly to PostgreSQL — no Java catalog server
- All catalog operations via `dlt/lib/iceberg_catalog.py`
- Config: `config/lakehouse.yaml`

### DuckDB (query engine only)
- Used in-memory for reads via `dlt/lib/duckdb_reader.py`
- `dbt` still materializes to `db/duckdb/lakehouse.duckdb` during builds
- `get_reader()` creates DuckDB views over Iceberg tables on local filesystem
- NEVER write data to DuckDB — all writes go through `write_iceberg()`

### Ollama (LLM)
- Runs on Windows host, NOT in Docker
- Models stored at: `D:\ollama\models` (env var `OLLAMA_MODELS`)
- API: `http://localhost:11434` (host) / `http://host.docker.internal:11434` (container)
- Models: `qwen3:30b-a3b` (summaries), `llama3:70b` (annotations), `minicpm-v:latest` (vision)

### Marker (PDF OCR)
- Installed in workspace container, uses GPU
- Cached markdown: `cache/marker/<filename>.md`

## Pipeline (Tabletop Rules)

```
PDF → Bronze (dlt→Iceberg, ~15s) → Silver+Gold (dbt, ~5s) → Publish (Iceberg) → AI Enrichment (Ollama, ~70min)
```

### Dagster orchestration (ALL pipeline runs go through Dagster)
- UI: http://localhost:3000
- Jobs: `tabletop_full_pipeline`, `tabletop_without_enrichment`
- Assets: `bronze_tabletop → dbt_tabletop → publish_to_iceberg → gold_ai_summaries / gold_ai_annotations`
- Asset definitions: `dagster/lakehouse_assets/assets.py`

### Key paths
- Bronze pipeline: `dlt/bronze_tabletop_rules.py`
- Iceberg library: `dlt/lib/iceberg_catalog.py` (write_iceberg, read_iceberg)
- DuckDB reader: `dlt/lib/duckdb_reader.py` (get_reader)
- Shared library: `dlt/lib/tabletop_cleanup.py`
- Publish script: `dlt/publish_to_iceberg.py`
- Lakehouse config: `config/lakehouse.yaml`
- Per-book configs: `documents/tabletop_rules/configs/`
- Silver models: `dbt/lakehouse_mvp/models/tabletop/silver/`
- Gold models: `dbt/lakehouse_mvp/models/tabletop/gold/`
- Bronze views macro: `dbt/lakehouse_mvp/macros/create_bronze_views.sql`

### Config-driven approach
- All thresholds, patterns, and corrections go in YAML configs — never hardcode
- Lakehouse infra config: `config/lakehouse.yaml` (catalog, namespaces)
- OCR corrections: `content_substitutions` in per-book config
- Authority tables: define which tables provide ground-truth entry names
- Entry anchors: for entries Marker doesn't render as headings

### Architecture rules
- **No one-off scripts.** All functionality belongs in a lakehouse layer:
  - Bronze (`dlt/`): ingestion, extraction, raw validation
  - Silver/Gold (`dbt/`): transforms, enrichment, quality checks
- All data stored as Iceberg tables on local filesystem (`bronze_tabletop.*`, `silver_tabletop.*`, `gold_tabletop.*`)
- Writes: always via `write_iceberg()` from `dlt/lib/iceberg_catalog.py`
- Reads: always via `get_reader()` from `dlt/lib/duckdb_reader.py`
- Validation steps (OCR check, page number validation, etc.) are bronze functions that store results in bronze Iceberg tables

### Current focus: Player's Handbook only
- Process one book until validation passes before moving to others
- 41/41 dbt tests passing
