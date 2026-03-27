# Lakehouse Project — Claude Instructions

## Memory
- **Read `.claude-memory/MEMORY.md` at the start of every conversation.** It indexes all persistent memory files. Read every file it references before doing any work.

## Infrastructure

### Docker
- Workspace container: `lakehouse-workspace` (Python 3.11, GPU-enabled)
- Dagster: `lakehouse-dagster-webserver` (port 3000), `lakehouse-dagster-daemon`
- Compose file: `docker/docker-compose.yml`
- All code mounted from host via volumes (edits on host appear in container)

### Storage: SeaweedFS (S3-compatible)
- S3 gateway: port 8333 (container: `seaweedfs-s3:8333`)
- Bucket: `lakehouse` — all Iceberg data at `s3://lakehouse/warehouse/`
- Auth: `lakehouse_key` / `lakehouse_secret` (configured in `docker/s3.json`)

### Catalog: PyIceberg SQL (PostgreSQL)
- PostgreSQL: `lakehouse-postgres` (port 5432, user=`iceberg`, db=`iceberg`)
- PyIceberg connects directly to PostgreSQL — no Java catalog server
- All catalog operations via `dlt/lib/iceberg_catalog.py`
- Config: `config/lakehouse.yaml`

### DuckDB (query engine only)
- Used in-memory for reads via `dlt/lib/duckdb_reader.py`
- `dbt` still materializes to `db/duckdb/lakehouse.duckdb` during builds
- `get_reader()` creates DuckDB views over Iceberg tables on S3
- NEVER write data to DuckDB — all writes go through `write_iceberg()`

### Ollama (LLM)
- Runs on Windows host, NOT in Docker
- Models stored at: `D:\ollama\models` (env var `OLLAMA_MODELS`)
- API: `http://localhost:11434` (host) / `http://host.docker.internal:11434` (container)
- Models: `llama3:70b` (enrichment), `minicpm-v:latest` (vision)

### Marker (PDF OCR)
- Installed in workspace container, uses GPU
- Cached markdown: `cache/marker/<filename>.md`

## Pipeline (Tabletop Rules)

```
PDF → Bronze (dlt→Iceberg, ~15s) → Silver+Gold (dbt, ~5s) → Publish (Iceberg) → AI Enrichment (Ollama, ~70min)
```

### Run commands (manual)
```bash
docker exec lakehouse-workspace python -u -m dlt.bronze_tabletop_rules
docker exec lakehouse-workspace bash -c "cd /workspace/dbt/lakehouse_mvp && dbt build --select tabletop"
docker exec lakehouse-workspace python -u dlt/publish_to_iceberg.py
docker exec lakehouse-workspace python -u scripts/tabletop_rules/enrich_summaries.py
docker exec lakehouse-workspace python -u scripts/tabletop_rules/enrich_annotations.py
```

### Dagster orchestration
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
- Lakehouse infra config: `config/lakehouse.yaml` (S3, catalog, namespaces)
- OCR corrections: `content_substitutions` in per-book config
- Authority tables: define which tables provide ground-truth entry names
- Entry anchors: for entries Marker doesn't render as headings

### Architecture rules
- **No one-off scripts.** All functionality belongs in a lakehouse layer:
  - Bronze (`dlt/`): ingestion, extraction, raw validation
  - Silver/Gold (`dbt/`): transforms, enrichment, quality checks
- All data stored as Iceberg tables on S3 (`bronze_tabletop.*`, `silver_tabletop.*`, `gold_tabletop.*`)
- Writes: always via `write_iceberg()` from `dlt/lib/iceberg_catalog.py`
- Reads: always via `get_reader()` from `dlt/lib/duckdb_reader.py`
- Validation steps (OCR check, page number validation, etc.) are bronze functions that store results in bronze Iceberg tables

### Current focus: Player's Handbook only
- Process one book until validation passes before moving to others
- 41/41 dbt tests passing
