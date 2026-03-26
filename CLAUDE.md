# Lakehouse Project — Claude Instructions

## Infrastructure

### Docker
- Workspace container: `lakehouse-workspace` (Python 3.11, GPU-enabled)
- Compose file: `docker/docker-compose.yml`
- All code mounted from host via volumes (edits on host appear in container)

### Ollama (LLM)
- Runs on Windows host, NOT in Docker
- Models stored at: `D:\ollama\models` (env var `OLLAMA_MODELS`)
- API: `http://localhost:11434` (host) / `http://host.docker.internal:11434` (container)
- Models: `llama3:70b` (enrichment), `minicpm-v:latest` (vision)

### DuckDB
- Path: `db/duckdb/lakehouse.duckdb` (host) / `/workspace/db/lakehouse.duckdb` (container)

### Marker (PDF OCR)
- Installed in workspace container, uses GPU
- Cached markdown: `cache/marker/<filename>.md`

## Pipeline (Tabletop Rules)

```
PDF → Bronze (dlt, ~15s) → Silver+Gold (dbt, ~5s) → AI Enrichment (Ollama, ~70min)
```

### Run commands
```bash
docker exec lakehouse-workspace python -u -m dlt.bronze_tabletop_rules
docker exec lakehouse-workspace bash -c "cd /workspace/dbt/lakehouse_mvp && dbt build --select tabletop"
docker exec lakehouse-workspace python -u scripts/tabletop_rules/enrich_summaries.py
docker exec lakehouse-workspace python -u scripts/tabletop_rules/enrich_annotations.py
```

### Key paths
- Bronze pipeline: `dlt/bronze_tabletop_rules.py`
- Shared library: `dlt/lib/tabletop_cleanup.py`
- Per-book configs: `documents/tabletop_rules/configs/`
- Silver models: `dbt/lakehouse_mvp/models/tabletop/silver/`
- Gold models: `dbt/lakehouse_mvp/models/tabletop/gold/`

### Config-driven approach
- All thresholds, patterns, and corrections go in YAML configs — never hardcode
- OCR corrections: `content_substitutions` in per-book config
- Authority tables: define which tables provide ground-truth entry names
- Entry anchors: for entries Marker doesn't render as headings

### Architecture rules
- **No one-off scripts.** All functionality belongs in a lakehouse layer:
  - Bronze (`dlt/`): ingestion, extraction, raw validation
  - Silver/Gold (`dbt/`): transforms, enrichment, quality checks
- Results always stored in proper tables (`bronze_tabletop.*`, `silver_tabletop.*`, `gold_tabletop.*`)
- Validation steps (OCR check, page number validation, etc.) are bronze functions that store results in bronze tables

### Current focus: Player's Handbook only
- Process one book until validation passes before moving to others
- 41/41 dbt tests passing
