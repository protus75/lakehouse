# Lakehouse

An open-source, Python-centric lakehouse stack with AI enrichment. Built on Iceberg + S3 + DuckDB + dbt + Dagster.

## Stack

| Component | Role | Location |
|-----------|------|----------|
| **SeaweedFS** | S3-compatible object storage | `s3://lakehouse/warehouse/` (port 8333) |
| **PyIceberg** | Table format + catalog (PostgreSQL-backed) | `dlt/lib/iceberg_catalog.py` |
| **DuckDB** | Query engine (in-memory, read-only) | `dlt/lib/duckdb_reader.py` |
| **dlt** | Bronze ingestion pipelines | `dlt/` |
| **dbt** | Silver/gold transforms + tests | `dbt/lakehouse_mvp/` |
| **Dagster** | Orchestration | http://localhost:3000 |
| **Ollama** | LLM enrichment (host GPU) | `qwen3:30b-a3b`, `llama3:70b` |
| **Dash** | Browser UI ([gamerules.ai](https://gamerules.ai)) | `dashapp/tabletop_browser.py` (port 8000) |

## Projects

### Tabletop Rules
PDF extraction and AI-enriched search for tabletop RPG rules (D&D 2e Player's Handbook).

- **Docs:** [`documents/tabletop_rules/README.md`](documents/tabletop_rules/README.md)
- **Pipeline:** `PDF → Bronze → Silver → Gold → Publish → AI Enrichment`
- **Browser:** Full scrollable book with ToC navigation, AI summaries, metadata badges

## Infrastructure

All services run in Docker (`docker/docker-compose.yml`):
- `lakehouse-workspace` — Python 3.11, GPU-enabled
- `lakehouse-dagster-webserver` / `lakehouse-dagster-daemon` — orchestration
- `seaweedfs-*` — S3 storage
- `lakehouse-postgres` — Iceberg catalog

Code is volume-mounted from host — edits appear in containers immediately.

## Documentation

- **[CLAUDE.md](CLAUDE.md)** — Development instructions and architecture rules
- **[Tabletop Rules](documents/tabletop_rules/README.md)** — Pipeline details, models, browser
- **[Architecture](ai/python_lakehouse_architecture.md)** — High-level design
