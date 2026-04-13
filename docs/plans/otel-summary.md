# OpenTelemetry Plan — Executive Summary

## What
Replace all `_log()`/`print()` with proper structured logging via OpenTelemetry API. Auto-instrument the full stack.

## Packages

**Core:**
- `opentelemetry-api`, `opentelemetry-sdk`

**Auto-instrumentation (existing stack):**
- `opentelemetry-instrumentation-requests` — Ollama HTTP calls
- `opentelemetry-instrumentation-psycopg2` — PostgreSQL/catalog ops
- `opentelemetry-instrumentation-fastapi` — RAG API
- `opentelemetry-instrumentation-flask` — Dash browser app
- `opentelemetry-instrumentation-chromadb` — RAG embeddings
- `opentelemetry-instrumentation-sqlalchemy` — Dagster run storage, PyIceberg catalog

**No OTel packages exist for:** DuckDB, PyArrow, PyIceberg, Streamlit, PyMuPDF, dlt, dbt (dbt-core)

## Config
- `config/lakehouse.yaml` — log level, format
- New module `dlt/lib/telemetry.py` — one-time init, used everywhere

## Output
- Structured logs to stdout (Docker captures them)
- No new containers, no backends, no collectors

## Phases
1. Infrastructure — packages, config, telemetry.py
2. Core libs — replace `_log()`/`print()` with `logging.getLogger(__name__)`
3. Auto-instrumentation — enable all the packages above
4. dlt/dbt — integrate native OTel support (pending research)
5. Dagster assets — add loggers alongside `context.log`
6. Enrichment scripts + RAG

## Open item
dlt and dbt OTel integration needs research — see [otel-detail.md](docs/plans/otel-detail.md).
