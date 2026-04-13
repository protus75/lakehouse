# OpenTelemetry — Detail Plan

## Code Changes

### Remove _log() pattern
- `dlt/lib/tabletop_cleanup.py` — delete `_log()` function
- Every file that imports `_log` switches to `logger = logging.getLogger(__name__)`

### Replace all print()/_log() call sites

| File | Current | Change to |
|------|---------|-----------|
| `dlt/lib/tabletop_cleanup.py` | `_log()` ~25 calls | `logger.info/debug/warning()` |
| `dlt/bronze_tabletop_rules.py` | `_log()` ~80 calls | `logger.info/debug/warning()` |
| `dlt/publish_to_iceberg.py` | `_log()` ~3 calls | `logger.info()` |
| `dlt/silver_tabletop/entries.py` | `print()` ~7 calls | `logger.info()` |
| `dlt/lib/iceberg_catalog.py` | silent | `logger.info()` on write/read, `logger.error()` on failures |
| `dlt/lib/duckdb_reader.py` | silent | `logger.info()` on reader creation |
| `dagster/lakehouse_assets/assets.py` | `context.log` ~34 calls | keep `context.log` (Dagster UI), add `logger` for structured stdout |
| `scripts/tabletop_rules/enrich_summaries.py` | `_log()` ~9 calls | `logger.info/warning()` |
| `scripts/tabletop_rules/enrich_annotations.py` | `_log()` ~8 calls | `logger.info/warning()` |

### Log levels guide
- `DEBUG` — detailed data (row counts, field values, per-entry progress)
- `INFO` — pipeline milestones (started X, wrote N rows, finished Y)
- `WARNING` — recoverable issues (missing field, skipped entry, fallback used)
- `ERROR` — failures that stop processing

### New module: dlt/lib/telemetry.py
- Read `telemetry` section from `config/lakehouse.yaml`
- Configure Python stdlib `logging` root logger with format from config
- Set log level from config
- `init_logging()` called once at startup — idempotent
- OTel API as the logging interface (backend-ready if ever needed, config change not code change)
- Auto-instrument: requests, psycopg2, fastapi, flask, chromadb, sqlalchemy

### Config addition (config/lakehouse.yaml)
```yaml
telemetry:
  log_level: INFO
  log_format: "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
```

### Init points
Call `init_logging()` at:
- Top of `dagster/lakehouse_assets/assets.py` (module load)
- Top of any standalone script entry point (enrich_summaries.py, etc.)
- Top of RAG API (rag/api.py)
- Top of Dash app (dashapp/tabletop_browser.py)

### Dagster assets
- Keep existing `context.log.*()` calls — they feed the Dagster UI
- Add `logger = logging.getLogger(__name__)` for structured stdout
- No wrapping, no spans, no decorators

## Packages to add (docker/requirements-base.txt)
```
opentelemetry-api
opentelemetry-sdk
opentelemetry-instrumentation-requests
opentelemetry-instrumentation-psycopg2
opentelemetry-instrumentation-sqlalchemy
```

## Packages to add (docker/requirements-workspace.txt)
```
opentelemetry-instrumentation-fastapi
opentelemetry-instrumentation-flask
opentelemetry-instrumentation-chromadb
```

## No OTel packages exist for
DuckDB, PyArrow, PyIceberg, Streamlit, PyMuPDF/Marker, dlt, dbt-core (dbt)

## dlt / dbt OTel integration — TBD
Needs research. Separate task.

## Implementation order
1. Add packages to requirements files
2. Add `telemetry` config to lakehouse.yaml
3. Create `dlt/lib/telemetry.py` with `init_logging()`
4. Replace `_log()`/`print()` in `dlt/lib/tabletop_cleanup.py` (kills _log, sets pattern)
5. Replace in `dlt/bronze_tabletop_rules.py` (~80 calls)
6. Replace in remaining dlt/ files (publish, silver, iceberg_catalog, duckdb_reader)
7. Add logger to `dagster/lakehouse_assets/assets.py` alongside context.log
8. Replace in enrichment scripts
9. Add init to RAG/Dash entry points
10. Rebuild Docker image

## Verification
- `docker logs lakehouse-dagster-daemon 2>&1 | grep "INFO"` — timestamped, module-tagged lines
- `docker logs lakehouse-dagster-daemon 2>&1 | grep "iceberg_catalog"` — write/read ops visible
- Run pipeline via Dagster, confirm structured output
- Set `log_level: DEBUG`, re-run, confirm verbose output
