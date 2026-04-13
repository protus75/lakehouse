---
name: dbt removal — replace with direct iceberg writes
description: All 15 dbt models converted to Dagster Python assets. dbt_build/publish_to_iceberg/dbt_test removed from asset graph. Cleanup remaining.
type: project
---

## Status: All models converted, cleanup pending (2026-04-13)

## What's done
- **All 8 silver models** converted and verified as Dagster Python assets
  - silver_entries (733 rows), silver_toc_sections (320), silver_known_entries (505)
  - silver_spell_crosscheck (503), silver_spell_meta (485), silver_entry_descriptions (732)
  - silver_page_anchors (263), silver_files (1), silver_tables (1115)
- **All 7 gold models** converted and verified as Dagster Python assets
  - gold_toc (320), gold_tables (1115), gold_entries (692)
  - gold_entry_index (733), gold_chunks (2117), gold_entry_descriptions (732), gold_files (1)
- Entry types verified: 485 spells, 155 rules, 69 tables, 15 class, 9 proficiency
- **dbt_build, publish_to_iceberg, dbt_test** assets removed from Dagster graph
- All job selections updated (tabletop_full_pipeline, tabletop_without_enrichment, silver_and_publish)
- scripts/dagster.py validate updated (imports gold_entry_index instead of dbt_build)

## Key fix during conversion
- Pandas NA vs Python None: all models now use `pd.notna()` and `_val()` helper
  instead of `is not None` checks (which don't catch pandas NA/NaN)

## Cleanup remaining — do in next session

### Files/directories to delete
- `dbt/` — entire directory (models, macros, profiles, dbt_project.yml)
- `dlt/publish_to_iceberg.py` — no longer needed
- `dlt/lib/dbt_iceberg_plugin.py` — the broken fork-unsafe plugin
- `db/duckdb/lakehouse.duckdb` — proprietary intermediate file (if on disk)

### Docker/pip cleanup
- Remove from `docker/requirements.txt`: `dbt-core`, `dbt-duckdb`, `dagster-dbt`
- Rebuild Docker image after requirements change
- Remove any dbt volume mounts from `docker/docker-compose.yml`

### Script cleanup
- `scripts/dagster.py`: remove dbt-logs, dbt-results, dbt-clean subcommands
- `scripts/dagster.py`: remove dbt references in state/watch output
- `scripts/dagster.py`: remove meta.dbt_test_results/dbt_test_failures from verify

### Config/docs cleanup
- `CLAUDE.md` — remove dbt references (key paths, pipeline description, dbt test counts)
- `config/lakehouse.yaml` — remove dbt config if present
- `.claude-memory/MEMORY.md` — update entries that reference dbt

### Verification after full removal
- `grep -ri "dbt" --include="*.py" --include="*.yml" --include="*.yaml"` in working dirs returns zero hits
- `docker exec lakehouse-dagster-daemon pip list | grep dbt` returns nothing
- All Dagster assets parse: `python scripts/dagster.py validate`
- Full pipeline runs end-to-end without dbt

## Rules for this work
- No proprietary files. Parquet or JSON only. No duckdb files.
- No batch-writing untested code. One model at a time, test, commit.
- No bash heredocs for writing Python. Use Edit tool.
- No plugin/adapter hacks. Direct `get_reader()` + `write_iceberg()`.
- Every model function returns row count. Every Dagster asset logs it.
- Explicit pyarrow schemas for any table with nullable columns.
