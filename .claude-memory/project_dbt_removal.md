---
name: dbt removal — replace with direct iceberg writes
description: Convert all 15 dbt models to Dagster Python assets. No duckdb file, no proprietary formats, no dbt.
type: project
---

## Status: WIP — silver_entries done, 15 models remaining (2026-04-12)

## Decision chain
1. Table extraction Phase 3 exposed that dbt couldn't see new bronze tables
   because views were hardcoded in a macro (create_bronze_views.sql)
2. Tried a dbt-duckdb plugin to register views dynamically from the catalog
3. Plugin caused SIGSEGV — DuckDB's iceberg extension isn't fork-safe across
   Dagster's multiprocess executor boundary
4. silver_entries was moved out of dbt as a Dagster asset — works, verified
   (733 rows, 7.6s, writes directly to iceberg)
5. User directive: no proprietary files ever (parquet or JSON only), no duckdb
   staging file, direct iceberg writes for everything
6. Decision: remove dbt entirely, all models become Dagster Python assets

## What works right now
- **silver_entries**: Dagster asset in `dlt/silver_tabletop/entries.py`, writes
  to iceberg via `write_iceberg()`. Verified end-to-end (733 rows).
- **Bronze pipeline**: all 16 bronze tables in iceberg, including the 3 new
  table-extraction tables (table_regions, table_cells, page_text_masks)
- **dbt pipeline**: still exists, still works IF the plugin is removed from
  profiles.yml. Currently reverted to the old duckdb-file path so it doesn't
  segfault. But the duckdb file violates the no-proprietary-files rule.

## What's half-done (DO NOT USE — written hastily, untested)
- `dlt/silver_tabletop/models.py` — 8 silver model functions, NOT TESTED
- `dlt/gold_tabletop/models.py` — 5 of 7 gold SQL model functions written,
  2 complex Python models (gold_chunks, gold_entry_index) NOT YET WRITTEN
- Neither file has been wired into Dagster assets
- Neither file has been run against real data

## Correct approach for next session
Convert ONE model at a time. For each model:
1. Read the dbt model file carefully
2. Write the equivalent function in `dlt/silver_tabletop/models.py` or
   `dlt/gold_tabletop/models.py`
3. Add a Dagster `@asset` in `dagster/lakehouse_assets/assets.py`
4. Register it in `Definitions(assets=[...])`
5. Add it to the relevant job selections
6. Run `python scripts/dagster.py validate` to verify parse
7. Run the pipeline with just that step (or a test job)
8. Verify the table lands in iceberg with correct row count
9. Commit
10. Move to the next model

Do NOT batch-write all models then try to run them. Do NOT use bash heredocs
to write Python files. Use Edit tool on the actual files.

## Conversion order (dependency-respecting)
Silver (no cross-silver deps except silver_entries which is done):
1. silver_toc_sections (needed by silver_tables, gold models)
2. silver_known_entries (needed by silver_spell_crosscheck)
3. silver_spell_crosscheck (needed by gold_entry_index)
4. silver_spell_meta (standalone)
5. silver_entry_descriptions (standalone)
6. silver_page_anchors (standalone)
7. silver_files (reads silver_entries)
8. silver_tables (reads silver_toc_sections)

Gold (depends on silver):
9. gold_toc (reads silver_toc_sections — trivial pass-through)
10. gold_tables (reads silver_tables — trivial pass-through)
11. gold_entries (reads silver_toc_sections + silver_entries)
12. gold_entry_index (reads silver_entries + silver_toc_sections + silver_spell_crosscheck + bronze authority — complex)
13. gold_chunks (reads silver_entries + silver_toc_sections — complex)
14. gold_entry_descriptions (reads silver_entry_descriptions + gold_entry_index)
15. gold_files (reads gold_chunks + silver_files + gold_toc)

Tests: 76 dbt tests. Convert AFTER all models work. Becomes a single Dagster
asset that runs check queries via get_reader() and writes results to
meta.test_results.

## Dagster asset graph (target state)
```
bronze_tabletop → toc_review → bronze_ocr_check
                → silver_entries
                     → silver_toc_sections
                          → silver_tables → gold_tables
                          → gold_toc
                          → gold_entries
                     → silver_known_entries → silver_spell_crosscheck
                                                → gold_entry_index
                                                     → gold_entry_descriptions
                     → silver_spell_meta
                     → silver_entry_descriptions
                     → silver_page_anchors
                     → silver_files → gold_files
                     → gold_chunks → gold_files
```

## Files to delete when done
- `dbt/` entire directory
- `dlt/publish_to_iceberg.py`
- `dlt/lib/dbt_iceberg_plugin.py` (the broken plugin)
- `db/duckdb/lakehouse.duckdb` (if it exists on disk)
- `dbt-core` and `dbt-duckdb` from `docker/requirements.txt`
- Remove `dbt_build`, `publish_to_iceberg`, `dbt_test` Dagster assets
- Update CLAUDE.md to remove all dbt references

## Rules for this work
- No proprietary files. Parquet or JSON only. No duckdb files.
- No batch-writing untested code. One model at a time, test, commit.
- No bash heredocs for writing Python. Use Edit tool.
- No plugin/adapter hacks. Direct `get_reader()` + `write_iceberg()`.
- Every model function returns row count. Every Dagster asset logs it.
- Explicit pyarrow schemas for any table with nullable columns.

**Why:** The dbt adapter (dbt-duckdb) writes to a proprietary duckdb file,
requires a publish step to copy to iceberg, and its iceberg plugin causes
SIGSEGV in Dagster's multiprocess executor. Direct iceberg writes via
write_iceberg() are simpler, faster, and proven (silver_entries works).

**How to apply:** Follow the conversion order above. Do not skip steps.
Do not batch. Test each model before moving to the next.
