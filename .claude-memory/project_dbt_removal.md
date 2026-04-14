---
name: dbt removal — COMPLETE
description: dbt fully removed. All 15 models are Dagster Python assets writing directly to Iceberg. No dbt, no duckdb file, no publish step.
type: project
---

## Status: COMPLETE (2026-04-13)

dbt has been fully removed from the lakehouse pipeline. All silver and gold
models run as Dagster Python assets using get_reader() + write_iceberg().

## Key fix during conversion
- Pandas NA vs Python None: all models use `pd.notna()` and `_val()` helper
  instead of `is not None` checks (which don't catch pandas NA/NaN)

**Why:** The dbt adapter (dbt-duckdb) wrote to a proprietary duckdb file,
required a publish step to copy to iceberg, and its iceberg plugin caused
SIGSEGV in Dagster's multiprocess executor. Direct iceberg writes via
write_iceberg() are simpler, faster, and proven.
