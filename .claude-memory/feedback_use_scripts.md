---
name: Use project scripts, don't wing it
description: Always use existing scripts (scripts/) for operations instead of running raw docker/CLI commands
type: feedback
---

Use the project's existing scripts for all operations — don't improvise raw docker exec or CLI commands.

**Why:** User was frustrated when I ran ad-hoc docker exec commands instead of using the provided `scripts/tabletop_rules/tabletop_browser.py` and `scripts/dagster.py` wrappers. The scripts handle env vars (MSYS_NO_PATHCONV), process management, and logging correctly.

**How to apply:** Before running any docker/pipeline/browser command, check `scripts/` for an existing wrapper. Key scripts:
- `scripts/dagster.py` — launch/status/reset/verify Dagster jobs
- `scripts/query_iceberg.py` — query Iceberg tables (--where, --full, --columns)
- `scripts/query_duckdb.py` — raw SQL queries via DuckDB
- `scripts/tabletop_rules/tabletop_browser.py` — start/stop/reset/log the Dash browser
- `scripts/tunnel.py` — Cloudflare tunnel management

Never pipe command output through inline python/grep/sed/awk. If the script doesn't support what you need, fix the script first.
