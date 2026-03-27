---
name: Multi-project support and cache/restart DX improvements
description: Future TODO — multi-project lakehouse config structure and reducing Dagster restart pain on code changes
type: project
---

## Multi-project support (future — after PHB complete)

Refactor `lakehouse.yaml` to a `projects:` dict keyed by project name, each with its own namespaces, paths, and configs. Shared infra (S3, catalog, write_iceberg, get_reader) is already project-agnostic.

Each new project gets: bronze pipeline, dbt model folders, Dagster asset group/jobs, per-document configs.

**Why:** Current config is hardcoded to tabletop_rules. Adding a second project (recipes, etc.) requires parameterizing namespaces and paths.

**How to apply:** Don't build until a second project is needed. When adding project #2, refactor config, parameterize bronze, add new dbt folders.

## Cache/restart DX improvements (future)

Current pain: every code change requires __pycache__ clear + docker restart + 15s wait due to Dagster gRPC module caching.

Options ranked:
1. **Dagster "Reload Definitions"** — UI button or API call to reimport without container restart. Test if this works reliably first.
2. **Subprocess-based assets** — bronze calls `subprocess.run(["python", "-m", "dlt.bronze_..."])` so Dagster never caches project code. Already used for dbt.
3. **File watcher for __pycache__** — auto-clear on .py save so it's never forgotten.
4. **`--use-python-environment-entry-point`** in workspace.yaml — no gRPC cache, slower per-run but zero cache issues.

**Why:** Cache clearing + restart is the #1 dev friction point and will get worse with multiple projects.

**How to apply:** Try option 1 first. If adding more projects, use option 2 (subprocess) so new projects don't require image rebuilds.
