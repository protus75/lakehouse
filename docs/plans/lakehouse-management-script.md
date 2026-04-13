# Plan: `scripts/lakehouse.py` — Activity-Based Lakehouse Management

## Context

Every code change requires clearing pycache, restarting Dagster containers, waiting for gRPC — and forgetting a step wastes an entire pipeline run. The current `scripts/dagster.py reset` is a single monolith that doesn't distinguish between activities. The user needs named commands for what they're **doing**, not piecemeal system tasks. The lakehouse is multi-project (tabletop is just one) — resets must never touch data.

## Script: `scripts/lakehouse.py`

### Commands

| Command | When to use | What it does |
|---------|-------------|--------------|
| `code-reload` | Changed Python in dlt/, dagster/, scripts/ | Clear pycache (host+container) → restart dagster → poll gRPC ready → verify definitions loaded |
| `dbt-reload` | Changed dbt models/macros/schema YAML | Clear pycache → dbt-clean → restart dagster → poll gRPC → verify |
| `config-reload` | Changed lakehouse.yaml or per-book configs | Same as code-reload (config re-read on import) |
| `debug-cycle` | Tight code→test→fix loop, need speed | Clear pycache → GraphQL reload (no restart) → verify. Falls back to full restart if reload fails |
| `install-package` | Added to requirements-*.txt | Rebuild images → recreate containers → wait healthy → preflight. Optional `--package name` to verify import |
| `full-restart` | Nuclear option, something weird | compose down → compose up → wait healthy → preflight |
| `status` | "What's going on?" | Container states, dagster health, ollama status, catalog summary, recent runs |
| `preflight` | Before any pipeline run | Containers + ollama + warehouse + catalog stale check + pycache warning |

### Global flags
- `--dry-run` — print steps without executing
- `--verbose` — extra output

### Multi-project safety guarantee
- **NO command touches data** (no Iceberg deletes, no catalog drops, no F:/ file removal)
- Only removes: `__pycache__/` dirs, dbt `target/` + `partial_parse.msgpack`
- Future data-wipe commands would require explicit `--project <name>` flag

### Implementation details

**Import strategy**: Use `importlib.util` to load `scripts/dagster.py` as `dagster_cli` module (avoids name collision with `dagster` pip package). Import: `gql`, `_docker_py`, `cmd_reload`, `cmd_dbt_clean`, `cmd_preflight`, `cmd_catalog`, `cmd_verify`, `cmd_runs`, constants.

**gRPC wait**: Replace blind `time.sleep(15)` with polling — try GraphQL endpoint every 3s, up to 45s timeout. Much better UX (finishes as soon as ready, clear timeout error if not).

**Docker compose**: Try `docker compose` first, fall back to `docker-compose`. Compose file resolved via `Path(__file__).parent.parent / "docker" / "docker-compose.yml"`.

**Step logging**: Each command prints numbered steps: `[1/5] Clearing host pycache... OK (12 dirs)`. Failures print `FAILED: <reason>` and stop.

**Verification**: Every command that restarts/reloads ends with a verification step that confirms dagster definitions are loaded via GraphQL query for repositories.

### Internal helpers (in lakehouse.py)

- `_clear_pycache_host()` — shutil.rmtree loop on project root
- `_clear_pycache_containers(containers)` — docker exec find/rm in each container
- `_restart_dagster()` — docker restart + poll gRPC
- `_wait_healthy(containers, timeout)` — poll docker inspect until running
- `_verify_definitions()` — GraphQL repository query
- `_compose(*args)` — docker compose wrapper with compose file path
- `_step(n, total, msg)` — step printer

### Files to create/modify

| File | Action |
|------|--------|
| `scripts/lakehouse.py` | **Create** — the new script (~300 lines) |

No modifications to `dagster.py` — import its functions, don't change them.

### Verification
1. `python scripts/lakehouse.py --dry-run code-reload` — prints steps without executing
2. `python scripts/lakehouse.py status` — shows all system state
3. `python scripts/lakehouse.py code-reload` — does the full cycle, ends with "Ready"
4. `python scripts/lakehouse.py debug-cycle` — fast reload via GraphQL
5. `python scripts/lakehouse.py preflight` — full readiness check
