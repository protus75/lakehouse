---
name: Pipeline and system operations
description: Kill stale processes before runs, stop means immediately, kill-all sequence, save debug state, no cd prefix in Bash tool
type: feedback
---

## Validate preconditions BEFORE launching any pipeline run
Run `python scripts/dagster.py catalog` and inspect for stale entries (catalog rows with no metadata on disk). If ANY appear, fix them first — `catalog clean` to drop them, or investigate why they exist. NEVER launch a pipeline against known-broken state.

**Why:** Stale catalog entries cause immediate dbt/iceberg failures (`Could not guess Iceberg table version`). Launching anyway wastes a 6-8 minute pipeline run on a problem that was visible in 2 seconds before launch. The user explicitly called this out: "why wasn't a test of stale data run beforehand."

**How to apply:** Before EVERY `dagster.py launch`:
1. `python scripts/dagster.py catalog` — verify zero `STALE` entries
2. `python scripts/dagster.py preflight` — verify containers, ollama, warehouse OK
3. Only then launch
This is the same discipline as "estimate and monitor" — validate inputs before committing to a long operation.

## Kill stale processes before every pipeline run
Check for leftover python processes in containers before runs. After completion: verify cleanup. Report GPU state.

**Why:** Stale processes hold GPU memory and cause OOM or hangs on the next run.

## Stop means stop — immediately
When user says "stop" or "stop everything": `docker stop $(docker ps -q)` FIRST. Then report. Don't ask questions or investigate first.

## Kill all means ALL
1. `docker stop $(docker ps -q)` — stop all containers
2. `docker buildx stop` — stop builds
3. `wsl --shutdown` — kill WSL entirely
4. Verify: vmmem and com.docker.backend gone from Task Manager
5. Report RAM/disk/network back to baseline

## No manual docker exec for pipeline work
NEVER run pipeline scripts via `docker exec`. ALL pipeline steps go through Dagster — enrichment, dbt, publish, everything. The only acceptable `docker exec` is small diagnostic queries for debugging. This is a lakehouse, not a pile of scripts.

**Why:** Manual commands bypass orchestration, lose logging, and can't be monitored via Dagster.
**How to apply:** Use `python scripts/dagster.py launch <job>` and monitor with `python scripts/dagster.py status <id>`.

## Bash tool: NEVER use cd prefix — CRITICAL
Run commands directly in the Bash tool without any `cd /path &&` prefix. The working directory is already set to the project root. Just run `git status`, not `cd d:/source/lakehouse/lakehouse && git status`.

**Why:** CLAUDE.md explicitly says this. User has corrected this multiple times. The cd prefix also triggers unnecessary permission prompts.
**How to apply:** Every Bash tool call — git commands, python scripts, docker commands — run directly without cd.

## Save working state aggressively
During debugging sessions, immediately save bugs, root causes, fixes, and verification status to memory. Include specific entry names, error counts, which validations are real vs false positives. Prevents re-discovery across fresh chats.
