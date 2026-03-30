---
name: Pipeline and system operations
description: Kill stale processes before runs, stop means immediately, kill-all sequence, save debug state to memory
type: feedback
---

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

## Save working state aggressively
During debugging sessions, immediately save bugs, root causes, fixes, and verification status to memory. Include specific entry names, error counts, which validations are real vs false positives. Prevents re-discovery across fresh chats.
