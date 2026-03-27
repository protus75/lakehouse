---
name: Pipeline operations rules
description: Generic pipeline rules — Dagster only, monitoring, stale processes, no downloads, finish tasks, zero validation errors
type: feedback
---

## Dagster only — CRITICAL (repeated twice)
NEVER run pipeline steps manually (docker exec, dbt build, etc). Always use Dagster UI at http://localhost:3000. Only exception: small diagnostic/sample queries for debugging.

## Monitor long-running tasks — CRITICAL
Docker processes crash silently. For ANY command >1 minute:
1. Run in background
2. Poll every 30s, compare output between polls
3. Check GPU (nvidia-smi) and container memory
4. If no output change after 60s — process is probably dead
5. Report: stage, % progress, ETA, GPU/memory stats
6. If dead, tell user immediately

NEVER go silent waiting on a long task.

## Kill stale processes
Before every pipeline run: check for leftover python processes in containers. After completion: verify cleanup. Report GPU state.

## No downloads — CRITICAL
Before any pipeline run: verify model cache volumes mounted, check env vars. Monitor stderr for "Downloading" in first 5 seconds — kill immediately if found. Ask user before any download. Metered network.

## Finish before moving on
Complete current task fully — run full validation, review results, fix issues. Don't present menus of options after partial results.

## Zero validation errors — CRITICAL
ZERO errors before new features. Never say "good enough." Never commit with known bad data. If unfixable, explain WHY.
