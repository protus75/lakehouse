---
name: Pipeline operations rules
description: Generic pipeline rules — Dagster only, monitoring, stale processes, no downloads, finish tasks, zero validation errors
type: feedback
---

## Dagster only — CRITICAL (repeated THREE times)
NEVER run pipeline steps manually (docker exec, dbt build, ollama pull, etc). Always use Dagster jobs/assets. Only exception: small diagnostic/sample queries for debugging.
**Why:** User has been very clear — all operations go through Dagster, no exceptions. This includes model pulls (use seed_models job), pipeline runs, and any other operational task.
**How to apply:** When tempted to run something directly, find or create the Dagster asset/job for it instead.

## Monitor long-running tasks — CRITICAL
Docker processes crash silently. For ANY command >1 minute:
1. Run in background
2. Poll every 30s, compare output between polls
3. Check disk I/O and network activity to verify actual progress — don't just re-read logs
4. If no output change after 60s — check disk/network; if idle, process is hung — cancel immediately
5. Report: stage, % progress, ETA, GPU/memory stats
6. If dead or hung, tell user and cancel immediately — do NOT keep polling a stuck process

NEVER go silent waiting on a long task. NEVER keep polling when logs show no change — verify with disk/network.

## Kill stale processes
Before every pipeline run: check for leftover python processes in containers. After completion: verify cleanup. Report GPU state.

## No downloads — CRITICAL
Before any pipeline run: verify model cache volumes mounted, check env vars. Monitor stderr for "Downloading" in first 5 seconds — kill immediately if found. Ask user before any download. Metered network.

## Finish before moving on
Complete current task fully — run full validation, review results, fix issues. Don't present menus of options after partial results.

## Zero validation errors — CRITICAL
ZERO errors before new features. Never say "good enough." Never commit with known bad data. If unfixable, explain WHY.
