---
name: Kill stale processes and monitor GPU
description: Always check for and kill stale python/GPU processes before running new ones, monitor GPU usage
type: feedback
---

Always check for stale python processes in the container before launching new pipeline runs.
After a run completes, verify no zombie processes remain.

**Why:** Stale bronze/marker processes were left running and consumed GPU at 50%.
**How to apply:** Before every pipeline run, check `cat /proc/*/cmdline` for leftover python processes. After completion, verify cleanup. Report GPU state if relevant.
