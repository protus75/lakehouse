---
name: Pipeline operations rules
description: Pipeline rules NOT in CLAUDE.md or hooks — kill stale processes, pre-run checks
type: feedback
---

## Kill stale processes
Before every pipeline run: check for leftover python processes in containers. After completion: verify cleanup. Report GPU state.

**Why:** Stale processes hold GPU memory and cause OOM or hangs on the next run.

## Finish before moving on
Complete current task fully — run full validation, review results, fix issues. Don't present menus of options after partial results.

**Why:** User has corrected this pattern. Partial results with a menu of "what next?" is frustrating.
