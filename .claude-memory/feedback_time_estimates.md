---
name: Always give time estimates
description: User wants time estimates for any task over 15s — never leave them waiting with no idea how long something takes
type: feedback
---

Always give a time estimate before starting any task expected to take more than 15 seconds.

**Why:** User got frustrated by repeatedly waiting undetermined amounts of time with no indication of duration. The system default says "don't give estimates" — user explicitly overrides this: they WANT estimates.

**How to apply:** Before kicking off a pipeline run, docker build, enrichment job, or any non-trivial command, state the expected duration. Combine with monitoring — if a task is expected to take >1 min, run in background and poll. Never go silent.
