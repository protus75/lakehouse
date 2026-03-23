---
name: Monitor long-running tasks and report progress
description: CRITICAL - Never blindly wait on background tasks - monitor GPU/memory, report progress, estimate time, detect crashes
type: feedback
---

CRITICAL RULE — Docker processes crash silently and frequently in this environment. NEVER trust a long-running task is alive.

For ANY command expected to take more than 1 minute:
1. Run in background
2. Poll every 30s with `block: false` — compare output length/content between polls
3. **Check GPU utilization and memory** with nvidia-smi during GPU tasks (Marker, embedding)
4. **Check container memory** for OOM issues
5. If no output change after 60s, check process is alive — it's probably dead
6. Report to user: what stage it's at, % progress if available, estimated time remaining, GPU/memory stats
7. If dead, tell user immediately and restart or investigate

NEVER:
- Call `block: true` with a long timeout and go silent
- Assume a task is running just because the task ID says "running"
- Wait more than 60s without reporting something to the user
- Skip GPU/memory checks when monitoring — user watches these and expects you to too

**Why:** Happened repeatedly — processes died, Claude waited silently for 10+ minutes with idle GPU, user saw no activity and no feedback. Extremely frustrating and wastes time.
**How to apply:** Every long command gets a monitoring loop with GPU/memory stats. No exceptions.
