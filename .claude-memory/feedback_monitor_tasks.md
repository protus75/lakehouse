---
name: Monitor long-running tasks and report progress
description: CRITICAL - Never blindly wait on background tasks - monitor, report progress, estimate time, detect crashes
type: feedback
---

CRITICAL RULE — Docker processes crash silently and frequently in this environment. NEVER trust a long-running task is alive.

For ANY command expected to take more than 1 minute:
1. Run in background
2. Poll every 30s with `block: false` — compare output length/content between polls
3. If no output change after 60s, check `docker exec ... ps aux | grep python` — the process is probably dead
4. Report to user: what stage it's at, % progress if available, estimated time remaining
5. If dead, tell user immediately and restart or investigate

NEVER:
- Call `block: true` with a 10-minute timeout and go silent
- Assume a task is running just because the task ID says "running"
- Wait more than 60s without reporting something to the user

**Why:** This happened repeatedly in this session. Processes died, Claude waited silently for 10+ minutes, user saw an idle computer with no feedback. Extremely frustrating and wastes time.
**How to apply:** Every long command gets a monitoring loop. No exceptions.
