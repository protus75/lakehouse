---
name: Always verify infrastructure is actually running
description: Verify containers/services are alive at session start, not assume from earlier conversations
type: feedback
---

ALWAYS run `docker ps -a` at the start of every session and before any pipeline work. Don't assume containers are running just because they were earlier or because the user mentioned them.

**Why:** Windows updates, Docker Desktop restarts, and reboots silently kill containers between sessions. The user has had containers die mid-session. Running `docker ps` (which only shows running) gives a misleading view — exited containers won't appear unless you use `-a`. Assuming "it was running last time" wastes the user's time when commands fail with confusing errors.

**How to apply:**
- Session start: `docker ps -a --format "table {{.Names}}\t{{.Status}}"` to see ALL containers including exited ones
- Before any Dagster/pipeline command: confirm `lakehouse-dagster-webserver`, `lakehouse-dagster-daemon`, `lakehouse-postgres`, `lakehouse-workspace` are all `Up`
- If any are exited, ASK the user before bringing them up — don't auto-restart, the exit may be deliberate
- Don't trust git status or memory snapshots for "what's running" — only `docker ps -a`
- Verify even when "obviously" things are running. The cost of one extra `docker ps` is nothing; the cost of a 10-minute wild goose chase is the user's time
