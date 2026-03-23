---
name: Allow readonly commands without prompting
description: Don't prompt for permission on ANY read-only commands - bash, docker exec, file searches, everything
type: feedback
---

Don't prompt for permission on read-only commands — syntax checks, database queries, validation scripts, file reads, docker exec reads, find/ls/glob searches. Just run them ALL without asking.

**Why:** User finds approval prompts for harmless read-only operations extremely disruptive — especially when there are many during debugging sessions.
**How to apply:** Use `dangerouslyDisableSandbox: true` for ALL read-only commands including docker exec, bash reads, file searches, etc. If it doesn't modify state, don't ask.
