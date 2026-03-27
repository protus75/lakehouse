---
name: No cd prefix in Bash commands
description: Never prefix Bash tool commands with cd — it breaks permission allow-list matching
type: feedback
---

Never use `cd d:/source/... &&` prefix in Bash tool commands. Run commands directly (e.g. `git status` not `cd /path && git status`).

**Why:** The permission allow-list uses prefix matching (e.g. `Bash(git log:*)`). Prepending `cd` makes the command start with `cd` instead of the actual tool, so it doesn't match and the user gets prompted unnecessarily.

**How to apply:** Always run Bash commands directly without `cd` prefix. The working directory persists between calls. This is already in CLAUDE.md but was being ignored.
