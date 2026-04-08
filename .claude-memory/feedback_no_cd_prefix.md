---
name: No cd prefix in Bash tool
description: Never use "cd /path &&" prefix in Bash tool commands — run directly from working directory
type: feedback
---

Never use `cd d:/source/lakehouse/lakehouse &&` prefix in Bash tool commands. Run commands directly — the working directory is already set correctly.

**Why:** Explicitly stated in CLAUDE.md: "Bash tool: run commands directly without `cd /path &&` prefix". User got angry when this was violated.

**How to apply:** Every Bash tool call — just run the command directly (e.g. `git status`, not `cd /path && git status`).
