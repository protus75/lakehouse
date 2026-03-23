---
name: Write memory files to repo .claude-memory dir
description: Memory files must go to .claude-memory/ in the repo, not the system path
type: feedback
---

Write all memory files to `d:\source\lakehouse\lakehouse\.claude-memory\`. The system path is symlinked here — one source of truth.

**Why:** User keeps memory files in the repo so they show in git status and can be committed.
**How to apply:** Always use `d:\source\lakehouse\lakehouse\.claude-memory\` for memory file paths.
