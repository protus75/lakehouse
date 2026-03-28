---
name: No cd prefix, and fix Git Bash path mangling
description: Never cd prefix in Bash tool, and always set MSYS_NO_PATHCONV=1 for docker exec with Linux paths
type: feedback
---

## No cd prefix
Never use `cd d:/source/... &&` prefix in Bash tool commands. Run commands directly.

**Why:** The permission allow-list uses prefix matching. Prepending `cd` breaks matching.

## Git Bash path mangling — CRITICAL
When running `docker exec` commands that contain Linux paths (e.g. `/workspace/...`), Git Bash on Windows converts them to Windows paths (e.g. `C:/Program Files/Git/workspace/...`). This breaks every `docker exec` command with absolute paths.

**Fix:** Always set `MSYS_NO_PATHCONV=1` in the environment, or prefix the command with `MSYS_NO_PATHCONV=1`. In Python scripts, pass `env={"MSYS_NO_PATHCONV": "1", ...}` to subprocess calls. In the Bash tool, prefix with `MSYS_NO_PATHCONV=1`.

**How to apply:** Any time you write a `docker exec` command with a Linux path — in scripts, in the Bash tool, or in instructions to the user — ensure path conversion is disabled.
