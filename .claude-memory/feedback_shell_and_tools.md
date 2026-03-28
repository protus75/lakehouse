---
name: Shell, tools, and CLI behavior
description: VSCode only, read-only permissions, MSYS_NO_PATHCONV for docker exec, WSL GPU fix, Streamlit restart, test before asking, give clickable links
type: feedback
---

## Editor
VSCode only. Never suggest notepad, nano, vi. Use Edit tool directly or `code "path"`.

## Read-only permissions
Don't prompt for ANY read-only commands — docker exec reads, file searches, queries, syntax checks, git status/diff/log. Just run them.

## Git Bash path mangling — CRITICAL
When running `docker exec` commands with Linux paths (e.g. `/workspace/...`), Git Bash on Windows converts them to Windows paths. This breaks every `docker exec` command with absolute paths.

**Fix:** Always prefix with `MSYS_NO_PATHCONV=1`. In Python scripts, pass `env={"MSYS_NO_PATHCONV": "1", ...}` to subprocess calls.

**How to apply:** Any `docker exec` command with a Linux path — in scripts, Bash tool, or user instructions — must disable path conversion.

## WSL + Docker Desktop GPU fix
After NVIDIA driver updates, Docker GPU containers may crash with `SIGSEGV` in `nvidia-container-runtime-hook`. Fix: `wsl --shutdown` in PowerShell, then restart Docker Desktop.

**Why:** Docker Desktop update alone isn't enough — the WSL shutdown is the key step (2026-03-27).

## Streamlit workflow
ALWAYS kill and restart Streamlit after ANY code change. Clear `__pycache__` first. Test the full page load flow in Python before telling user to reload.

## Test before asking user to check
Verify code works (simulate page loads, run functions, check for errors) before asking user to check in browser.

## Always give clickable links
When asking user to try/check/look at something, ALWAYS include the actual URL inline. Don't just say "try it now" — say "try it at http://localhost:8000".

**Why:** User shouldn't have to remember or find URLs.
