---
name: Tooling and command preferences
description: Tool preferences NOT in CLAUDE.md or hooks — WSL GPU fix, Streamlit, VSCode, no jq, read-only permissions, git ops
type: feedback
---

## Editor
VSCode only. Never suggest notepad, nano, vi. Use Edit tool directly or `code "path"`.

## No jq
Use `python -c "import json..."` instead. jq is not installed.

## Read-only permissions
Don't prompt for ANY read-only commands — docker exec reads, file searches, queries, syntax checks. Just run them.

## Git operations
Never prompt for read-only git commands (status, diff, log, etc.) — just run them. Only prompt for write operations (add, commit, push, etc.).

## WSL + Docker Desktop GPU fix
After updating the NVIDIA driver, Docker GPU containers may crash with `SIGSEGV` in `nvidia-container-runtime-hook`. Fix: run `wsl --shutdown` in PowerShell, then restart Docker Desktop.

**Why:** Spent significant debugging time on this (2026-03-27). Docker Desktop update alone wasn't enough — the WSL shutdown was the key step.

## Streamlit workflow
ALWAYS kill and restart Streamlit after ANY code change. Clear `__pycache__` first. Test the full page load flow in Python before telling user to reload.

## Web app links
Always provide clickable URLs when referencing web apps (http://localhost:3000 for Dagster, http://localhost:8501 for Streamlit, etc.).

## Test before asking
Verify code works (simulate page loads, run functions, check for errors) before asking user to check in browser.
