---
name: Tooling and command preferences
description: Shell commands, editor preferences, tools to avoid (jq, notepad), permissions, package management, Streamlit/UI workflow
type: feedback
---

## Shell commands
- No inline comments in copyable commands — put explanations in markdown above code fences
- Always use PowerShell syntax (`;` not `&&`), label blocks as `powershell`
- Always include `cd` or use absolute paths — never assume directory
- Always specify which terminal (PowerShell, WSL2, etc.)

## Editor
VSCode only. Never suggest notepad, nano, vi. Use Edit tool directly or `code "path"`.

## No jq
Use `python -c "import json..."` instead. jq is not installed and user considers it crap.

## Read-only permissions
Don't prompt for ANY read-only commands — docker exec reads, file searches, queries, syntax checks. Just run them.

## Package management
NEVER pip install in container — lost on restart. Add to `docker/requirements.txt`, rebuild image, recreate containers. `docker exec pip install` is always temporary.

## Streamlit workflow
ALWAYS kill and restart Streamlit after ANY code change. Clear `__pycache__` first. Test the full page load flow in Python before telling user to reload.

## Web app links
Always provide clickable URLs when referencing web apps (http://localhost:3000 for Dagster, http://localhost:8501 for Streamlit, etc.).

## Test before asking
Verify code works (simulate page loads, run functions, check for errors) before asking user to check in browser.
