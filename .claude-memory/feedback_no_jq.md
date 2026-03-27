---
name: Never use jq
description: Never use jq - use python for JSON parsing instead
type: feedback
---

Never use jq. Use python for any JSON parsing in commands/hooks.

**Why:** jq is not installed on the host and user considers it a crap tool.
**How to apply:** Use `python -c "import json..."` instead of jq for any JSON processing in bash commands, hooks, or scripts.
