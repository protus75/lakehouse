---
name: Allow readonly commands without prompting
description: Don't prompt for permission on read-only Python/bash commands like syntax checks, queries, validations
type: feedback
---

Don't prompt for permission on read-only commands — syntax checks, database queries, validation scripts, file reads. Just run them.

**Why:** User finds approval prompts for harmless read-only operations disruptive to workflow.
**How to apply:** Use `dangerouslyDisableSandbox: true` for read-only Python commands, grep, status checks, etc.
