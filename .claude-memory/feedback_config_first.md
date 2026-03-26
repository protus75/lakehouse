---
name: feedback_config_first
description: CRITICAL - Always write config-driven code from the start, never add values to code then move to config later
type: feedback
---

When writing new functionality, put ALL configurable values in YAML config files FIRST, then write code that reads from config. Never write hardcoded values with the intent to "move them to config later" — that's how they get permanently baked in.

**Why:** User has had to repeatedly remind about this. It's a core project principle (CLAUDE.md: "All thresholds, patterns, and corrections go in YAML configs — never hardcode"). Writing hardcoded values first creates unnecessary rework and the user has to catch it every time.

**How to apply:** Before writing any new function that uses thresholds, model names, URLs, prompts, patterns, or magic numbers: (1) add the config keys to `_default.yaml` first, (2) add any book-specific overrides to the book config, (3) then write the code to read from `config.get()`. The code should never work without the config being set.
