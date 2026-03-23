---
name: No hardcoded values - everything in config
description: CRITICAL - Never hardcode thresholds, field names, patterns, or magic numbers. Everything goes in YAML config.
type: feedback
---

NEVER hardcode values in ingestion or validation code. ALL thresholds, field names, patterns, and magic numbers go in the YAML config files (`documents/tabletop_rules/configs/_default.yaml` and per-book overrides).

This includes:
- Metadata field names and required fields
- Length thresholds (min content, fragment detection, description detection)
- Pattern strings (spell patterns, heading patterns, etc.)
- Chunk sizes and overlap
- Dedup signature lengths
- Sub-heading limits
- Any numeric constant that could vary between books

**Why:** User has corrected this multiple times across sessions. The whole point of the config system is to avoid hardcoded values. Code should read from config with no inline fallbacks that differ from the config defaults.
**How to apply:** Before writing ANY literal string or number in ingestion/validation code, check if it should be in config. If a value exists in config, NEVER duplicate it as a fallback in code. If it's a new threshold, add it to `_default.yaml` first, then reference it from code.
