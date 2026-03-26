---
name: feedback_no_hardcoded_anything
description: CRITICAL repeated violation - never hardcode pixel values, thresholds, strings, or any magic numbers anywhere
type: feedback
---

STOP hardcoding values in scripts, review tools, or anywhere else. This includes pixel positions, column offsets, threshold values, regex patterns, and string literals.

**Why:** User has had to correct this multiple times. Hardcoded values break when applied to different books/PDFs. Everything must either come from YAML config or be dynamically calculated from the data.

**How to apply:** Before writing ANY literal number or string that could vary between books:
1. Can it be computed from the data? → Dynamic calculation (e.g. cluster x-positions)
2. Must it be specified per-book? → Put in YAML config
3. Is it truly universal? → Put in _default.yaml
Never take the shortcut of hardcoding first and "migrating later."
