---
name: Coding rules and standards
description: Coding rules NOT already in CLAUDE.md — fix at ingestion, no warn hacks
type: feedback
---

## Fix at ingestion, not export
Data quality issues belong in ingestion/parsing code, not export layer. Use config-driven patterns so fixes apply to all inputs.

**Why:** Fixing in export means every consumer must re-apply the same fix. Fixing at ingestion fixes it once for all downstream.

## Never set test severity to warn
Fix actual failures. Zero validation errors means zero — not "zero errors but some warnings." Never suppress with severity='warn'.

**Why:** User corrected this explicitly. "Good enough" is not acceptable for a rules reference.
