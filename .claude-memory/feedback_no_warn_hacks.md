---
name: feedback_no_warn_hacks
description: NEVER set test severity to warn to bypass failures — fix the actual issue
type: feedback
---

Never set dbt test severity to 'warn' to make the pipeline pass. Fix the actual failures. Zero validation errors means zero — not "zero errors but some warnings hiding problems."

**Why:** User caught me hiding 3 failing tests as warnings to get the pipeline running. That's the exact opposite of the zero-errors rule.

**How to apply:** If a test fails, fix the code or fix the test if it's wrong. Never suppress with severity='warn'.
