---
name: ZERO validation errors required before any new features
description: CRITICAL - Rules must be 100% accurate. No moving to features until all data validates clean.
type: feedback
---

ZERO validation errors. Period. This is a parse of RULES that must be fully available and accurate. Not "best effort", not "95% is good enough", not "known issues we'll fix later."

**Why:** User has said this multiple times. Every time I try to move on with remaining validation errors, it's wrong. Rules data must be 100% correct before adding features on top.

**How to apply:**
- Never say "good enough" or "diminishing returns" about validation errors
- Never commit with known bad data
- Never start new features (AI summaries, new models, etc.) until existing data validates 100%
- If something can't be fixed, explain WHY it's impossible, don't just skip it
- Run ALL validators after every change and fix everything before committing
