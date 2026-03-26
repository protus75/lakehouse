---
name: Test before asking user to check
description: Always verify code works (simulate page loads, run tests) before asking user to try it
type: feedback
---

Always test that code actually works before asking the user to check it in the browser or run it.

**Why:** User got frustrated being asked to reload multiple times only to hit errors each time. Wasted their time.
**How to apply:** After any Streamlit/UI change, simulate the full page load flow in Python (call all the functions the page calls, check for KeyErrors, missing imports, etc.) before telling the user to reload. Same applies to any script — do a dry run first.
