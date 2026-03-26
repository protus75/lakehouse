---
name: Always restart Streamlit after code changes
description: MUST restart Streamlit after every code change - user keeps seeing stale cached UI
type: feedback
---

ALWAYS kill and restart Streamlit after ANY code change to streamlit/ files. Every single time.

**Why:** Streamlit caches modules aggressively. The user repeatedly saw stale old code because Streamlit wasn't restarted, wasting their time and causing frustration.
**How to apply:** After editing any file under streamlit/, immediately kill all streamlit processes, clear __pycache__, and restart before telling the user to reload.
