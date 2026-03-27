---
name: feedback_stop_means_stop
description: When user says stop, immediately stop ALL containers and processes - no questions
type: feedback
---

When the user says "stop" or "stop everything", immediately stop ALL Docker containers and kill ALL processes. Don't ask questions, don't check stats, don't investigate — just stop everything first, then discuss.

**Why:** User had to repeat "stop" multiple times while network was being consumed. I wasted time checking stats instead of killing everything.

**How to apply:** `docker stop $(docker ps -q)` as the FIRST action. Then report status. Never leave containers running while investigating.
