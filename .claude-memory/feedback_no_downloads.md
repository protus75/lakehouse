---
name: feedback_no_downloads
description: CRITICAL - Never allow model/package downloads without asking. Kill immediately if detected.
type: feedback
---

Before launching any pipeline or process, verify no large downloads will occur. Check that model caches are mounted and accessible. If a download is detected (in stderr/logs), kill the process immediately.

**Why:** User has metered/limited wifi. Unexpected 1.35GB model downloads are unacceptable. Happened when Marker model cache wasn't mounted in daemon container.

**How to apply:** Before any pipeline run: (1) verify all model cache volumes are mounted, (2) check HF_HOME and other cache env vars are set, (3) monitor stderr for "Downloading" in first 5 seconds, (4) kill immediately if download detected, (5) ask user before proceeding if any download is needed.
