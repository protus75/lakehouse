---
name: feedback_monitor_properly
description: Run long tasks in background, monitor progress, report ETAs — never run blocking and never skip monitoring
type: feedback
---

Always run long docker/pipeline tasks in background, monitor their progress, and report ETAs. Check for stale processes before starting. Never run blocking commands that tie up the conversation.

**Why:** User explicitly corrected this. It's also in CLAUDE.md under the memory instructions about monitoring.

**How to apply:** Before any pipeline run: (1) kill stale processes, (2) estimate runtime, (3) run in background, (4) periodically check output and report progress. Don't move on to next steps until current validation is complete and results are reviewed.
