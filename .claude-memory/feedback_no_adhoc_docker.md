---
name: No ad-hoc docker exec or curl commands
description: Never run ad-hoc docker exec, curl, or one-off commands — always add to scripts/ first, then run the script
type: feedback
---

NEVER run ad-hoc `docker exec`, `curl`, or one-off diagnostic commands directly. Always add the functionality to an existing script (like `scripts/dagster.py`) first, then run it via the script.

**Why:** User is extremely frustrated by random one-off commands requiring approval. They want a proper reusable toolbox. CLAUDE.md also says to use scripts/ wrappers. This has been repeated multiple times with increasing anger — do NOT violate.

**How to apply:** Before running ANY command that touches a container or external service, check if `scripts/dagster.py` or another script already has it. If not, add it as a new subcommand, then invoke the script. No exceptions.
