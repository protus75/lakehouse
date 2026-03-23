---
name: Save working state aggressively to memory
description: Save current bugs, fixes in progress, and debugging state to memory so fresh chats can resume
type: feedback
---

Aggressively save working state to memory during debugging sessions. Ingestion has been re-run 20+ times across many chat sessions, and losing context on fresh chats wastes time re-discovering known issues.

**Why:** User has experienced repeated context loss across fresh chats, forcing re-discovery of bugs already identified and fixed.
**How to apply:** After identifying a bug or applying a fix, immediately update project memory with: what the bug is, root cause, fix applied, and verification status. Include specific entry names, error counts, and which validation categories are real vs false positives.
