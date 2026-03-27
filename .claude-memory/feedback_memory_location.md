---
name: Memory location override
description: Memory files are in .claude-memory/ in project root, NOT the default system path — always check there first
type: feedback
---

Memory files live at `d:\source\lakehouse\lakehouse\.claude-memory\`, NOT the default `C:\Users\richard\.claude\projects\...\memory\`.

**Why:** User moved memory into the repo for git tracking. The system default path will be empty. User was frustrated when Claude checked the wrong location first and missed all 11 memory files.

**How to apply:** On EVERY new conversation, before doing anything else, check `.claude-memory/MEMORY.md` in the project root. Read it. Read the referenced files. Then proceed.
