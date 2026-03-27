---
name: System operations and memory rules
description: Stop/kill commands, save working state aggressively, memory file location
type: feedback
---

## Stop means stop — immediately
When user says "stop" or "stop everything": `docker stop $(docker ps -q)` FIRST. Then report. Don't ask questions or investigate first.

## Kill all means ALL
1. `docker stop $(docker ps -q)` — stop all containers
2. `docker buildx stop` — stop builds
3. `wsl --shutdown` — kill WSL entirely
4. Verify: vmmem and com.docker.backend gone from Task Manager
5. Report RAM/disk/network back to baseline

## Save working state aggressively
During debugging sessions, immediately save bugs, root causes, fixes, and verification status to memory. Include specific entry names, error counts, which validations are real vs false positives. Prevents re-discovery across fresh chats.

## Memory file location
Write all memory files to `d:\source\lakehouse\lakehouse\.claude-memory\`. User keeps them in repo for git tracking.
