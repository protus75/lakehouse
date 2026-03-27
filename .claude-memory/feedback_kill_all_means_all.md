---
name: feedback_kill_all_means_all
description: "Stop all" means stop ALL Docker + WSL immediately, verify zero activity
type: feedback
---

When user says "stop all" or "kill all":
1. `docker stop $(docker ps -q)` — stop all containers
2. `docker buildx stop` — stop any builds
3. `wsl --shutdown` — kill WSL entirely (Docker Desktop, buildkit, everything)
4. Verify: check Task Manager-visible processes are gone (vmmem, com.docker.backend)
5. Report RAM/disk/network are back to baseline

Do ALL steps immediately. Don't leave background builds running. Don't leave WSL up. The user monitors system resources and can see when things are still active.

**Why:** Multiple times I left builds, WSL, or buildkit running after user said stop. They saw high RAM/disk/network and had to ask repeatedly.
