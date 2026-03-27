---
name: feedback_pip_needs_rebuild
description: pip install in container is lost on restart — MUST rebuild image for new packages
type: feedback
---

NEVER pip install directly in a container and expect it to persist. Docker containers reset to their image state on restart. New packages MUST be added to requirements.txt AND the image rebuilt with `docker compose build`.

**Why:** User had to wait through 3 failed pipeline runs because I pip installed pyspellchecker in the container, restarted, and lost it. Then the grpc subprocess couldn't see it either.

**How to apply:**
1. Add package to `docker/requirements.txt`
2. Rebuild image: `docker compose -f docker/docker-compose.yml build dagster-daemon dagster-webserver`
3. Recreate containers: `docker compose -f docker/docker-compose.yml up -d dagster-daemon dagster-webserver`
4. Clear pycache + restart both
5. NEVER use `docker exec pip install` as a fix — it's always temporary
