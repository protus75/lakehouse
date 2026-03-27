---
name: feedback_dagster_only
description: NEVER run pipeline manually - always use Dagster UI or API, no exceptions
type: feedback
---

NEVER run pipeline steps manually (dbt build, dbt run, docker exec python, etc). Always use Dagster to trigger pipeline runs. The only exception is small diagnostic/sample queries for debugging.

**Why:** The whole point is to validate the system works end-to-end through Dagster. Running dbt directly bypasses orchestration and doesn't prove the system works.

**How to apply:** Use the Dagster UI at http://localhost:3000 to trigger jobs. If the CLI doesn't work, use the UI or GraphQL API. Tell the user to trigger from the UI if programmatic methods fail.
