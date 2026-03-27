---
name: Always use Dagster, never manual pipeline runs
description: CRITICAL - Never run pipeline steps manually via docker exec, always use Dagster jobs/assets
type: feedback
---

NEVER run pipeline steps manually (docker exec python -u ..., dbt build, publish_to_iceberg). Always use Dagster.

**Why:** Manual runs bypass orchestration, skip dependencies, cause inconsistent state. The whole point of Dagster is to manage the pipeline.
**How to apply:** Use Dagster UI at http://localhost:3000 to trigger jobs/assets. Jobs: `tabletop_full_pipeline`, `tabletop_without_enrichment`. Assets: `bronze_tabletop → dbt_tabletop → publish_to_iceberg → gold_ai_summaries / gold_ai_annotations`.
