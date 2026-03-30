---
name: Never use overwrite_all, isolate pipeline pieces
description: Never use write_iceberg(overwrite_all=True). Each pipeline piece owns its own tables exclusively — no shared tables between pieces.
type: feedback
---

Never use `write_iceberg(overwrite_all=True)`. It drops the entire table and destroys data written by other pipeline steps.

**Why:** User lost an hour of AI enrichment summaries because `publish_to_iceberg` used `overwrite_all=True` on `gold_entry_descriptions`, wiping the summary rows that the enrichment script had written. Silent data loss with no recovery.

**How to apply:**
- Each pipeline piece (bronze extraction, silver/gold publish, enrichment summaries, enrichment annotations) must be a separate, independent pipeline.
- Each piece owns its own tables exclusively. No two pieces write to the same table.
- Build a wrapper script that can flexibly call only the pieces needed.
- If data from multiple pieces needs to be combined, do it at read time (joins/views), not by co-locating rows in one table.
