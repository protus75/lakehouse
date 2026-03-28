---
name: Integer keys migration — COMPLETED
description: Hash-based stable integer keys implemented across all layers (2026-03-28)
type: project
---

**Status: DONE.** All IDs (entry_id, toc_id, chunk_id) are SHA-256 hash-based int64 via `dlt/lib/stable_keys.py`. Same entity always gets the same ID across rebuilds.

Remaining from original plan:
- Phase 6 (incremental enrichment) — not yet leveraged, re-run enrichment still needed
- Phase 7 (dbt FK relationship tests) — not yet added
