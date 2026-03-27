---
name: Integer keys migration plan
description: Plan to replace unstable auto-increment IDs with deterministic hash-based stable integer keys across all layers
type: project
---

All current IDs (entry_id, toc_id, chunk_id) are auto-increment counters or row_number() — they reset on every rebuild, breaking enrichment data and cross-references.

**Solution:** SHA-256 hash of natural key columns, truncated to int64. Same entity always gets the same ID.

**Phases:**
1. Foundation: new `dlt/lib/stable_keys.py` + `stable_keys` section in `config/lakehouse.yaml`
2. Silver: stable entry_id (hash of source_file+toc_title+entry_title+section_title), stable toc_id (hash of source_file+title), add toc_id FK to silver_entries
3. Gold: stable chunk_id (hash of source_file+toc_title+entry_title+chunk_index), propagate entry_id/toc_id as integer FKs
4. Query layer: replace string joins (entry_title, toc_title) with integer FK joins in Streamlit
5. parent_toc_id: self-referential FK on silver_toc_sections replacing parent_title string
6. Enrichment: remove orphan purge logic — stable IDs make enrichment truly incremental (saves ~70min per rebuild)
7. dbt relationship tests for FK validation

**Why:** String keys break on rebuild, waste enrichment time, and won't scale to multiple books.
**How to apply:** Implement phases 1-3 first, then 4-7. One-time re-enrichment needed after migration.
