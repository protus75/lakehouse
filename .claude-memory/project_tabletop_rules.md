---
name: Tabletop rules project — decisions and context not in code
description: Non-obvious decisions, gotchas, and remaining work for tabletop rules pipeline
type: project
---

## Current focus: Player's Handbook (PHB) only
Process one book until validation passes before moving to others.

## Key decisions
- pymupdf page_texts is primary content source — Marker drops pages
- SeaweedFS batch delete_objects is broken — use single-object deletes
- Bronze uses overwrite_all (DuckDB iceberg extension doesn't honor PyIceberg delete files)
- AI summaries + annotations are stale — need re-run after stable keys migration

## Remaining work
1. 8 dbt test failures to investigate
2. Table cell quality (36 few_columns, 39 smushed)
3. Browser app needs gold_tables rendering + ToC-driven content assembly
4. Re-run AI enrichment after entry builder rewrite
5. Stable integer keys migration (see project_tabletop_integer_keys.md)
