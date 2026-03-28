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

## Remaining work — browser rewrite (next session)
The browser app needs a clean rewrite. Current state: matching works (486 spells, 65 profs), data is correct in Iceberg, but browser display is broken.

What needs to happen:
1. gold_entries.sql: LEFT JOIN from toc → entries on toc_id (not toc_title string). Stashed version has this but also has broken \n page separator change — apply the SQL only.
2. publish_to_iceberg.py: add gold_entries to PUBLISH_MAP
3. Entry builder: toc_entry = section dict (not chapter) so entries get correct toc_id. Lines ~983 (spells → level_sub), ~1008 (ch-level authority → ch), ~1046 (sub-level authority → sub), ~1079 (toc sections → sec)
4. tabletop_browser.py: rewrite from scratch. Query gold_entries ordered by sort_order. Loop entries. For each new toc_id render heading + anchor. Render content. Sidebar: loop gold_toc, link to #toc-{toc_id}. No caching, no grouping, no searching.
5. Content paragraph collapse: page boundaries create false \n\n breaks. Fix in _clean_entry_content (merge paragraphs where prev doesn't end .!?:; and next starts lowercase). Do NOT change _get_page_range_text separator — it breaks heading detection.
6. Container has no ps/fuser — browser start script needs different kill method (save PID to file)
7. Add no-cache HTTP headers to Dash app

## Other remaining work
1. 8 dbt test failures to investigate
2. Table cell quality (36 few_columns, 39 smushed)
3. Re-run AI enrichment after entry builder rewrite
4. Stable integer keys migration (see project_tabletop_integer_keys.md)
