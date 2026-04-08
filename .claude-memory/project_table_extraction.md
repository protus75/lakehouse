---
name: Table extraction — font-switch detector implementation
description: PHB-validated font-switch table detection. Phases 1-5 implemented 2026-04-08, Phase 6 pending pipeline validation.
type: project
---

## Status: PHASES 1-5 IMPLEMENTED, Phase 6 deferred (2026-04-08, PHB)

The new font-switch detection path is fully wired into bronze + silver but
runs ALONGSIDE the legacy `extract_all_tables` / `_clean_table_pages_vlm`
path. Phase 6 (delete dead code) is deferred until a real pipeline run
validates the new path against real data.

Detector result on PHB: **all 69 declared ToC `is_table` entries (across 50
distinct pages) covered, 0 missing pages, 0 under pages.** Some pages have
2-3 tables stacked, so page count (50) is less than table count (69). The
count test is per-page lower-bound: every page with N declared tables must
have >= N detected regions. EXTRA pages (mid-section unlabeled tables or
non-table tabular content) are out of scope — silver only masks regions on
pages the detector finds them on.

## Architecture

### Source of truth
PyMuPDF span (font, size, bold) metadata. PHB body=`Formata-Light 9.5`,
table headers=`Formata-Regular 9.5` (same family/size, regular weight not
light). Headings use `UniversityRomanStd-Bold` so no false positives.
Per-book onboarding probe identifies the right styles for each new book.

### Hard rules (locked from probe results)
- `page.find_tables()` is dead — 0/47 PHB detection.
- minicpm-v cannot do bbox coordinates — would need qwen2.5-vl or similar.
- No text matching of ToC titles against page content.
- ToC page numbers ARE reliable.
- 2-column body text. Tables interleave with prose. Tables may occupy 1 or 2 columns.
- Per-region bboxes mandatory. Char-offset masks (not line-index).
- Silver leaves surrounding prose char-by-char intact.
- Count test = lower bound only: `detected_per_page >= toc_is_table_per_page`.

## Code locations

### Core library
- [dlt/lib/table_regions.py](dlt/lib/table_regions.py)
  - `detect_table_regions(page, cfg)` — main detector
  - `extract_page_text_with_span_map(page)` — char-offset span map (substring-equivalent to `page.get_text()`, 319/322 PHB pages byte-identical)
  - `region_char_ranges(region, span_map)` — bbox → char ranges
  - `extract_table_cells(page, region)` — structured (row, col, text) tuples
  - `_try_headerless_columns()` — fallback for tables with only a label row

### Bronze
- [dlt/bronze_tabletop_rules.py](dlt/bronze_tabletop_rules.py)
  - `detect_all_regions(filepath, page_printed, toc_sections, config)` — orchestrator, walks all non-excluded pages
  - `store_bronze` — writes the 3 new tables (regions/cells/masks)
  - `validate_bronze` — has new `table_region_coverage` check
  - Pipeline step 8b (between watermarks and store)

### Silver
- [dlt/lib/tabletop_cleanup.py](dlt/lib/tabletop_cleanup.py)
  - `build_entries_from_pages` accepts `page_text_masks` arg
  - Applies masks BEFORE watermark/page-number cleanup (offsets reference raw bronze.page_texts)
  - Skips legacy `_clean_table_pages_vlm` when masks are present
- [dbt/lakehouse_mvp/models/tabletop/silver/silver_entries.py](dbt/lakehouse_mvp/models/tabletop/silver/silver_entries.py)
  - Loads `bronze_tabletop.page_text_masks` and passes to builder

### Tooling
- [scripts/table_extraction.py](scripts/table_extraction.py) — single entrypoint with subcommands:
  - `fonts` — per-page font histogram (onboarding new books)
  - `vlm` — VLM bbox probe (proves whichever model is broken)
  - `detect` — run detector across the book, write summary JSON
  - `inspect` — query summary by status (OK/MISSING/UNDER/EXTRA) or page number
  - `debug` — verbose dump of detector internals for one page
  - `span-map` — verify byte-equivalence with `page.get_text()`
  - `mask` — dump original vs masked page text for given pages
  - `dry-bronze` — run `detect_all_regions` without Iceberg writes

### Config
- [documents/tabletop_rules/configs/DnD2e_Handbook_Player.yaml](documents/tabletop_rules/configs/DnD2e_Handbook_Player.yaml) `table_detection:` block
- [documents/tabletop_rules/configs/_default.yaml](documents/tabletop_rules/configs/_default.yaml) `lineage.catalog_tables` includes the 3 new tables
- Glossary added to PHB `exclude_chapters` (was generating false positives)

## New bronze tables (created on first bronze run after Phase 3)
```
bronze_tabletop.table_regions(source_file, page_index, printed_page_num,
  region_index, bbox_x0/y0/x1/y1, header_bbox_x0/y0/x1/y1,
  row_count, col_count, header_row_count, detection_method, run_id)

bronze_tabletop.table_cells(source_file, page_index, region_index,
  row_index, col_index, cell_text, run_id)

bronze_tabletop.page_text_masks(source_file, page_index, printed_page_num,
  char_start, char_end, reason, run_id)
```

## Detector algorithm (final)

1. Walk `page.get_text("dict")` spans, identify body style by histogram
2. Find header spans matching `(header_font, header_size, header_bold)`
3. Cluster header spans into header rows by y, then group adjacent rows
   into clusters by x-overlap + span-count compatibility (sequential
   single-span rows DON'T coalesce — kills glossary false positives)
4. For each cluster, extract column ranges as **midpoint zones** between
   header span centers (handles centered/right-aligned cell content) with
   `outer_margin` extension on leftmost/rightmost columns
5. Pick the row with the most distinct x-positions as the column template
   (handles 2-row headers like Table 53 THAC0s where one row has finer granularity)
6. Trace data rows down: include rows whose words align to columns; SKIP
   non-overlapping rows (other-column body text on 2-column pages); stop
   on 2 consecutive misses
7. **Headerless fallback**: when a cluster has only 1 single-span "label"
   row (e.g. "Armor *", "Table 12:"), look in the rows immediately below
   for the first column-aligned body-font row and use IT as the template

## Per-book onboarding workflow
1. Run `python scripts/table_extraction.py fonts <pages>` on a few known table pages
2. Read histogram, identify body style and table-header style
3. Add `table_detection:` block to per-book config
4. Run `python scripts/table_extraction.py detect` → check count test
5. Iterate config tolerances if needed (most should work with defaults)
6. Add to `exclude_chapters` any sections with table-like content that aren't real tables (Glossary, Index, etc.)

Default in `_default.yaml`: `table_detection.enabled: false`. Detector
returns immediately if not enabled — books without onboarding write empty
tables and don't crash.

## Phases (commit hash)

- ✅ **Phase 0**: Probes (`47d6ae6`) — proved find_tables() and minicpm-v dead, font-switch viable
- ✅ **Phase 1**: Detector (`176b6e1`, `c5aa216`) — `detect_table_regions`, 50/50 PHB
- ✅ **Phase 2**: Char-offset span maps (`8d19875`) — `extract_page_text_with_span_map`, `region_char_ranges`
- ✅ **Phase 3**: Bronze writes (`6b6bbbf`) — 3 new tables, `detect_all_regions` orchestrator wired
- ✅ **Phase 4**: Silver mask application (`e1f49e6`) — masks applied before cleanup, VLM strip skipped when masks present
- ✅ **Phase 5**: Validation (`9999d7b`) — `table_region_coverage` check fails run on UNDER pages
- ⏳ **Phase 6 (DEFERRED)**: Delete `_extract_table_vlm`, `strip_tables_from_markdown`, `_clean_table_pages_vlm`, fuzzy strategies in `extract_all_tables`, `table_hints` config. Held until real pipeline run validates the new path. Phase 6 also needs a sub-plan because it changes the `tables_raw` consumer in silver.

## What still needs to happen before Phase 6

1. Cache reset + Dagster restart (per CLAUDE.md rules)
2. Run `tabletop_full_pipeline` or `tabletop_without_enrichment` on PHB
3. Inspect `bronze_tabletop.table_regions`, `table_cells`, `page_text_masks` for sanity
4. Check `bronze_tabletop.validation_results` for the new `table_region_coverage` row (should be 'pass' for PHB)
5. Spot-check silver entries on previously-leaky pages (76 Proficiencies, 90 Equipment, 92 Armor, 121 THAC0s) to confirm masks blank table content without damaging surrounding prose
6. If everything looks good → Phase 6 cleanup
7. If anything's off → individual commits are revertable

## Probe results — PHB (2026-04-08)

### Font probe (font-switch viable)
- Body: `Formata-Light 9.5` across all 6 test pages (89-94% of chars)
- Tables: `Formata-Regular 9.5` (3-14% of chars) — same family/size, regular weight
- Headings: `UniversityRomanStd-Bold 16/20/40` (different font, no collision)
- Footnotes: `Formata-Light 7.0` (smaller size, distinguishable)

### VLM probe (DEAD with minicpm-v)
- 6 pages: 0/4 valid responses, 2 returned suspect single bboxes that don't match real tables
- minicpm-v ignores JSON instructions, hallucinates table contents, returns coords out of [0,1]
- Would need qwen2.5-vl or internvl. Not pursuing.

### find_tables() probe (DEAD)
- 28 false positives (artwork borders, sidebars), 0 of 47 real tables matched, 0 errors

### Detector progression on PHB
PHB has 69 declared ToC tables on 50 distinct pages. Counts below are
"matched_pages" (det >= toc per page), not raw table counts.

- v1 (raw bbox columns): 36 matched, 10 missing, 17 extra
- v2 (midpoint zones): 42 matched, 4 missing, 4 under, 17 extra
- v3 (column template = best-merged row): 47 matched, 1 missing, 2 under, 17 extra
- v4 (strict clustering, no sequential singletons): 49 matched, 1 missing, 0 under, 14 extra
- v5 (headerless fallback for label-only clusters): **50 matched, 0 missing, 0 under, 14 extra** ← FINAL (all 69 tables covered)

**Why:** Every prior attempt failed because it worked in markdown/text space
after Marker had destroyed the geometry signal. PyMuPDF holds the geometry —
use the (font, size) span signal directly, cache once, never call an LLM
during silver. PHB validates the approach. Per-book onboarding probe ensures
the same signal exists in each new book before enabling the detector.

**How to apply:** Don't touch the detector until after a real pipeline run
validates Phases 1-5 end-to-end. If validation passes, do Phase 6 cleanup.
If a new book is being onboarded, follow the per-book workflow and add a
`table_detection:` block to its config. Default behavior is detector
disabled, so unconfigured books are safe.
