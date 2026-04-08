---
name: Table extraction — font-switch detector plan
description: Per-book font-switch table detection plan. Approved 2026-04-08 after PHB probe validation.
type: project
---

## Status: PLANNED — ready to implement (2026-04-08, PHB first)

Replaces every prior attempt (fuzzy match, VLM page, VLM line numbers, qwen3, PyMuPDF find_tables). Probes proved:
- `page.find_tables()` detects 0/47 ToC tables on PHB. Dead.
- minicpm-v cannot return bbox coordinates (ignores JSON instructions, hallucinates). Dead unless we set up qwen2.5-vl.
- **PyMuPDF font/size span data IS reliable.** PHB body=`Formata-Light 9.5` (89-94% chars), table headers=`Formata-Regular 9.5` (same family/size, regular weight not light). Headings use a different font (`UniversityRomanStd-Bold`) so no false positives. Verified on pages 19, 27, 76, 90, 94, 121.

## Hard constraints (locked)
- No text matching of ToC titles against page content (abbreviations, plurals, line feeds break it).
- No PyMuPDF `find_tables()` (proven dead).
- ToC page numbers ARE reliable.
- Tables interleave with prose within sections; pages have 2-column body text; tables may occupy 1 column or both.
- Per-region bboxes mandatory. Char-offset masks (not line-index). Silver must leave surrounding prose char-by-char intact.
- Count test = lower bound only: `detected_regions_per_page >= toc_is_table_count_per_page` (mid-section unlabeled tables legitimately exceed ToC count).

## Detection signal: font-switch (PHB-validated)
For text-content tables (e.g. PHB Table 37 proficiencies), header AND cells use Formata-Regular.
For numeric tables (e.g. PHB Weapons p94), only the HEADER row uses Formata-Regular; data cells use the body font. Algorithm must extend the bbox down from header rows by tracing column alignment.

## Per-book onboarding workflow (Phase 0)
**Every new book requires probing before table extraction is enabled.** Detector is config-driven, no book-specific code branches.

1. Run `scripts/probe_pymupdf_fonts.py <book>` on a few known table pages.
2. Read histogram, identify body style and table-header style.
3. Write `table_detection` block in per-book config:
   ```yaml
   table_detection:
     enabled: true
     body_font: Formata-Light
     body_size: 9.5
     header_font: Formata-Regular
     header_size: 9.5
     header_bold: false
     # null = use column-alignment trace from header for data rows
     cell_font: null
     min_columns: 2
     min_rows: 2
   ```
4. Run `scripts/probe_table_regions.py <book>` to verify count test passes.
5. If font-switch fails for this book: fall back to manual per-page bbox overrides or wait for proper VLM (qwen2.5-vl).

Default in `_default.yaml`: `table_detection.enabled: false`. New books crash-free until onboarded.

## Implementation phases

### Phase 0 — onboarding tooling (DONE for PHB)
- `scripts/probe_pymupdf_fonts.py` — font histogram + non-body span dump
- `scripts/probe_vlm_bbox.py` — VLM bbox probe (proves whichever model is broken)
- `scripts/check_versions.py` — workspace package versions
- `scripts/probe_table_regions.py` — Phase 1 detector smoke test against ToC counts

### Phase 1 — bronze: `extract_table_regions(page, cfg)` config-driven
1. Walk `page.get_text("dict")` spans, build histogram.
2. Identify Formata-Regular spans (or whatever cfg says is the header font).
3. Cluster header spans by spatial adjacency: same y0 (within ~3pt) = one row, contiguous y0 rows + same x-range = one cluster.
4. For each cluster, extract column x0 positions from the first row.
5. Trace down: include lines below where ≥2 words sit at matching column x0s. Stop when alignment breaks for 2+ lines or another header cluster starts.
6. Output `[{bbox, span_indices, row_count, col_count}, ...]`.

### Phase 2 — bronze: `extract_page_text_with_span_map(page)`
Custom span assembler replaces `page.get_text()`:
1. Walk dict in pymupdf reading order (left col first then right col).
2. Build text string char-by-char, recording `(char_start, char_end, span_bbox)` per span.
3. Returns `(text, span_map)`.
4. **Verify text is byte-equivalent (or close enough)** to default `get_text()` so silver string matching doesn't regress. Diff against existing `bronze_tabletop.page_texts` content as the test.

### Phase 3 — bronze: cells + masks + new tables
- For each region: assign each word to (row, col), build `bronze_tabletop.table_cells`
- For each masked span: write char range to `bronze_tabletop.page_text_masks`
- Match regions to ToC `is_table` entries by page proximity → populate `tables_raw`
- New tables:
  ```
  bronze_tabletop.table_regions(source_file, page_index, region_index,
    bbox_x0/y0/x1/y1, detection_method, row_count, col_count,
    validation_issues, run_id, extracted_at)
  bronze_tabletop.table_cells(source_file, page_index, region_index,
    row_index, col_index, cell_text, run_id)
  bronze_tabletop.page_text_masks(source_file, page_index,
    char_offset_start, char_offset_end, reason, run_id)
  ```

### Phase 4 — silver: replace `_clean_table_pages_vlm` with `_apply_page_masks`
Reads `page_text_masks`, blanks char ranges in `clean_pages` text. Deterministic, sub-second. Delete VLM strip code.

### Phase 5 — bronze validation: per-page count test
`detected_regions_per_page >= toc_is_table_count_per_page`. Mismatches surface in `bronze_run_issues`. Per-book config can record expected counts for known unlabeled tables (Weapons, Armor for PHB).

### Phase 6 — delete dead code
- `_extract_table_vlm` (whole-page VLM)
- `strip_tables_from_markdown` (markdown string stripping)
- `_clean_table_pages_vlm` (silver-time VLM line stripping)
- Strategy 1/2/3 fuzzy title matching in `extract_all_tables`
- `table_hints` config section

## What gets deleted (final)
- `dlt/bronze_tabletop_rules.py`: `_extract_table_vlm`, `strip_tables_from_markdown`, fuzzy matching
- `dlt/lib/tabletop_cleanup.py`: `_clean_table_pages_vlm`
- Per-book config: `table_hints`

## Probe results — PHB (2026-04-08)

### Font probe (font-switch viable)
- Body: Formata-Light 9.5 across all 6 test pages (89-94% of chars)
- Tables: Formata-Regular 9.5 (3-14% of chars) — same family/size, regular weight
- Headings: UniversityRomanStd-Bold 16/20/40 (different font, no collision)
- Footnotes: Formata-Light 7.0 (smaller size, distinguishable)
- Page 19/76/94/121 all detected table headers cleanly via font-switch

### VLM probe (DEAD with minicpm-v)
- 6 pages: 0/4 valid responses, 2 returned suspect single bboxes that don't match real tables
- minicpm-v ignores JSON instructions, hallucinates table contents, returns coords out of [0,1]
- Would need qwen2.5-vl or internvl. Not pursuing now.

### find_tables() probe (DEAD)
- 28 false positives (artwork borders, sidebars), 0 of 47 real tables matched, 0 errors

**Why:** Every prior attempt failed because it worked in markdown/text space after Marker had destroyed the geometry signal. PyMuPDF holds the geometry — use the (font, size) span signal directly, cache once, never call an LLM during silver. PHB validates the approach. Per-book onboarding probe ensures the same signal exists in each new book before enabling the detector.

**How to apply:** Implement phase 0→6 in order. PHB only until count test is clean. New books require Phase 0 probe + config block before bronze table extraction is enabled. Detector code is single-implementation, fully config-driven.
