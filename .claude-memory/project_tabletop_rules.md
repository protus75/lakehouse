---
name: Tabletop rules project — settings, status, and pipeline details
description: Tabletop-specific — PHB only, pymupdf page_texts for content, Dagster jobs, enrichment, pipeline status
type: project
---

## Current focus: Player's Handbook (PHB) only
Process one book until validation passes before moving to others. Never run all 6 PDFs — wastes 30-45 min during debugging.

## PDF extraction: pymupdf (primary) + Marker (tables/formatting)
- **pymupdf page_texts** is the primary content source — has clean text for every page
- **Marker drops whole pages** (chapter openers with decorative layouts) — confirmed pages 10, 18, and ~30 others missing from Marker output. pymupdf has them all.
- Marker still useful for: table extraction, markdown formatting hints
- Always read printed page numbers from PDF text. Never calculate offsets.
- Fix quality issues at ingestion/cleanup layer, not by switching tools.

## Config-driven approach
- All thresholds, patterns, corrections in YAML configs
- Per-book configs: `documents/tabletop_rules/configs/`
- Default config: `_default.yaml`
- Authority tables define ground-truth entry names
- OCR corrections via `content_substitutions` in per-book config

## Dagster jobs
- `tabletop_full_pipeline` — full run including AI enrichment (~70 min)
- `tabletop_without_enrichment` — bronze → silver → gold → publish (no LLM, ~20s)
- Assets: `bronze_tabletop → dbt_tabletop → publish_to_iceberg → gold_ai_summaries / gold_ai_annotations`

## Enrichment workflow
- Summaries + annotations use `llama3:70b` (~42GB VRAM+RAM)
- Vision (OCR) uses `minicpm-v:latest`
- AI summaries + annotations are stale — need re-run after stable keys migration

## Pipeline
```
PDF → Bronze (dlt, ~12s) → Silver+Gold (dbt, ~6s) → Publish (Iceberg) → AI Enrichment (Ollama, ~70min)
```

## Bronze (`dlt/bronze_tabletop_rules.py`)
- marker_extractions, page_texts, toc_raw, known_entries_raw, spell_list_entries
- tables_raw: 50/57 tables for PHB (702 rows), 7 missed are Marker OCR edge cases
- authority_table_entries: 235 entries (87 proficiency, 153 equipment, 24 secondary skills)
- watermarks, files

## Silver (`dbt/.../tabletop/silver/`)
- silver_entries, silver_spell_crosscheck (rapidfuzz across 4 appendixes)
- silver_page_anchors, silver_toc_sections, silver_known_entries, silver_files

## Gold (`dbt/.../tabletop/gold/`)
- gold_entry_index: 438 spells with ZERO gaps
- gold_chunks, gold_ai_summaries (stale), gold_ai_annotations (stale)

## What's Done
- Pipeline split: dbt_build → publish_to_iceberg → dbt_test
- 69/69 ToC tables extracted
- ToC truth used for extraction when toc_reviewed=true
- Spell crosscheck with rapidfuzz, zero gaps
- `build_entries_from_pages()` in tabletop_cleanup.py — pymupdf-based entry builder
- `build_entries_from_stream()` — Marker-based (legacy, kept for reference)
- Hyphen rejoining for pymupdf text (`_rejoin_page_text`)
- Extended ToC with spells injected by level (alphabetical)

## Entry builder status — WIP
`build_entries_from_pages()` uses pymupdf page_texts + ToC page ranges:
- 199/206 non-spell ToC sections matched with normalized fuzzy matching
- **Problem: common-word sections match wrong occurrence** (e.g., "Elves" matches
  a mention in Dwarves text instead of the actual Elves heading on page 28)
- The current approach concatenates all chapter pages then searches — loses page boundaries
- **Next approach: search each section's specific ToC page FIRST.** The ToC says "Elves"
  starts on page 28, so search page 28's text for "Elves" at a paragraph start.
  Then expand to adjacent pages for the content body.
- For sections sharing a page (same page_start), find titles in order within that page.
- Chapter intro text (before first sub-section) now captured separately.
- Spells need work — only 36 matched (need ~487). Same common-word problem.
- Tables skipped for now — decide later whether pymupdf or Marker has better tables.

## Other remaining work
1. Table cell quality (36 few_columns, 39 smushed) — Marker rendering issues
2. Browser app needs gold_tables rendering + ToC-driven content assembly
3. Re-run AI enrichment after entry builder rewrite
4. Dash app hosted at gamerules.ai via Cloudflare tunnel

## Validation requirements
- Zero errors before new features
- Rules data must be 100% accurate — this is a rules reference, not "best effort"
- Run ALL validators after every change, fix everything before committing
