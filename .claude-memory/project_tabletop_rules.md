---
name: Tabletop rules project — settings, status, and pipeline details
description: Tabletop-specific — PHB only, Marker PDF, Dagster jobs, enrichment, validation, config, pipeline status, gold enrichment plans
type: project
---

## Current focus: Player's Handbook (PHB) only
Process one book until validation passes before moving to others. Never run all 6 PDFs — wastes 30-45 min during debugging.

## PDF extraction: Marker
- Marker is the chosen PDF-to-markdown tool. Docling was tried and performed poorly — don't suggest switching.
- Always read printed page numbers from PDF text. Never calculate offsets — PDFs have unnumbered pages that break offset math.
- Fix quality issues at ingestion/cleanup layer, not by switching tools.

## Config-driven approach
- All thresholds, patterns, corrections in YAML configs
- Per-book configs: `documents/tabletop_rules/configs/`
- Default config: `_default.yaml`
- Authority tables define ground-truth entry names
- Entry anchors for entries Marker doesn't render as headings
- OCR corrections via `content_substitutions` in per-book config

## Dagster jobs
- `tabletop_full_pipeline` — full run including AI enrichment (~70 min)
- `tabletop_without_enrichment` — bronze → silver → gold → publish (no LLM, ~20s)
- Assets: `bronze_tabletop → dbt_tabletop → publish_to_iceberg → gold_ai_summaries / gold_ai_annotations`

## Enrichment workflow
- Summaries + annotations use `llama3:70b` (~42GB VRAM+RAM)
- Vision (OCR) uses `minicpm-v:latest`
- Unload models between passes: `POST /api/generate {"model":"<name>","keep_alive":0}`
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
- 100% level, 99.7% school, 100% sphere, 100% reversible
- silver_page_anchors, silver_toc_sections, silver_known_entries, silver_files

## Gold (`dbt/.../tabletop/gold/`)
- gold_entry_index: 438 spells with ZERO gaps
- gold_chunks: 2816 query-ready chunks
- gold_ai_summaries: 751 entries (stale)
- gold_ai_annotations: 495 combat/popular flags (stale)

## 42 pass, 8 fail (50 total tests including gold)

## What's Done
- Pipeline split: dbt_build → publish_to_iceberg → dbt_test (data on S3 even when tests fail)
- Test results on S3 at meta.dbt_test_results
- 69/69 ToC tables extracted (including unlabeled Weapons/Armor via config hints)
- Tables stripped from entry content — standalone data in silver_tables/gold_tables
- ToC truth used for extraction when toc_reviewed=true (reads from Iceberg, not re-extracting)
- OCR: 16→8 issues (garble stripped, hyphen rejoining, gibberish detection)
- Duplicate heading dedup (Marker page header repeats discarded)
- All uniqueness tests pass (toc_id, entry_id with page_start + content_prefix in hash)
- write_iceberg overwrite_all properly handles stale catalog
- Spell data validated across 4 appendix sources, zero gaps
- Spell crosscheck with rapidfuzz

## What Needs Work — CRITICAL: Entry builder rewrite
build_entries() must produce one silver record per ToC entry:
- **toc mode** (default): one entry per ToC sub-section
- **per_list mode**: one entry per spell (from spell_list_entries) or proficiency (from authority_table_entries)
- **per_anchor mode**: one entry per config anchor (races, etc.)

Current WIP has 151 entries (need 700+). Per_list spell matching not finding headings within chapter ranges. **Next approach: page-first splitting**
1. Split by page texts (pymupdf per-page, already in bronze)
2. Assign pages to chapters using ToC page ranges
3. Within each chapter, split into sub-sections/spells by heading match
4. Assemble multi-page entries by linking across page boundaries within chapter

entry_mode config added to DnD2e_Handbook_Player.yaml (per_list for spell sections, per_anchor for races).

## Other remaining work
1. Table cell quality (36 few_columns, 39 smushed) — Marker rendering issues
2. Browser app needs gold_tables rendering + ToC-driven content assembly
3. Re-run AI enrichment after entry builder rewrite
4. Dash app hosted at gamerules.ai via Cloudflare tunnel

## Validation requirements
- Zero errors before new features
- Rules data must be 100% accurate
- Rules data must be 100% accurate — this is a rules reference, not "best effort"
- Run ALL validators after every change, fix everything before committing
