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

## 41/41 dbt tests passing

## What's Done
- Spell data validated across 4 appendix sources, zero gaps
- Spell crosscheck with rapidfuzz
- State machine for spell_class/level from ToC + sub-section headings
- Inline entry detection (config-driven)
- AI summaries + annotations populated (but stale)

## What Needs Work
1. Tables in silver/gold: tables_raw needs silver/gold models
2. 7 missed tables (T19, T21, T33, T40, T43, T53, T65) — Marker OCR edge cases
3. Proficiency validation: authority table whitelist for inline entries
4. Equipment validation: same pattern
5. Class validation: mixed with rules content
6. Re-run AI enrichment after silver/key changes

## Gold Enrichment Plans
- **AI Summaries:** concise LLM-generated summaries for spells, proficiencies, classes, rules
- **Cross-Reference Indices:** structured tags for DB queries ("all 3rd level wizard evocation spells")
- **AI Annotations:** combat (yes/no) + popular (yes/no) flags, LLM-classified

## Validation requirements
- 41/41 dbt tests must pass
- Rules data must be 100% accurate — this is a rules reference, not "best effort"
- Run ALL validators after every change, fix everything before committing
