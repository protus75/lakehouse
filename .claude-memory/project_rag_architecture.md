---
name: RAG architecture and current work status
description: Medallion lakehouse, validation status, what's done and what needs work
type: project
---

## Architecture (2026-03-25)

### Pipeline
```
PDF → Bronze (dlt, ~12s) → Silver+Gold (dbt, ~6s) → AI Enrichment (Ollama, ~70min)
```

### Bronze (`dlt/bronze_tabletop_rules.py`)
- marker_extractions, page_texts, toc_raw, known_entries_raw (full metadata from App 5/6/7)
- spell_list_entries (App 1, with is_reversible from italic detection)
- authority_table_entries (WIP - config-driven table parsing)
- watermarks, files

### Silver (`dbt/lakehouse_mvp/models/tabletop/silver/`)
- silver_entries: cleaned entries with spell_class, spell_level, school, sphere
- silver_spell_crosscheck: fuzzy-matched across all 4 appendixes (rapidfuzz)
  100% level, 99.7% school, 100% sphere, 100% reversible, 0 real level mismatches
- silver_page_anchors, silver_toc_sections, silver_known_entries, silver_files

### Gold (`dbt/lakehouse_mvp/models/tabletop/gold/`)
- gold_entry_index: uses silver_spell_crosscheck as authority for spell data
  438 spells with ZERO gaps on level/school/sphere/reversible
- gold_chunks: 2816 query-ready chunks
- gold_toc, gold_files
- gold_ai_summaries: 751 entries (stale - needs re-run after silver rebuild)
- gold_ai_annotations: 495 combat/popular flags (stale)

### Shared Library: `dlt/lib/tabletop_cleanup.py`
### Config: `documents/tabletop_rules/configs/`
### 41/41 dbt tests passing

## What's Done
- Spell data: fully validated across 4 appendix sources, zero gaps
- Spell crosscheck: fuzzy matching with rapidfuzz
- State machine: spell_class/level from ToC + sub-section headings
- Inline entry detection: config-driven for "Name: description" format
- AI summaries + annotations: populated but stale (entry_ids changed)

## What Needs Work (in order)
1. **Authority table parsing**: extract proficiency/equipment/skill names from Tables 37/44/36
   - Config lists which tables are authoritative for which entry type
   - Parser needs to use page numbers to locate tables in markdown
   - Currently WIP — parser finds wrong table (ToC reference instead of actual)
2. **Proficiency validation**: individual proficiency entries not being created
   - inline_entry_pattern detects "Name: description" but needs the authority table whitelist
3. **Equipment validation**: same pattern as proficiencies
4. **Class validation**: class entries exist but mixed with rules content
5. **Re-run AI enrichment**: summaries + annotations stale after silver changes
6. **Monolith cleanup**: old load_tabletop_rules_docs.py still exists alongside new pipeline

## How to Run
```bash
docker exec lakehouse-workspace python -u dlt/bronze_tabletop_rules.py
docker exec lakehouse-workspace bash -c "cd /workspace/dbt/lakehouse_mvp && dbt build --select tabletop"
docker exec lakehouse-workspace python -u scripts/tabletop_rules/enrich_summaries.py
docker exec lakehouse-workspace python -u scripts/tabletop_rules/enrich_annotations.py
```
