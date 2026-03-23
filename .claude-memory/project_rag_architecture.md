---
name: RAG architecture decisions
description: Key architecture decisions, current bugs, and fix history for tabletop rules RAG system
type: project
---

## Current Architecture (as of 2026-03-22)

**Ingestion:** `dlt/load_tabletop_rules_docs.py`
- ToC state machine: walks Marker headings sequentially, matches against ToC sections in order
- pymupdf: page number resolution only (targeted search within matched section's page range)
- Marker: full document markdown extraction (no page splitting — continuous document)
- Marker cache: `cache/marker/{stem}.md` — avoids re-running Marker on every ingestion (added 2026-03-22)
- ToC is first-class entity stored in DuckDB `toc` table
- Known entry names from excluded index sections act as heading whitelist
- Config-driven per book via YAML in `documents/tabletop_rules/configs/`

**Query:** `rag/query_tabletop_rules.py`
- Two-stage: LLM routes to ToC section(s), then search within section
- Multi-entity queries: each entity looked up independently
- Reranking boosts chunks with entry title match + stat block fields + length

## Key Files
- `dlt/load_tabletop_rules_docs.py` — ingestion pipeline
- `rag/query_tabletop_rules.py` — two-stage query engine
- `rag/embed_tabletop_rules.py` — ChromaDB embedding
- `rag/export.py` — markdown export with metadata tables
- `rag/api.py` — FastAPI + browse/chat web UI
- `documents/tabletop_rules/configs/` — per-book YAML configs
- `scripts/tabletop_rules/validate_spells.py` — validation (runs in docker)
- `scripts/tabletop_rules/debug_missing_desc.py` — debug script for tracing entry issues

## Resolved (2026-03-22)
- ToC state machine replaced brittle `build_heading_chapter_map`
- Content cleanup at ingestion: `_clean_entry_content` handles smashed metadata, leading spaces, image refs
- `_deduplicate_marker_blocks` removes duplicate metadata from page-boundary re-renders
- `_merge_orphan_entries` two-pass: group-merge same-title fragments + hungry/orphan recovery
- Index extraction fixed: only real entries with page numbers, strips level annotations
- Validation rewritten: per-entry not per-chunk (684 fake metadata errors → 5 real)
- Redundant cleanup removed from `export.py`

## Current Bug Being Fixed (2026-03-22)
**Duplicate section headings from Marker page breaks cause description loss**
- Marker re-renders section headings (e.g. `## Priest Spells`) at top of new pages
- `build_entries` sees H1/H2 heading → calls flush() → orphans the description text
- Affected entries: Insect Plague, Raise Dead, Stone Tell, Command (metadata only, no desc)
- Dust Devil: duplicate entry header (handled by merge pass 1)
- **Fix applied:** skip duplicate section headings in `build_entries` (line ~641)
- Ingestion re-running to verify fix

## Validation Status (pre-fix)
- 348 total issues reported
- 310 orphan_chunks — FALSE POSITIVES from chunking (second chunks naturally start lowercase)
- 21 missing_from_index — FALSE POSITIVES (known_entries has non-spell index items)
- 6 no_description — 4 REAL (Insect Plague, Raise Dead, Stone Tell, Command), 2 false positive
- 11 hyphenated_words — mix of OCR artifacts ("D- M") and legitimate text ("4th- and")
- Validation itself needs fixing after ingestion fix (orphan_chunks + missing_from_index checks are wrong)

## How to Run
- Ingestion: `docker exec lakehouse-workspace python -c "from dlt.load_tabletop_rules_docs import run; run(game_system='D&D 2e', content_type='rules')"`
- Validation: `docker exec lakehouse-workspace python scripts/tabletop_rules/validate_spells.py`
- Both run inside Docker container `lakehouse-workspace`
- Marker takes ~5-10 min first run, cached after that
- Re-ingestion with cache: ~30s
