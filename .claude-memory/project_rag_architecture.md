---
name: RAG architecture decisions
description: Key architecture decisions for the tabletop rules RAG system
type: project
---

## Current Architecture (as of 2026-03-22)

**Ingestion:** `dlt/load_tabletop_rules_docs.py`
- ToC state machine: walks Marker headings sequentially, matches against ToC sections in order
- pymupdf: page number resolution only (targeted search within matched section's page range)
- Marker: full document markdown extraction (no page splitting — continuous document)
- ToC is first-class entity stored in DuckDB `toc` table
- Known entry names from excluded index sections act as heading whitelist
- Config-driven per book via YAML in `documents/tabletop_rules/configs/`
- `build_page_chapter_map` removed — chapter assignment comes from ToC order, not page lookup

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
- `scripts/tabletop_rules/` — rebuild, validate, test scripts

## Resolved (2026-03-22)
- ToC state machine replaced brittle `build_heading_chapter_map` (no more page text search)
- `build_page_chapter_map` removed — chapter assignment from ToC order
- Content cleanup at ingestion: `_clean_entry_content` handles smashed metadata, leading spaces, image refs
- `_deduplicate_marker_blocks` removes duplicate metadata from page-boundary re-renders
- `_merge_orphan_entries` two-pass: group-merge same-title fragments + hungry/orphan recovery
- Index extraction fixed: only real entries with page numbers, strips level annotations
- Validation rewritten: per-entry not per-chunk (684 fake metadata errors → 5 real)
- Redundant cleanup removed from `export.py`

## Remaining (Marker OCR quality)
- 310/430 spell entries have orphan chunks (mid-word page breaks in Marker output)
- 5 entries missing Components: field, 6 entries missing description
- These are Marker text extraction issues, not parsing issues
