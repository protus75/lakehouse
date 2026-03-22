---
name: RAG architecture decisions
description: Key architecture decisions for the tabletop rules RAG system
type: project
---

## Current Architecture (as of 2026-03-22)

**Ingestion:** `dlt/load_tabletop_rules_docs.py`
- pymupdf: page numbers → ToC chapter assignment (reliable)
- Marker: full document markdown extraction (no page splitting — continuous document)
- ToC is first-class entity stored in DuckDB `toc` table
- Known entry names from excluded index sections act as heading whitelist
- Config-driven per book via YAML in `documents/tabletop_rules/configs/`

**Query:** `rag/query_tabletop_rules.py`
- Two-stage: LLM routes to ToC section(s), then search within section
- Multi-entity queries: each entity looked up independently
- Reranking boosts chunks with entry title match + stat block fields + length

**NEXT TODO: Replace `build_heading_chapter_map` with ToC state machine**
- The ToC provides exact order and whitelist of headings
- Walk Marker markdown sequentially, advance through ToC entries
- No forward-search against pymupdf pages — that approach is brittle
- The ToC state machine knows where it is in the book and what heading comes next

## Key Files
- `dlt/load_tabletop_rules_docs.py` — ingestion pipeline
- `rag/query_tabletop_rules.py` — two-stage query engine
- `rag/embed_tabletop_rules.py` — ChromaDB embedding
- `rag/export.py` — markdown export with metadata tables
- `rag/api.py` — FastAPI + browse/chat web UI
- `documents/tabletop_rules/configs/` — per-book YAML configs
- `scripts/tabletop_rules/` — rebuild, validate, test scripts

## Export Issues Still Open
- Metadata sometimes smashed on one line (Marker formatting)
- Some entries missing descriptions (page boundary in Marker)
- Leading spaces from Marker
- Strip content patterns need to be applied consistently
