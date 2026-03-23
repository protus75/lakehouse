---
name: RAG architecture decisions
description: Key architecture, fix history, validation status for tabletop rules RAG system
type: project
---

## Current Architecture (as of 2026-03-23)

**Ingestion:** `dlt/load_tabletop_rules_docs.py`
- ToC state machine: walks Marker headings sequentially, matches against ToC sections in order
- pymupdf: page number resolution only (targeted search within matched section's page range)
- Marker: full document markdown extraction (no page splitting — continuous document)
- Marker cache: `cache/marker/{stem}.md` — skips 10-15 min Marker on re-ingest
- ToC parsing: string-based `_extract_toc_line` (no regex — old regex caused 52s backtracking)
- Known entries from excluded index sections act as heading whitelist
- Config-driven per book via YAML in `documents/tabletop_rules/configs/`
- All thresholds in config (`ingestion` and `validation` sections in `_default.yaml`)
- Flushed per-step timing logs for monitoring (`_log()`, step timers in `parse_pdf`)
- Content cleanup: string-based smashed metadata splitting (no regex)
- Content substitutions: config-driven OCR artifact fixes (e.g. "D- M" → "DM")

**Query:** `rag/query_tabletop_rules.py`
- Two-stage: LLM routes to ToC section(s), then search within section
- Multi-entity queries: each entity looked up independently

## Key Files
- `dlt/load_tabletop_rules_docs.py` — ingestion pipeline
- `documents/tabletop_rules/configs/_default.yaml` — all thresholds and defaults
- `documents/tabletop_rules/configs/{BookName}.yaml` — per-book overrides
- `scripts/tabletop_rules/validate_spells.py` — validation (runs in docker)
- `rag/query_tabletop_rules.py` — two-stage query engine
- `rag/embed_tabletop_rules.py` — ChromaDB embedding
- `rag/export.py` — markdown export
- `rag/api.py` — FastAPI + browse/chat web UI

## Resolved (2026-03-23)
- Duplicate section headings from Marker page breaks → skip in `build_entries`
- Section boundary mid-entry (Command spell) → don't flush hungry entries on section change
- Smashed metadata (Demishadow Monsters) → string-based split replaces regex
- 52s ToC parse hang → `_extract_toc_line` string ops replaces catastrophic regex
- 310 false positive orphan chunks → removed broken validation check
- 21 false positive missing_from_index → removed (known_entries has non-spell items)
- 11 hyphenated words → config-driven exclusion + "D- M"→"DM" content substitution
- All hardcoded values → moved to YAML config (`ingestion` + `validation` sections)
- Buffered output → `flush=True` on all prints, per-step timing in `parse_pdf`
- Marker cache added → re-ingest in ~18s instead of 10-15 min

## Validation Status (2026-03-23)
- **0 issues** across 434 spell entries, 1393 chunks
- DnD2e Handbook Player.pdf passes clean

## How to Run (single book only until it passes!)
- Ingest one book: `docker exec lakehouse-workspace python -u -c "from pathlib import Path; from dlt.load_tabletop_rules_docs import parse_pdf; parse_pdf(Path('/workspace/documents/tabletop_rules/raw/DnD2e Handbook Player.pdf'), game_system='D&D 2e', content_type='rules')"`
- Validate: `docker exec lakehouse-workspace python scripts/tabletop_rules/validate_spells.py`
- Both run inside Docker container `lakehouse-workspace`
- Marker ~10 min first run, cached after. Re-ingest with cache: ~18s
- Use `-u` flag for unbuffered output monitoring
