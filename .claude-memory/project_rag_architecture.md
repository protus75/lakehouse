---
name: RAG architecture decisions
description: Key architecture, fix history, validation status for tabletop rules RAG system
type: project
---

## Current Architecture (as of 2026-03-23)

**Ingestion:** `dlt/load_tabletop_rules_docs.py`
- Page-position interpolation: finds unique text from each PDF page in markdown, builds anchor map, interpolates heading→page→ToC section. No text matching against ToC titles.
- Marker cache: `cache/marker/{stem}.md`
- Non-spell sections: every H3/H4 heading creates named entry. Spell sections: known_entries whitelist.
- Content cleanup: string-based (no regex), config-driven substitutions
- Flushed per-step timing logs

**Validation:** 3 scripts
- `validate_spells.py` — spell metadata checks
- `validate_sections.py` — section-level chunk distribution
- `validate_content.py` — config-driven expected entries/tables per section

## Validation Status (2026-03-23)
- Spells: 1 issue (Adjudicating Illusions false positive — not a spell)
- Content: 5 issues (Tables 13-15, 42-43 in Ch1 overflow, Table 45 fixed via substitution)
- Sections: 2 issues (Ch1 overflow from front matter, Ch13 empty 2-page chapter)

## Remaining Issues
1. **Ch1 overflow** — front matter pages (1-17) have no ToC section, pile into Ch1. Root cause of missing tables in Ch3/Ch6. Need front matter exclusion or section.
2. **Ch13 empty** — 2-page chapter, no page anchor found
3. **Adjudicating Illusions** — Notes on Spells section leaking into spell validation

## How to Run
- Ingest: `docker exec lakehouse-workspace python -u -c "from pathlib import Path; from dlt.load_tabletop_rules_docs import parse_pdf; parse_pdf(Path('/workspace/documents/tabletop_rules/raw/DnD2e Handbook Player.pdf'), game_system='D&D 2e', content_type='rules')"`
- Validate spells: `docker exec lakehouse-workspace python scripts/tabletop_rules/validate_spells.py`
- Validate sections: `docker exec lakehouse-workspace python scripts/tabletop_rules/validate_sections.py`
- Validate content: `docker exec lakehouse-workspace python scripts/tabletop_rules/validate_content.py`
- Marker cached after first run. Re-ingest: ~17s
