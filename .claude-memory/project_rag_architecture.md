---
name: RAG architecture decisions
description: Key architecture, validation status, remaining issues for tabletop rules RAG system
type: project
---

## Current Architecture (as of 2026-03-23)

**Ingestion:** `dlt/load_tabletop_rules_docs.py` (~19s with Marker cache)
- Page-position anchors: unique text from each PDF page located in markdown (291/322)
- Front matter guard: content before first anchor excluded
- Fallback anchor lengths: config-driven [40, 30, 20] char snippets
- `whitelist_sections` config: explicit sections using known_entries filter
- Non-whitelist sections: every H3/H4 heading → named entry
- Content cleanup: string-based, config-driven substitutions
- Marker cache: `cache/marker/{stem}.md`

**Validation:** 3 scripts, all in `scripts/tabletop_rules/`
- `validate_spells.py` — uses `whitelist_sections` config
- `validate_sections.py` — chunk distribution per ToC section
- `validate_content.py` — config-driven expected entries/tables

## Validation Status (2026-03-23)
- **Spells: 0 issues** (431 entries)
- **Sections: 17/17 populated**, 1 overflow (Ch1, 170 chunks for 8 pages)
- **Content: 4 issues** (Tables 14, 15, 42, 43 in wrong section from bad anchors)

## Remaining Issues
- Ch1 overflow: bad page anchors mapping pages 36/42/53/129/308/311 into Ch1
- 4 missing tables: downstream of Ch1 overflow
- Root cause: text snippets from some pages match at wrong positions in markdown

## Key Config Settings
- `whitelist_sections`: sections that filter H3/H4 headings against known_entries
- `ingestion.anchor_snippet_lengths`: [40, 30, 20] for multi-pass anchor matching
- `validation.section_content`: expected entries/tables per section
- `content_substitutions`: OCR artifact fixes

## How to Run
- Ingest one book: `docker exec lakehouse-workspace python -u -c "from pathlib import Path; from dlt.load_tabletop_rules_docs import parse_pdf; parse_pdf(Path('/workspace/documents/tabletop_rules/raw/BOOK.pdf'), game_system='...', content_type='rules')"`
- All 3 validators: `docker exec lakehouse-workspace python scripts/tabletop_rules/validate_spells.py && docker exec lakehouse-workspace python scripts/tabletop_rules/validate_content.py && docker exec lakehouse-workspace python scripts/tabletop_rules/validate_sections.py`
