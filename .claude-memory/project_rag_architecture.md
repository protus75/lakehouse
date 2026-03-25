---
name: RAG architecture decisions
description: Medallion lakehouse architecture with AI enrichment for tabletop rules
type: project
---

## Architecture (2026-03-25)

### Medallion Pipeline
```
PDF → Bronze (dlt, 9s) → Silver+Gold (dbt, 5s) → AI Enrichment (Ollama, ~45min)
```

### Bronze (dlt, `dlt/bronze_tabletop_rules.py`)
- `bronze_tabletop.marker_extractions` — full Marker markdown
- `bronze_tabletop.page_texts` — pymupdf per-page with printed page numbers
- `bronze_tabletop.toc_raw` — ToC entries
- `bronze_tabletop.known_entries_raw` — index entry names
- `bronze_tabletop.watermarks` — detected repeated lines
- `bronze_tabletop.files` — PDF metadata + config hash

### Silver (dbt, `dbt/lakehouse_mvp/models/tabletop/silver/`)
- `silver_page_anchors` — page-position interpolation
- `silver_entries` — cleaned entries with chapter assignment (804 entries)
- `silver_toc_sections`, `silver_known_entries`, `silver_files`

### Gold (dbt + scripts)
- `gold_chunks` — 800-char chunked entries (2816 chunks)
- `gold_entry_index` — structured cross-ref (type, level, school, sphere, components)
- `gold_toc`, `gold_files`
- `gold_ai_summaries` — LLM summaries (751 entries, script-based)
- `gold_ai_annotations` — combat/popular flags (495 entries, 259 combat, 165 popular)

### Shared Library
- `dlt/lib/tabletop_cleanup.py` — pure functions used by both dbt models and scripts

### Enrichment Scripts (resumable, Ollama llama3:70b)
- `scripts/tabletop_rules/enrich_summaries.py`
- `scripts/tabletop_rules/enrich_annotations.py`

### Validation
- dbt: 36 tests passing (silver + gold)
- `scripts/tabletop_rules/validate_spells.py` — 0 issues
- `scripts/tabletop_rules/validate_sections.py` — 1 issue (Ch1 overflow)
- `scripts/tabletop_rules/validate_content.py` — 4 issues (missing tables from overflow)

### Compatibility
- `documents_tabletop_rules.*` views → gold/silver tables
- RAG query engine works unchanged via views

## How to Run
```bash
# Bronze extraction
docker exec lakehouse-workspace python -u dlt/bronze_tabletop_rules.py

# Silver + Gold (dbt)
docker exec lakehouse-workspace bash -c "cd /workspace/dbt/lakehouse_mvp && dbt build --select tabletop"

# AI Enrichment (requires Ollama running with llama3:70b)
docker exec lakehouse-workspace python -u scripts/tabletop_rules/enrich_summaries.py
docker exec lakehouse-workspace python -u scripts/tabletop_rules/enrich_annotations.py
```
