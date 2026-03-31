---
name: Table extraction — WIP, needs proper planning
description: Table extraction and stripping from entries is half-done and hacky. Needs a clean plan before continuing.
type: project
---

## Status: WIP — done poorly, needs replanning (2026-03-30)

AI rushed implementation without proper planning. Current state is hacky and half-working.

### What works
- 69 table entries created from tables_raw rendered as markdown pipe format
- entry_type='table' in gold_entry_index (is_table flag from ToC)
- VLM fallback (minicpm-v) for tables that fail Marker pipe extraction — wired but untested at scale
- query_iceberg.py improved with --where, --full, --truncate

### What doesn't work
- **Table content still leaks into surrounding entries** — the main unsolved problem
- LLM line classification approach (qwen3) is too slow (~20-30 min) and runs every silver rebuild
- minicpm-v doesn't reliably return JSON for line identification
- 8 toc_coverage regressions where LLM strips section headings near tables

### Two separate problems
1. **Extract table content** — VLM (minicpm-v) sees the page image, extracts structured table data. This is bronze-level.
2. **Strip table data from entry prose** — text LLM (qwen3) classifies lines as table vs prose. Also bronze-level — results must be cached.

### Next steps — need proper plan
- Move LLM line classification to bronze as `table_line_masks` Iceberg table
- Compute once during bronze extraction, cache result
- Silver reads cached masks instantly — no LLM call during silver rebuild
- Handle failures: pages with no mask keep original text, can re-run failed pages later
- The whole approach needs a clean plan — AI kept hacking instead of planning

**Why:** AI tried 4 different approaches in one session (fuzzy match → VLM page replacement → VLM line numbers → qwen3 line numbers) without properly planning any of them. Each one caused regressions. Need to step back and design properly.

**How to apply:** Before touching this again, plan the full approach end-to-end. Consider: what runs when, what's cached where, what happens on failure, how long each step takes, and how to iterate without 20-min feedback loops.
