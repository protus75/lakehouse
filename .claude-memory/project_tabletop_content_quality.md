---
name: project_content_quality
description: Content quality issues and gold layer features — status as of 2026-03-28
type: project
---

Content quality issues found while reviewing Dash browser (2026-03-26). Status updated 2026-03-28.

**Pipeline fixes — DONE:**
1. ~~Spurious chapter ref entries~~ — fixed
3. ~~Zero-chunk ToC entries~~ — fixed
4. ~~"(chapter N)" in headings~~ — fixed
5. ~~Spell school in content title~~ — fixed
11. ~~Raw HTML in content~~ — fixed

**Pipeline fixes — need spot-check in browser:**
2. Proficiency split by table headings — likely fixed, verify in browser
6. Duplicate heading at end of content body
7. False paragraph breaks from OCR
9. Markdown headings mid-content (should be inline bold)
10. Inline sub-headings too prominent
16. Entries start mid-sentence

**Table-specific issues — deferred:**
8. Smushed inline tables/lists
12. Mangled table data (smushed columns, missing columns)
13. Split table column headers
14. OCR garbled bullet lists

**Gold features:**
18. ~~gold_entries model~~ — done
19. Structured spell stats (Range, Components, Duration, Casting Time, Area of Effect, Saving Throw)
20. Unit-converted content (distances in 5ft squares)
21. ~~Stable integer keys~~ — done (hash-based entry_id, toc_id, chunk_id via dlt/lib/stable_keys.py)
22. ~~Re-run AI enrichment~~ — gold_entry_descriptions model + enrich_summaries.py updated (qwen3:30b-a3b, reads clean descriptions). Run needed.

**Browser toggles (after gold features):**
23. Combat Only / Popular Only filters
24. Spell Components toggle
25. Distances in squares toggle

**Priority:** Spot-check 2/6/7/9/10/16 in browser, then re-run enrichment (#22).

**Why:** Garbage in, garbage out. Fix content before enrichment.
**How to apply:** Verify in browser at http://localhost:8000. Use Dagster for pipeline runs.
