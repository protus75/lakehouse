---
name: project_content_quality
description: Gold layer content quality issues found 2026-03-26 — must fix before running enrichment
type: project
---

Content quality issues found when testing Streamlit UI against gold layer (2026-03-26):

**Problems identified:**
- Entries start mid-sentence (e.g. "Ability Scores" chunk begins with "low ability scores are not all that bad anyway!")
- Table data rendered as raw numbers without formatting (chunk 60: bare number columns)
- Pipe-formatted table fragments split across chunks (chunk 67: partial table rows)
- 291/2716 chunks have NULL entry_title — orphaned content not matched to any heading
- Entry list was alphabetically sorted instead of book order (fixed in streamlit, but gold_chunks ordering may need review)
- Duplicate entries in gold_ai_summaries from multiple enrichment restarts (dedup added to write_iceberg calls)

**Root cause:** Silver layer entry extraction (`silver_entries.py`) and gold chunking (`gold_chunks.py`) — heading detection cuts content incorrectly, tables get split mid-row during chunking.

**Impact:** Enrichment summaries based on bad content are unreliable. Must fix content quality before rerunning enrichment.

**Next steps:**
1. Audit silver_entries.py heading/boundary detection
2. Audit gold_chunks.py chunking logic — tables should not be split
3. Add validation tasks: content completeness, table integrity, entry boundary accuracy
4. Fix smushed inline tables/lists in silver cleanup — e.g. "Warrior 1d10 Priest 1d8 Rogue 1d6 Mage 1d4" should be table/list not one line
5. Fix false paragraph breaks from Marker OCR — e.g. "Everything a player needs to\n\nknow" should be one paragraph. Marker splits mid-sentence at PDF line breaks.
6. Fix issues, rerun dbt, verify in browser
7. Then rerun enrichment

**Why:** Garbage in, garbage out. Summaries from truncated/garbled content waste 4 hours of LLM time and produce misleading results.

**How to apply:** Fix silver/gold content quality before any enrichment runs. Add dbt tests or validation checks for content integrity.
