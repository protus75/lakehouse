---
name: project_content_quality
description: Content quality issues and gold layer features — must fix before running enrichment
type: project
---

Content quality issues found while reviewing Dash browser (2026-03-26):

**Pipeline fixes needed (silver/bronze):**
1. Silver heading detection creates spurious entries from chapter refs in content — `### Chapter 3`, `### **Wizard Spells**` become entry titles. Dozens of bad entries, some assigned to wrong chapters.
2. Proficiency entries badly split — Table 38 content injected mid-entry (Ancient History), then entry continues after. Heading detection splits entries at table headings found in content.
3. ToC entries like "The Goal", "Required Materials" have 0 chunks — heading not recognized due to Marker line-splitting ("The\nGoal").
4. Entry content includes "(chapter N)" in headings — e.g. "Step 2: Choose a Race (chapter 2)". Strip in silver cleanup.
5. Spell school in content title inconsistent — some have "(Conjuration/Summoning)" in heading, others don't. Already in gold_entry_index. Strip from content via strip_content_patterns config.
6. Entry content repeats the heading title at end (duplicate title in content body).
7. False paragraph breaks from Marker OCR — "Everything a player needs to\n\nknow" should be one paragraph.
8. Smushed inline tables/lists — "Warrior 1d10 Priest 1d8 Rogue 1d6 Mage 1d4" should be structured.
9. Random large bold text mid-content — Marker renders inline terms as markdown headings. Normalize to inline bold.
10. Inline sub-headings (e.g. "Good.", "Evil.") render too prominently. Normalize heading markers within entry content.
11. Raw HTML in content — `<b>-</b>` in table cells. Add `<b>`, `<i>` to HTML stripping in tabletop_cleanup.py.
12. Table data mangled two ways: (a) columns smushed ("BardLevel1234567891011121314151617181920"), (b) whole columns missing. Use bronze tables_raw or re-extract with vision model.
13. Table column headers split across cells — "WeProfic eaponencies" instead of "Weapon Proficiencies".
14. OCR garbled bullet lists — Riding Airborne has "t -FBQPOUPUIFTBEEMFPGUIFDSFBUVSF" gibberish. Needs re-OCR with vision model or content_substitutions.
15. Intro/meta sections classified as "rule" with NaN spell_level — entry_type classification too broad.
16. Entries start mid-sentence — heading detection cuts at wrong boundary.
17. NULL entry_title chunks — orphaned content not matched to headings.

**Gold layer features needed:**
18. `gold_entries` model — full entry content (no chunk overlap) for browser. Currently browser reads silver_entries which breaks architecture.
19. Structured spell stats — extract Range, Components, Duration, Casting Time, Area of Effect, Saving Throw as columns in gold_entry_index or new gold_spell_stats. Browser renders as key-value block. Spell Components toggle hides Components field and material component text.
20. Unit-converted content — distances in squares (5ft grid). Store as alternate content field in gold. Browser toggle switches between original and converted.
21. Stable integer keys — see project_integer_keys_plan.md. Must implement before re-running enrichment.
22. AI summaries linked to wrong entries due to unstable IDs. Re-run enrichment after key migration.

**Browser toggles (after gold features):**
23. Combat Only / Popular Only filters from gold_ai_annotations.
24. Spell Components toggle hides Components stat + material component sentences.
25. Distances in squares toggle switches to unit-converted content.

**Priority order:** Fix items 1-6 first (worst content issues), then 18+21 (gold_entries + stable keys), then re-run enrichment, then remaining items.

**Why:** Garbage in, garbage out. Summaries from truncated/garbled content waste 4 hours of LLM time and produce misleading results.
**How to apply:** Fix silver/gold content quality before any enrichment runs. Use Dagster for all pipeline runs. Verify in browser at http://localhost:8000.
