---
name: project_spell_entry_type
description: gold_entry_index needs entry_type='spell' for spell entries — currently all entries are 'rule'
type: project
---

The `gold_entry_index` table has `entry_type` column but all entries are labeled `'rule'` — spells are not distinguished. Spell entries should have `entry_type='spell'`.

**Why:** The browser needs to know which entries are spells for the AI Summary toggle (swap summary vs full content). Currently using a workaround: checking if `toc_title` contains "Level Spells". Proper fix is to set `entry_type='spell'` in the gold model so downstream consumers don't need heuristics.

**How to apply:** Update the gold_entry_index dbt model to label spell entries based on their toc section. The `spell_level` and `spell_class` columns in `gold_entries` are populated but `gold_entry_index.entry_type` is not derived from them.
