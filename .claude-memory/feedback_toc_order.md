---
name: ToC sort order is sections only
description: Tables have no meaningful order in ToC — only chapters and sections. Don't interleave tables between a section and its sub-sections.
type: feedback
---

Tables in the ToC review YAML should not be interleaved between a section and its sub-sections. Only chapters and sections have meaningful ordering. Tables should be grouped separately (e.g., after their parent section's sub-sections).

**Why:** Tables mixed between sections cause sort_order gaps that break the page-guided section search. The entry builder uses sort_order to determine iteration order for matching section headings in page text.

**How to apply:** When editing toc_review YAML files, keep section entries contiguous. Group tables at the end of their parent section, not between sub-sections.
