---
name: Match working reference code, don't reinvent
description: When working code/scripts already exist that solve the problem correctly, copy that logic exactly — don't rewrite or "improve"
type: feedback
---

When a working reference exists (script, dump, query), match its logic exactly in the new code. Don't invent new approaches.

**Why:** The dump_gold_entries.py script had correct ORDER BY, correct coalesce for headings, correct depth handling. The browser should have copied that logic verbatim. Instead Claude wrote different logic (ORDER BY entry_id, no section headings, different rendering) and got it wrong.

**How to apply:** Before writing new code that renders/displays the same data, read existing scripts that already work with that data. Copy their queries, ordering, and display logic. If it works in one place, make it work the same way in the next place.
