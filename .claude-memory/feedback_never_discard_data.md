---
name: NEVER discard data during extraction
description: CRITICAL - Keep ALL information from source. Never throw away fields. Ask before discarding anything.
type: feedback
---

NEVER throw away information during extraction. The spell index has Name, Class (Pr/Wiz), Level, and Page — ALL of it must be captured and stored, not just the name.

**Why:** User has had to correct this multiple times. extract_known_entries was only storing spell names and throwing away class, level, and page number. That forced fragile workarounds (section-title parsing, page-anchor inference) to recover data that was already available in the source.

**How to apply:**
- When extracting from ANY source, capture EVERY field available
- Store raw extracted data in bronze with ALL columns
- Never reduce or simplify during extraction — that's for silver/gold to decide
- If unsure whether a field is needed, KEEP IT and ask the user
- This applies to indexes, metadata, ToC entries, everything
