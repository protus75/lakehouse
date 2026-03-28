---
name: Content matching rules
description: Rules for matching section titles to PDF text — no exact matches, prefer page-guided search, use fuzzy
type: feedback
---

## No exact string matching for content parsing
User has explicitly corrected this multiple times. Always use fuzzy matching (rapidfuzz).
Handles OCR errors, line breaks mid-word, whitespace differences.

**Why:** PDF text has line breaks mid-title ("An Example\n of Play"), OCR errors ("Cnomes" for "Gnomes"), and whitespace variations. Exact matching misses these.

## Common-word sections need page-guided search
Titles like "Fighter", "Elves", "Healing" appear dozens of times in body text before
their actual section heading. Sequential first-match grabs the wrong one.

**Fix:** Use ToC page numbers to search each section's specific page first. "Elves" on
page 28 means search page 28's text, not the whole chapter. The title at the start of
a paragraph on its ToC page IS the section heading.

**How to apply:** When building entries from page_texts, search each section's ToC page first, then expand for content. Don't concatenate all chapter pages and search from the start.

## Clean up PDF line continuations before matching
pymupdf text has hyphenated line breaks ("Constitu-\ntion"). Rejoin before matching
or content storage. Also normalize whitespace for fuzzy matching.

## Chapter intros exist
Content between the chapter start and the first sub-section is the chapter intro.
Don't drop it — capture as a chapter-level entry.
