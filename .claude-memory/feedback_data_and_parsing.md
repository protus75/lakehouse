---
name: Data handling, parsing, and content matching rules
description: Fix at ingestion, fuzzy matching, preserve casing, page-guided search, rejoin hyphens, chapter intros, ToC table ordering, browser gold-only, no regex for parsing
type: feedback
---

## Browser must NEVER access silver — gold only
The Dash browser app must only read from gold_tabletop namespace. Never include silver_tabletop in the reader or queries.

**Why:** Browser is a consumer — it reads the final gold layer only. Silver is an internal transform layer.
**How to apply:** Any browser code change must use `get_reader(namespaces=["gold_tabletop"])`. Never add silver tables or namespaces to the browser.

## Fix at ingestion, not export
Data quality issues belong in ingestion/parsing code, not export layer. Use config-driven patterns so fixes apply to all downstream consumers.

**Why:** Fixing in export means every consumer must re-apply the same fix.

## Always use fuzzy matching for content
User has explicitly corrected this multiple times. Always use rapidfuzz for matching section titles to PDF text. Handles OCR errors, line breaks mid-word, whitespace differences.

**Why:** PDF text has line breaks mid-title ("An Example\n of Play"), OCR errors ("Cnomes" for "Gnomes"), and whitespace variations. Exact matching misses these.

## Page-guided search for common-word sections
Titles like "Fighter", "Elves", "Healing" appear dozens of times before their actual heading. Use ToC page numbers to search the specific page first.

**How to apply:** When building entries from page_texts, search each section's ToC page first, then expand. Don't concatenate all chapter pages and search from the start.

## Clean up PDF line continuations
pymupdf text has hyphenated line breaks ("Constitu-\ntion"). Rejoin before matching or content storage. Normalize whitespace for fuzzy matching.

## Chapter intros exist
Content between chapter start and first sub-section is the chapter intro. Don't drop it — capture as a chapter-level entry.

## Preserve original casing — never lowercase stored data
Store original casing from source (PDF, config, tables). Only use `.lower()` for comparisons, dedup keys, lookups, sorting — never on stored data.

**Why:** Lowercasing destroys information. "Animal Lore" and "Fireball" are proper names. Browser and downstream consumers need correct casing.

## Prefer simple string ops over regex and old Unix text tools
Don't default to regex, grep patterns, sed, or awk for content parsing. Use simple string methods (`split`, `startswith`, `find`, `in`, `strip`) first — they're more readable, maintainable, and less brittle. Regex is only acceptable for truly atomic patterns (extracting a single number, matching a fixed format like a date). For anything semantic, use fuzzy matching, ML, or LLM classification.

**Why:** AI training data is saturated with regex/grep/sed solutions, creating a bias toward them even when simpler approaches work better. Regex encodes format assumptions that break on real data, is unreadable months later, and is the wrong abstraction level for content understanding. The CLAUDE.md rule exists because this is a known failure mode.
**How to apply:** When writing any text processing code, start with string methods. Only escalate to regex if the pattern is truly fixed-format and atomic. If you catch yourself writing a regex longer than ~20 chars, stop and use string ops or a parser instead.

## ToC sort order: sections only, not tables
Tables should not be interleaved between a section and its sub-sections in ToC ordering. Group tables after their parent section's sub-sections.

**Why:** Tables mixed between sections cause sort_order gaps that break page-guided section search. The entry builder uses sort_order to determine iteration order.
