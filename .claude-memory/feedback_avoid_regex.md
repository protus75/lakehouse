---
name: Minimize regex in parsing
description: Use Marker ML headings and LLM classification instead of brittle regex patterns
type: feedback
---

Avoid regex for content parsing. Use it ONLY for:
- ToC page number extraction (well-defined format)
- Watermark detection (statistical frequency, not pattern matching)
- Config-driven strip patterns that the USER defines for their books

For everything else use:
- Marker's ML-based heading detection (# ## ### from layout analysis)
- Known entries whitelist from the book's index sections
- LLM classification for ambiguous cases

**Why:** Regex patterns for heading detection, metadata extraction, and content cleanup were the root cause of dozens of painful iterations. Every regex fix broke something else. Marker already solves the hard visual parsing problems (columns, tables, headings).

**How to apply:** When tempted to add a regex to detect or classify content, ask: can Marker's output or the known entries list handle this? If yes, use those. Only reach for regex as a last resort, and put it in the config so the user controls it.
