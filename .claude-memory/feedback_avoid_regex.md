---
name: STOP using regex - use string ops, Marker headings, or LLM
description: CRITICAL - Regex causes catastrophic backtracking and wastes hours debugging. Use string operations, Marker ML, or LLM instead.
type: feedback
---

STOP using regex for content parsing. Regex has repeatedly caused:
- Catastrophic backtracking (parse_toc taking 52s+ on 15 pages due to `.*` + dot-leader pattern)
- Unmaintainable patterns that break across books
- Hours of debugging wasted on regex bugs across 20+ ingestion reruns

Use instead:
1. **String operations** — split, startswith, endswith, find, `in` — for simple matching
2. **Marker ML headings** — Marker already detects headings with `#` markers, use those directly
3. **LLM** — for complex content classification that regex can't handle reliably
4. **Config-driven user patterns** — ONLY when user explicitly defines them for their books

**Why:** User has corrected this many times. parse_toc regex caused a 52s hang scanning 15 pages. `.*` combined with `(?:\.[\s.]*){2,}` causes exponential backtracking. Every complex regex introduced has eventually caused a bug or performance issue.

**How to apply:** Before writing ANY regex, ask: can this be done with string split/find/startswith? If yes, use that. Refactor existing regex to string ops wherever possible. Only use regex for truly atomic patterns (extracting a number, matching a fixed format), never for greedy content matching with `.*`.
