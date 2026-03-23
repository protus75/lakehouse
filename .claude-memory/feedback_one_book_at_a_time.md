---
name: Only process one book until it works
description: Don't run ingestion on all PDFs - fix one book first, then expand
type: feedback
---

Only process the single example book (DnD2e Handbook Player.pdf) until validation passes with zero errors. Never run all 6 PDFs until the one book works perfectly.

**Why:** Ingestion takes 5-10 min per book through Marker. Running all 6 wastes 30-45 min when only one matters during debugging.
**How to apply:** Always pass the specific file path, not the directory. Use `parse_pdf(Path("...Player.pdf"))` not `run()`.
