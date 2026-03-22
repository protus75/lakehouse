---
name: Fix ingestion not export
description: Always fix data quality issues at ingestion time with config, never patch in export
type: feedback
---

When data quality issues are found in exported content (missing metadata, junk lines, wrong formatting, school/type annotations in descriptions, etc.), fix them at ingestion time in the parsing code, not by adding cleanup regex to the export layer.

**Why:** Export patches are fragile, book-specific, and have to be repeated for every output. Fixing at ingestion means the data is clean in the database for ALL consumers — export, RAG queries, API, etc. The user went through dozens of painful iterations because of export-side patches that should have been ingestion fixes.

**How to apply:**
- All content cleanup belongs in `load_tabletop_rules_docs.py`, not `export.py`
- Use config-driven patterns (`strip_content_patterns`, `metadata_fields`, etc.) so fixes apply to all books of the same system without code changes
- The export should be a simple renderer of clean data, not a cleanup layer
- When adding a new cleanup rule, add it to the YAML config schema and update the new book prompt
