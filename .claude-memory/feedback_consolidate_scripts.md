---
name: Consolidate scripts by domain, never one-script-per-action
description: Group related tooling into single scripts with subcommands. Don't proliferate one-shot scripts.
type: feedback
---

When building tooling for a domain (probing, debugging, validating a feature), put ALL related actions into ONE script with subcommands. Do NOT create a separate script per action.

Bad pattern (what I did for table extraction):
- scripts/probe_pymupdf_tables.py
- scripts/probe_pymupdf_fonts.py
- scripts/probe_vlm_bbox.py
- scripts/probe_table_regions.py
- scripts/inspect_region_probe.py
- scripts/debug_table_detection.py

Good pattern:
- scripts/table_extraction.py with subcommands: probe-fonts, probe-vlm, detect, inspect, debug

**Why:** One-script-per-action is hack-o-ramming. It explodes the script directory, duplicates boilerplate (CONTAINER, PDF_PATH, subprocess wrapper), splits related context across files, and makes maintenance painful. The user explicitly called this out as "fucking planning this not hack-o-ramming."

**How to apply:**
- Before creating a new script, check if there's an existing script in the same domain. If yes, add a subcommand to it.
- Use argparse subcommands or simple positional dispatch for grouping.
- Constants (CONTAINER, paths, config locations) defined once at the top.
- Delete dead probe scripts once they've served their purpose (e.g. probe_pymupdf_tables.py after PyMuPDF find_tables() was proven dead).
- One script per logical domain, not per action within the domain.
