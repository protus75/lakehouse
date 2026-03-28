"""Dump gold_entries to output/gold_entries_dump.txt for content review."""
import sys
sys.path.insert(0, "/workspace")

import os
from dlt.lib.duckdb_reader import get_reader

os.makedirs("/workspace/output", exist_ok=True)
c = get_reader(["gold_tabletop"])
rows = c.execute("""
    SELECT sort_order, depth, is_chapter, is_table,
           coalesce(entry_title, toc_title) as title,
           coalesce(length(content), 0) as clen,
           coalesce(left(content, 150), '') as preview
    FROM gold_tabletop.gold_entries
    ORDER BY sort_order, entry_title
""").fetchall()

with open("/workspace/output/gold_entries_dump.txt", "w", encoding="utf-8") as f:
    for so, depth, is_ch, is_tbl, title, clen, preview in rows:
        indent = "  " * (depth or 0)
        preview = preview.replace("\n", " ").strip()[:100]
        f.write(f"{indent}{title} ({clen} chars)\n")
        if preview:
            f.write(f"{indent}  {preview}...\n\n")
        else:
            f.write("\n")

print(f"{len(rows)} entries written to output/gold_entries_dump.txt")
