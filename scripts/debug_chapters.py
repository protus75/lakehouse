#!/usr/bin/env python3
"""Debug: check chapter ranges and spell headings."""
import sys
sys.path.insert(0, "/workspace")
import re
from pathlib import Path
from dlt.lib.tabletop_cleanup import load_config, _build_page_position_map
from dlt.lib.duckdb_reader import get_reader

config = load_config(Path("DnD2e Handbook Player.pdf"), Path("/workspace/documents/tabletop_rules/configs"))
conn = get_reader(["bronze_tabletop"])

md = conn.execute("SELECT markdown_text FROM bronze_tabletop.marker_extractions LIMIT 1").fetchone()[0]
pages_df = conn.execute(
    "SELECT page_index, page_text, printed_page_num FROM bronze_tabletop.page_texts "
    "WHERE source_file = 'DnD2e Handbook Player.pdf' ORDER BY page_index"
).fetchdf()
page_texts = pages_df["page_text"].tolist()
page_printed = dict(zip(pages_df["page_index"].tolist(), pages_df["printed_page_num"].tolist()))

anchors = _build_page_position_map(md, page_texts, page_printed, len(page_texts), config)
lines = md.split("\n")
line_starts = []
pos = 0
for line in lines:
    line_starts.append(pos)
    pos += len(line) + 1

def page_to_line(pp):
    best_pos = 0
    for md_pos, p in anchors:
        if p <= pp:
            best_pos = md_pos
        elif p > pp:
            break
    for li in range(len(line_starts) - 1, -1, -1):
        if line_starts[li] <= best_pos:
            return li
    return 0

# Check chapter ranges
for pg, name in [(170, "App3 Wizard"), (252, "App4 Priest"), (300, "App5 School")]:
    li = page_to_line(pg)
    print(f"{name} page={pg} -> line={li}: {lines[li][:60]}")

ch3_start = page_to_line(170)
ch4_start = page_to_line(252)
print(f"\nApp3 range: lines {ch3_start}-{ch4_start} ({ch4_start - ch3_start} lines)")

# Count headings
h34_count = sum(1 for i in range(ch3_start, ch4_start) if re.match(r"^#{3,4}\s", lines[i]))
print(f"H3/H4 headings in App3: {h34_count}")

# Show first 10
shown = 0
for i in range(ch3_start, ch4_start):
    if re.match(r"^#{3,4}\s", lines[i]) and shown < 10:
        clean = re.sub(r"\*+", "", lines[i].lstrip("#").strip()).strip()
        print(f"  L{i}: {clean[:50]}")
        shown += 1

# Check if "Affect Normal Fires" is findable
print("\nSearching for 'Affect Normal Fires':")
for i in range(ch3_start, ch4_start):
    if "Affect Normal Fires" in lines[i]:
        print(f"  Found at L{i}: {lines[i][:60]}")
