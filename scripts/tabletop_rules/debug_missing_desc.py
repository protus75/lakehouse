"""Debug: trace why specific entries end up with no description.
Runs the pipeline through build_entries and dumps raw Marker text around problem spells."""

import re
import sys
sys.path.insert(0, "/workspace")
from pathlib import Path
from dlt.load_tabletop_rules_docs import (
    PDFCache, load_config, extract_marker_markdown, _detect_watermarks,
    build_heading_chapter_map, extract_known_entries, build_entries, parse_toc,
)

filepath = Path("/workspace/documents/tabletop_rules/raw/DnD2e Handbook Player.pdf")
config = load_config(filepath)
pdf = PDFCache(filepath, config)
toc_data = parse_toc(pdf, config)

print("Extracting Marker markdown...")
markdown = extract_marker_markdown(filepath)
print(f"Marker: {len(markdown):,} chars")

watermarks = _detect_watermarks(pdf)
if watermarks:
    lines = [l for l in markdown.split("\n") if l.strip() not in watermarks]
    markdown = "\n".join(lines)

# Dump raw Marker text around problem entries
TARGETS = ["Insect Plague", "Raise Dead", "Stone Tell", "Command", "Dust Devil"]
for name in TARGETS:
    # Find in Marker markdown
    pattern = re.compile(re.escape(name), re.IGNORECASE)
    for m in pattern.finditer(markdown):
        start = max(0, m.start() - 50)
        end = min(len(markdown), m.end() + 800)
        context = markdown[start:end]
        # Only show if it's near a heading
        if "###" in context[:100] or "####" in context[:100]:
            print(f"\n{'='*60}")
            print(f"RAW MARKER for {name} (pos {m.start()}):")
            print(f"{'='*60}")
            print(context)
            print(f"{'='*60}")
            break

# Now run build_entries and check results
heading_chapter_map = build_heading_chapter_map(markdown, toc_data["sections"], pdf)
known_entries = extract_known_entries(pdf, toc_data, config)
entries = build_entries(markdown, heading_chapter_map, known_entries, config, toc_data["sections"])

print(f"\n\n{'='*60}")
print("ENTRIES FOR TARGETS:")
print(f"{'='*60}")
for entry in entries:
    if entry.get("entry_title") in TARGETS:
        title = entry["entry_title"]
        content = entry["content"]
        has_desc = len(content.split("\n")) > 8 and len(content) > 200
        print(f"\n--- {title} ({len(content)} chars, has_desc={has_desc}) ---")
        print(content[:600])
        print("---")
