"""Test spell summarization on one spell per level. Writes output to markdown file.
Usage: python test_summarize.py [toc_pattern] [source_file]
  toc_pattern: ToC title substring to match (default: "priest spell")
  source_file: filter to one book (default: all books)
"""

import sys
import duckdb
import requests
from pathlib import Path

DB_PATH = "/workspace/db/lakehouse.duckdb"
OLLAMA_URL = "http://host.docker.internal:11434"
MODEL = "llama3:70b"

toc_pattern = sys.argv[1] if len(sys.argv) > 1 else "priest spell"
source_filter = sys.argv[2] if len(sys.argv) > 2 else None

output_name = toc_pattern.replace(" ", "_")
OUTPUT = Path(f"/workspace/data/exports/test_summarize_{output_name}.md")

LEVEL_ORDER = {
    "first": 1, "second": 2, "third": 3, "fourth": 4,
    "fifth": 5, "sixth": 6, "seventh": 7, "eighth": 8, "ninth": 9,
}

conn = duckdb.connect(DB_PATH, read_only=True)

query = """
    SELECT DISTINCT ON (c.section_title) c.entry_title, c.section_title, c.content, c.source_file
    FROM documents_tabletop_rules.chunks c
    JOIN documents_tabletop_rules.toc t ON c.toc_id = t.toc_id
    WHERE LOWER(t.title) LIKE ?
    AND c.entry_title IS NOT NULL
    AND NOT t.is_excluded
"""
params = [f"%{toc_pattern.lower()}%"]

if source_filter:
    query += "    AND c.source_file = ?\n"
    params.append(source_filter)

query += "    ORDER BY c.section_title, c.entry_title"

rows = conn.execute(query, params).fetchall()
rows = sorted(rows, key=lambda r: next(
    (v for k, v in LEVEL_ORDER.items() if k in r[1].lower()), 99
))
conn.close()

print(f"Testing summarization on {len(rows)} entries (one per level)")
print(f"  ToC pattern: '{toc_pattern}'")
if source_filter:
    print(f"  Source: {source_filter}")
OUTPUT.parent.mkdir(parents=True, exist_ok=True)

lines = [f"# Summarization Test: {toc_pattern}\n"]
lines.append(f"*One entry per level, {len(rows)} total*\n")
lines.append("---\n")

for i, (title, level, content, source) in enumerate(rows):
    print(f"  [{i+1}/{len(rows)}] {level} | {title} ({source})...")
    lines.append(f"\n## {level}\n")
    lines.append(f"### {title}\n")
    lines.append(f"*Source: {source}*\n")

    prompt = f"""Summarize this tabletop RPG entry concisely.
Keep ALL metadata fields in exact "Key: Value" format on separate lines.
Condense the description to 2-4 sentences capturing the key mechanical effect.
For entries with saving throws, include both pass and fail outcomes.
Always report Reversible as "Reversible: Yes" or "Reversible: No" if applicable.

ENTRY: {title}
{content}

SUMMARY:"""

    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": MODEL, "prompt": prompt, "stream": False},
            timeout=180,
        )
        r.raise_for_status()
        summary = r.json()["response"].strip()
    except Exception as e:
        summary = f"ERROR: {e}"

    lines.append(summary)
    lines.append("")

with open(OUTPUT, "w", encoding="utf-8") as f:
    f.write("\n".join(lines))

print(f"\nDone! Output: {OUTPUT}")
