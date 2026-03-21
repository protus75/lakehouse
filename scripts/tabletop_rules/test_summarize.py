"""Test spell summarization on one spell per level. Writes output to markdown file."""

import duckdb
import requests
from pathlib import Path

DB_PATH = "/workspace/db/lakehouse.duckdb"
OLLAMA_URL = "http://host.docker.internal:11434"
MODEL = "llama3:70b"
OUTPUT = Path("/workspace/data/exports/test_summarize_priest_spells.md")

conn = duckdb.connect(DB_PATH, read_only=True)
LEVEL_ORDER = {
    "first": 1, "second": 2, "third": 3, "fourth": 4,
    "fifth": 5, "sixth": 6, "seventh": 7, "eighth": 8, "ninth": 9,
}

rows = conn.execute("""
    SELECT DISTINCT ON (c.section_title) c.entry_title, c.section_title, c.content
    FROM documents_tabletop_rules.chunks c
    JOIN documents_tabletop_rules.toc t ON c.toc_id = t.toc_id
    WHERE LOWER(t.title) LIKE '%priest spell%'
    AND c.entry_title IS NOT NULL
    AND NOT t.is_excluded
    ORDER BY c.section_title, c.entry_title
""").fetchall()

rows = sorted(rows, key=lambda r: next(
    (v for k, v in LEVEL_ORDER.items() if k in r[1].lower()), 99
))
conn.close()

print(f"Testing summarization on {len(rows)} spells (one per level)")
OUTPUT.parent.mkdir(parents=True, exist_ok=True)

lines = ["# Priest Spell Summarization Test\n"]
lines.append(f"*One spell per level, {len(rows)} total*\n")
lines.append("---\n")

for i, (title, level, content) in enumerate(rows):
    print(f"  [{i+1}/{len(rows)}] {level} | {title}...")
    lines.append(f"\n## {level}\n")
    lines.append(f"### {title}\n")

    prompt = f"""Summarize this tabletop RPG spell concisely.
Keep ALL metadata fields in exact "Key: Value" format on separate lines.
Condense the description to 2-4 sentences capturing the key mechanical effect.
For spells with saving throws, include both pass and fail outcomes.
Always report Reversible as "Reversible: Yes" or "Reversible: No".

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
