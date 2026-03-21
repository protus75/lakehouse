"""
Export entries from the RAG database as unified markdown documents.
Combines entries across all ingested books by category.

Usage:
  from rag.export import export_markdown
  export_markdown('priest_spells', output='/workspace/data/priest_spells_combined.md')
  export_markdown('wizard_spells')
  export_markdown('psionics')
  export_markdown('proficiencies')
  export_markdown('all_spells')
  export_markdown('full_book', source_file='DnD2e Handbook Player.pdf')
"""

import duckdb
import requests
from pathlib import Path

DB_PATH = "/workspace/db/lakehouse.duckdb"
OUTPUT_DIR = Path("/workspace/data/exports")
OLLAMA_URL = "http://host.docker.internal:11434"
DEFAULT_MODEL = "llama3:70b"


def _summarize_entry(entry_title: str, content: str, model: str = DEFAULT_MODEL) -> str:
    """Send an entry through the LLM for summarization.
    Returns summarized text, or original content on failure."""
    prompt = f"""Summarize this tabletop RPG rule book entry concisely.
Keep ALL metadata fields (School, Sphere, Range, Components, Duration, Casting Time,
Area of Effect, Saving Throw, Power Score, PSP Cost, etc.) in exact "Key: Value" format.
Condense the description to 2-4 sentences capturing the key mechanical effect.
For spells with saving throws, include both pass and fail outcomes.
Always report Reversible as "Reversible: Yes" or "Reversible: No".

ENTRY: {entry_title}
{content}

SUMMARY:"""

    try:
        response = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": model, "prompt": prompt, "stream": False},
            timeout=120,
        )
        response.raise_for_status()
        return response.json()["response"].strip()
    except Exception as e:
        print(f"    Summarize failed for '{entry_title}': {e}")
        return content


# ── Category queries ─────────────────────────────────────────────

CATEGORIES = {
    "priest_spells": {
        "title": "Priest Spells — Combined Reference",
        "description": "All priest spells from all ingested source books.",
        "query": """
            SELECT c.entry_title, c.section_title, c.content, c.page_numbers,
                   t.title as toc_title, f.document_title, f.game_system
            FROM documents_tabletop_rules.chunks c
            JOIN documents_tabletop_rules.toc t ON c.toc_id = t.toc_id
            JOIN documents_tabletop_rules.files f ON c.source_file = f.source_file
            WHERE LOWER(t.title) LIKE '%priest spell%'
            AND NOT t.is_excluded
            ORDER BY CASE
                WHEN LOWER(c.section_title) LIKE 'first%' THEN 1
                WHEN LOWER(c.section_title) LIKE 'second%' THEN 2
                WHEN LOWER(c.section_title) LIKE 'third%' THEN 3
                WHEN LOWER(c.section_title) LIKE 'fourth%' THEN 4
                WHEN LOWER(c.section_title) LIKE 'fifth%' THEN 5
                WHEN LOWER(c.section_title) LIKE 'sixth%' THEN 6
                WHEN LOWER(c.section_title) LIKE 'seventh%' THEN 7
                WHEN LOWER(c.section_title) LIKE 'eighth%' THEN 8
                WHEN LOWER(c.section_title) LIKE 'ninth%' THEN 9
                ELSE 99 END,
               c.entry_title, c.page_numbers
        """,
        "group_by": "section_title",
    },
    "wizard_spells": {
        "title": "Wizard Spells — Combined Reference",
        "description": "All wizard spells from all ingested source books.",
        "query": """
            SELECT c.entry_title, c.section_title, c.content, c.page_numbers,
                   t.title as toc_title, f.document_title, f.game_system
            FROM documents_tabletop_rules.chunks c
            JOIN documents_tabletop_rules.toc t ON c.toc_id = t.toc_id
            JOIN documents_tabletop_rules.files f ON c.source_file = f.source_file
            WHERE LOWER(t.title) LIKE '%wizard spell%'
            AND NOT t.is_excluded
            ORDER BY CASE
                WHEN LOWER(c.section_title) LIKE 'first%' THEN 1
                WHEN LOWER(c.section_title) LIKE 'second%' THEN 2
                WHEN LOWER(c.section_title) LIKE 'third%' THEN 3
                WHEN LOWER(c.section_title) LIKE 'fourth%' THEN 4
                WHEN LOWER(c.section_title) LIKE 'fifth%' THEN 5
                WHEN LOWER(c.section_title) LIKE 'sixth%' THEN 6
                WHEN LOWER(c.section_title) LIKE 'seventh%' THEN 7
                WHEN LOWER(c.section_title) LIKE 'eighth%' THEN 8
                WHEN LOWER(c.section_title) LIKE 'ninth%' THEN 9
                ELSE 99 END,
               c.entry_title, c.page_numbers
        """,
        "group_by": "section_title",
    },
    "all_spells": {
        "title": "All Spells — Combined Reference",
        "description": "All wizard and priest spells from all ingested source books.",
        "query": """
            SELECT c.entry_title, c.section_title, c.content, c.page_numbers,
                   t.title as toc_title, f.document_title, f.game_system
            FROM documents_tabletop_rules.chunks c
            JOIN documents_tabletop_rules.toc t ON c.toc_id = t.toc_id
            JOIN documents_tabletop_rules.files f ON c.source_file = f.source_file
            WHERE (LOWER(t.title) LIKE '%spell%' OR LOWER(t.title) LIKE '%magic%')
            AND NOT t.is_excluded
            AND LOWER(t.title) NOT LIKE '%school%'
            AND LOWER(t.title) NOT LIKE '%sphere%'
            AND LOWER(t.title) NOT LIKE '%list%'
            ORDER BY t.title,
               CASE
                WHEN LOWER(c.section_title) LIKE 'first%' THEN 1
                WHEN LOWER(c.section_title) LIKE 'second%' THEN 2
                WHEN LOWER(c.section_title) LIKE 'third%' THEN 3
                WHEN LOWER(c.section_title) LIKE 'fourth%' THEN 4
                WHEN LOWER(c.section_title) LIKE 'fifth%' THEN 5
                WHEN LOWER(c.section_title) LIKE 'sixth%' THEN 6
                WHEN LOWER(c.section_title) LIKE 'seventh%' THEN 7
                WHEN LOWER(c.section_title) LIKE 'eighth%' THEN 8
                WHEN LOWER(c.section_title) LIKE 'ninth%' THEN 9
                ELSE 99 END,
               c.entry_title, c.page_numbers
        """,
        "group_by": "toc_title",
    },
    "psionics": {
        "title": "Psionic Powers — Combined Reference",
        "description": "All psionic powers from all ingested source books.",
        "query": """
            SELECT c.entry_title, c.section_title, c.content, c.page_numbers,
                   t.title as toc_title, f.document_title, f.game_system
            FROM documents_tabletop_rules.chunks c
            JOIN documents_tabletop_rules.toc t ON c.toc_id = t.toc_id
            JOIN documents_tabletop_rules.files f ON c.source_file = f.source_file
            WHERE (LOWER(t.title) LIKE '%psionic%'
                   OR LOWER(t.title) LIKE '%clairsentien%'
                   OR LOWER(t.title) LIKE '%psychokin%'
                   OR LOWER(t.title) LIKE '%psychometab%'
                   OR LOWER(t.title) LIKE '%psychoport%'
                   OR LOWER(t.title) LIKE '%telepathy%'
                   OR LOWER(t.title) LIKE '%metapsionic%')
            AND NOT t.is_excluded
            ORDER BY t.title,
               CASE
                WHEN LOWER(c.section_title) LIKE 'first%' THEN 1
                WHEN LOWER(c.section_title) LIKE 'second%' THEN 2
                WHEN LOWER(c.section_title) LIKE 'third%' THEN 3
                WHEN LOWER(c.section_title) LIKE 'fourth%' THEN 4
                WHEN LOWER(c.section_title) LIKE 'fifth%' THEN 5
                WHEN LOWER(c.section_title) LIKE 'sixth%' THEN 6
                WHEN LOWER(c.section_title) LIKE 'seventh%' THEN 7
                WHEN LOWER(c.section_title) LIKE 'eighth%' THEN 8
                WHEN LOWER(c.section_title) LIKE 'ninth%' THEN 9
                ELSE 99 END,
               c.entry_title, c.page_numbers
        """,
        "group_by": "toc_title",
    },
    "proficiencies": {
        "title": "Proficiencies — Combined Reference",
        "description": "All proficiencies from all ingested source books.",
        "query": """
            SELECT c.entry_title, c.section_title, c.content, c.page_numbers,
                   t.title as toc_title, f.document_title, f.game_system
            FROM documents_tabletop_rules.chunks c
            JOIN documents_tabletop_rules.toc t ON c.toc_id = t.toc_id
            JOIN documents_tabletop_rules.files f ON c.source_file = f.source_file
            WHERE LOWER(t.title) LIKE '%proficien%'
            AND NOT t.is_excluded
            ORDER BY t.title,
               CASE
                WHEN LOWER(c.section_title) LIKE 'first%' THEN 1
                WHEN LOWER(c.section_title) LIKE 'second%' THEN 2
                WHEN LOWER(c.section_title) LIKE 'third%' THEN 3
                WHEN LOWER(c.section_title) LIKE 'fourth%' THEN 4
                WHEN LOWER(c.section_title) LIKE 'fifth%' THEN 5
                WHEN LOWER(c.section_title) LIKE 'sixth%' THEN 6
                WHEN LOWER(c.section_title) LIKE 'seventh%' THEN 7
                WHEN LOWER(c.section_title) LIKE 'eighth%' THEN 8
                WHEN LOWER(c.section_title) LIKE 'ninth%' THEN 9
                ELSE 99 END,
               c.entry_title, c.page_numbers
        """,
        "group_by": "toc_title",
    },
    "combat": {
        "title": "Combat Rules — Combined Reference",
        "description": "All combat rules from all ingested source books.",
        "query": """
            SELECT c.entry_title, c.section_title, c.content, c.page_numbers,
                   t.title as toc_title, f.document_title, f.game_system
            FROM documents_tabletop_rules.chunks c
            JOIN documents_tabletop_rules.toc t ON c.toc_id = t.toc_id
            JOIN documents_tabletop_rules.files f ON c.source_file = f.source_file
            WHERE LOWER(t.title) LIKE '%combat%'
            AND NOT t.is_excluded
            ORDER BY t.title,
               CASE
                WHEN LOWER(c.section_title) LIKE 'first%' THEN 1
                WHEN LOWER(c.section_title) LIKE 'second%' THEN 2
                WHEN LOWER(c.section_title) LIKE 'third%' THEN 3
                WHEN LOWER(c.section_title) LIKE 'fourth%' THEN 4
                WHEN LOWER(c.section_title) LIKE 'fifth%' THEN 5
                WHEN LOWER(c.section_title) LIKE 'sixth%' THEN 6
                WHEN LOWER(c.section_title) LIKE 'seventh%' THEN 7
                WHEN LOWER(c.section_title) LIKE 'eighth%' THEN 8
                WHEN LOWER(c.section_title) LIKE 'ninth%' THEN 9
                ELSE 99 END,
               c.entry_title, c.page_numbers
        """,
        "group_by": "toc_title",
    },
    "equipment": {
        "title": "Equipment — Combined Reference",
        "description": "All equipment rules from all ingested source books.",
        "query": """
            SELECT c.entry_title, c.section_title, c.content, c.page_numbers,
                   t.title as toc_title, f.document_title, f.game_system
            FROM documents_tabletop_rules.chunks c
            JOIN documents_tabletop_rules.toc t ON c.toc_id = t.toc_id
            JOIN documents_tabletop_rules.files f ON c.source_file = f.source_file
            WHERE (LOWER(t.title) LIKE '%equipment%' OR LOWER(t.title) LIKE '%money%')
            AND NOT t.is_excluded
            ORDER BY t.title,
               CASE
                WHEN LOWER(c.section_title) LIKE 'first%' THEN 1
                WHEN LOWER(c.section_title) LIKE 'second%' THEN 2
                WHEN LOWER(c.section_title) LIKE 'third%' THEN 3
                WHEN LOWER(c.section_title) LIKE 'fourth%' THEN 4
                WHEN LOWER(c.section_title) LIKE 'fifth%' THEN 5
                WHEN LOWER(c.section_title) LIKE 'sixth%' THEN 6
                WHEN LOWER(c.section_title) LIKE 'seventh%' THEN 7
                WHEN LOWER(c.section_title) LIKE 'eighth%' THEN 8
                WHEN LOWER(c.section_title) LIKE 'ninth%' THEN 9
                ELSE 99 END,
               c.entry_title, c.page_numbers
        """,
        "group_by": "toc_title",
    },
    "classes": {
        "title": "Character Classes — Combined Reference",
        "description": "All character class information from all ingested source books.",
        "query": """
            SELECT c.entry_title, c.section_title, c.content, c.page_numbers,
                   t.title as toc_title, f.document_title, f.game_system
            FROM documents_tabletop_rules.chunks c
            JOIN documents_tabletop_rules.toc t ON c.toc_id = t.toc_id
            JOIN documents_tabletop_rules.files f ON c.source_file = f.source_file
            WHERE (LOWER(t.title) LIKE '%class%' OR LOWER(t.title) LIKE '%kit%')
            AND NOT t.is_excluded
            ORDER BY t.title,
               CASE
                WHEN LOWER(c.section_title) LIKE 'first%' THEN 1
                WHEN LOWER(c.section_title) LIKE 'second%' THEN 2
                WHEN LOWER(c.section_title) LIKE 'third%' THEN 3
                WHEN LOWER(c.section_title) LIKE 'fourth%' THEN 4
                WHEN LOWER(c.section_title) LIKE 'fifth%' THEN 5
                WHEN LOWER(c.section_title) LIKE 'sixth%' THEN 6
                WHEN LOWER(c.section_title) LIKE 'seventh%' THEN 7
                WHEN LOWER(c.section_title) LIKE 'eighth%' THEN 8
                WHEN LOWER(c.section_title) LIKE 'ninth%' THEN 9
                ELSE 99 END,
               c.entry_title, c.page_numbers
        """,
        "group_by": "toc_title",
    },
}


# ── Export functions ─────────────────────────────────────────────

def export_markdown(
    category: str,
    output: str | None = None,
    source_file: str | None = None,
    summarize: bool = False,
    model: str = DEFAULT_MODEL,
) -> str:
    """Export a category of entries as a unified markdown document.

    Args:
        category: One of the predefined categories, or 'full_book' for a single book export
        output: Output file path. Defaults to /workspace/data/exports/{category}.md
        source_file: Filter to a single source book (required for 'full_book')
        summarize: If True, send each entry through the LLM for summarization
        model: LLM model to use for summarization

    Returns:
        The output file path
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if category == "full_book":
        return _export_full_book(source_file, output)

    if category not in CATEGORIES:
        available = ", ".join(sorted(CATEGORIES.keys()))
        print(f"Unknown category '{category}'. Available: {available}, full_book")
        return ""

    cat = CATEGORIES[category]
    if output is None:
        output = str(OUTPUT_DIR / f"{category}.md")

    conn = duckdb.connect(DB_PATH, read_only=True)
    query = cat["query"]
    if source_file:
        query = query.replace("ORDER BY", f"AND c.source_file = '{source_file}'\n            ORDER BY")

    rows = conn.execute(query).fetchall()
    conn.close()

    if not rows:
        print(f"No entries found for category '{category}'")
        return ""

    # Build markdown
    lines = [f"# {cat['title']}\n"]
    lines.append(f"*{cat['description']}*\n")
    lines.append(f"**{len(rows)} entries from {len(set(r[5] for r in rows))} source(s)**\n")
    lines.append("---\n")

    group_field = cat.get("group_by", "toc_title")
    group_idx = {"section_title": 1, "toc_title": 4, "document_title": 5}.get(group_field, 4)

    # Combine chunks that belong to the same entry, track source per entry
    from collections import OrderedDict
    entries = OrderedDict()
    for row in rows:
        entry_title = row[0] or ""
        section_title = row[1] or ""
        content = row[2] or ""
        toc_title = row[4] or ""
        doc_title = row[5] or ""
        group = row[group_idx] or "Ungrouped"
        # Key includes doc_title so same spell from different books stays separate
        key = (group, doc_title, entry_title or section_title)

        if key not in entries:
            entries[key] = {"group": group, "doc_title": doc_title,
                            "entry_title": entry_title, "chunks": []}
        entries[key]["chunks"].append(content)

    if summarize:
        print(f"Summarizing {len(entries)} entries (this will take a while)...")

    current_group = None
    entry_count = 0
    for key, entry in entries.items():
        group = entry["group"]
        if group != current_group:
            current_group = group
            lines.append(f"\n## {group}\n")

        title = entry["entry_title"]
        if title:
            lines.append(f"\n### {title}\n")
            lines.append(f"*Source: {entry['doc_title']}*\n")

        combined = "\n".join(entry["chunks"])
        if summarize and title:
            combined = _summarize_entry(title, combined, model=model)
            entry_count += 1
            if entry_count % 10 == 0:
                print(f"  Summarized {entry_count}/{len(entries)} entries...")

        lines.append(combined)
        lines.append("")

    md = "\n".join(lines)

    with open(output, "w", encoding="utf-8") as f:
        f.write(md)

    print(f"Exported {len(entries)} entries to {output}")
    return output


def _export_full_book(source_file: str | None, output: str | None) -> str:
    """Export an entire book as markdown."""
    if not source_file:
        conn = duckdb.connect(DB_PATH, read_only=True)
        files = conn.execute(
            "SELECT source_file, document_title FROM documents_tabletop_rules.files ORDER BY source_file"
        ).fetchall()
        conn.close()
        print("Available books:")
        for f in files:
            print(f"  {f[0]} ({f[1]})")
        return ""

    if output is None:
        stem = Path(source_file).stem
        output = str(OUTPUT_DIR / f"{stem}.md")

    conn = duckdb.connect(DB_PATH, read_only=True)
    rows = conn.execute("""
        SELECT c.entry_title, c.section_title, c.content, c.page_numbers,
               t.title as toc_title
        FROM documents_tabletop_rules.chunks c
        JOIN documents_tabletop_rules.toc t ON c.toc_id = t.toc_id
        WHERE c.source_file = ?
        ORDER BY t.page_start, c.page_numbers, c.chunk_id
    """, [source_file]).fetchall()

    doc = conn.execute(
        "SELECT document_title, game_system FROM documents_tabletop_rules.files WHERE source_file = ?",
        [source_file]
    ).fetchone()
    conn.close()

    if not rows:
        print(f"No entries found for '{source_file}'")
        return ""

    title = doc[0] if doc else source_file
    lines = [f"# {title}\n"]
    lines.append(f"**{len(rows)} entries**\n")
    lines.append("---\n")

    current_toc = None
    current_entry = None
    for row in rows:
        entry_title = row[0] or ""
        section_title = row[1] or ""
        content = row[2] or ""
        toc_title = row[4] or ""

        if toc_title != current_toc:
            current_toc = toc_title
            lines.append(f"\n## {toc_title}\n")
            current_entry = None

        if entry_title and entry_title != current_entry:
            current_entry = entry_title
            lines.append(f"\n### {entry_title}\n")

        lines.append(content)
        lines.append("")

    md = "\n".join(lines)

    with open(output, "w", encoding="utf-8") as f:
        f.write(md)

    print(f"Exported {len(rows)} entries to {output}")
    return output


def list_categories():
    """Print available export categories."""
    print("Available export categories:")
    for name, cat in sorted(CATEGORIES.items()):
        print(f"  {name:20s} — {cat['title']}")
    print(f"  {'full_book':20s} — Export a single book (requires source_file)")


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        list_categories()
    else:
        category = sys.argv[1]
        source = sys.argv[2] if len(sys.argv) > 2 else None
        export_markdown(category, source_file=source)
