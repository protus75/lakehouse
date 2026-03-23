"""Validate non-spell content: proficiencies, equipment, classes, tables.

Checks that expected entries and tables from config appear in chunk content.
Config-driven — each book defines expected_entries and expected_tables per section.

Run: docker exec lakehouse-workspace python scripts/tabletop_rules/validate_content.py
"""

import duckdb
import yaml
from pathlib import Path

DB_PATH = "/workspace/db/lakehouse.duckdb"
CONFIGS_DIR = Path("/workspace/documents/tabletop_rules/configs")


def load_config(source_file: str) -> dict:
    stem = Path(source_file).stem
    default_path = CONFIGS_DIR / "_default.yaml"
    book_path = CONFIGS_DIR / f"{stem}.yaml"
    config = {}
    if default_path.exists():
        with open(default_path) as f:
            config = yaml.safe_load(f) or {}
    if book_path.exists():
        with open(book_path) as f:
            book = yaml.safe_load(f) or {}
        for k, v in book.items():
            if isinstance(v, dict) and isinstance(config.get(k), dict):
                config[k].update(v)
            else:
                config[k] = v
    return config


conn = duckdb.connect(DB_PATH, read_only=True)

source_files = conn.execute(
    "SELECT DISTINCT source_file FROM documents_tabletop_rules.files"
).fetchall()

total_issues = 0

for (sf,) in source_files:
    config = load_config(sf)
    section_content = config.get("validation", {}).get("section_content", {})
    if not section_content:
        continue

    print(f"\n{'='*60}", flush=True)
    print(f"  {sf}", flush=True)
    print(f"{'='*60}", flush=True)

    for section_key, checks in section_content.items():
        # Find ToC section matching this key (substring match)
        toc_rows = conn.execute("""
            SELECT t.toc_id, t.title
            FROM documents_tabletop_rules.toc t
            WHERE t.source_file = ? AND LOWER(t.title) LIKE ?
            AND NOT t.is_excluded
        """, [sf, f"%{section_key.lower()}%"]).fetchall()

        if not toc_rows:
            print(f"\n  [{section_key}] — section not found in ToC", flush=True)
            total_issues += 1
            continue

        # Get all chunk content for matching sections
        toc_ids = [r[0] for r in toc_rows]
        placeholders = ",".join(["?"] * len(toc_ids))
        chunks = conn.execute(f"""
            SELECT content FROM documents_tabletop_rules.chunks
            WHERE source_file = ? AND toc_id IN ({placeholders})
        """, [sf] + toc_ids).fetchall()

        combined = "\n".join(r[0] for r in chunks).lower()
        section_title = toc_rows[0][1]

        # Check expected entries
        expected_entries = checks.get("expected_entries", [])
        missing_entries = []
        found_entries = []
        for entry in expected_entries:
            if entry.lower() in combined:
                found_entries.append(entry)
            else:
                missing_entries.append(entry)

        # Check expected tables
        expected_tables = checks.get("expected_tables", [])
        missing_tables = []
        found_tables = []
        for table in expected_tables:
            if table.lower() in combined:
                found_tables.append(table)
            else:
                missing_tables.append(table)

        # Report
        entry_ok = len(found_entries) == len(expected_entries)
        table_ok = len(found_tables) == len(expected_tables)
        status = "OK" if entry_ok and table_ok else "!!"
        issues = len(missing_entries) + len(missing_tables)
        total_issues += issues

        print(f"\n  [{status}] {section_title}", flush=True)
        print(f"       Entries: {len(found_entries)}/{len(expected_entries)} found, "
              f"{len(chunks)} chunks, {len(combined):,} chars", flush=True)
        print(f"       Tables:  {len(found_tables)}/{len(expected_tables)} found", flush=True)

        if missing_entries:
            print(f"       Missing entries: {missing_entries}", flush=True)
        if missing_tables:
            print(f"       Missing tables:  {missing_tables}", flush=True)

conn.close()

print(f"\n{'='*60}", flush=True)
if total_issues == 0:
    print("All content validation passed!", flush=True)
else:
    print(f"Total issues: {total_issues}", flush=True)
