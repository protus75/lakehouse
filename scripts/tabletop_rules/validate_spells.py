"""Validate spell/entry parsing in the database.
Uses known_entries table (populated at ingestion) as ground truth.
No PDF reading — everything comes from the DB.

Validates per-ENTRY (combining all chunks), not per-chunk."""

import re
import duckdb
import yaml
from pathlib import Path
from collections import OrderedDict

DB_PATH = "/workspace/db/lakehouse.duckdb"
CONFIGS_DIR = Path("/workspace/documents/tabletop_rules/configs")


def load_validation_config(source_file: str) -> dict:
    stem = Path(source_file).stem
    config_path = CONFIGS_DIR / f"{stem}.yaml"
    default_path = CONFIGS_DIR / "_default.yaml"
    config = {}
    if default_path.exists():
        with open(default_path) as f:
            config = yaml.safe_load(f) or {}
    if config_path.exists():
        with open(config_path) as f:
            book = yaml.safe_load(f) or {}
        for k, v in book.items():
            if isinstance(v, dict) and isinstance(config.get(k), dict):
                config[k].update(v)
            else:
                config[k] = v
    return config


# ── Main ─────────────────────────────────────────────────────────

conn = duckdb.connect(DB_PATH, read_only=True)

source_files = conn.execute(
    "SELECT DISTINCT source_file FROM documents_tabletop_rules.files"
).fetchall()

# Get known entries from DB (populated during ingestion)
all_known = set()
for (sf,) in source_files:
    names = conn.execute(
        "SELECT entry_name FROM documents_tabletop_rules.known_entries WHERE source_file = ?",
        [sf],
    ).fetchall()
    if names:
        known = {r[0] for r in names}
        print(f"  Known entries from {sf}: {len(known)}")
        all_known.update(known)

# Get spell chunks
all_toc_patterns = set()
for (sf,) in source_files:
    config = load_validation_config(sf)
    validation = config.get("validation", {})
    patterns = validation.get("spell_toc_patterns", ["spell"])
    all_toc_patterns.update(p.lower() for p in patterns)

toc_where = " OR ".join([f"LOWER(t.title) LIKE '%{p}%'" for p in all_toc_patterns])
if not toc_where:
    toc_where = "LOWER(t.title) LIKE '%spell%'"

rows = conn.execute(f"""
    SELECT c.entry_title, c.section_title, c.content, c.page_numbers,
           t.title as toc_title, c.source_file, c.chunk_id
    FROM documents_tabletop_rules.chunks c
    JOIN documents_tabletop_rules.toc t ON c.toc_id = t.toc_id
    WHERE ({toc_where})
    AND c.entry_title IS NOT NULL
    AND NOT t.is_excluded
    ORDER BY t.title, c.section_title, c.entry_title, c.chunk_id
""").fetchall()
conn.close()

# Group chunks into entries
entries = OrderedDict()
for row in rows:
    title, section, content, pages, toc, source, chunk_id = row
    title = title or ""
    key = (source or "", toc or "", title)
    if key not in entries:
        entries[key] = {
            "title": title, "section": section or "", "toc": toc or "",
            "source": source or "", "pages": pages or "",
            "chunks": [], "combined": "",
        }
    entries[key]["chunks"].append(content or "")

for entry in entries.values():
    entry["combined"] = "\n\n".join(entry["chunks"])

total_chunks = len(rows)
total_entries = len(entries)
print(f"\nValidating {total_entries} spell entries ({total_chunks} chunks) against {len(all_known)} known spell names...\n")

issues = {
    "missing_from_index": [],
    "missing_metadata": [],
    "no_description": [],
    "orphan_chunks": [],
    "duplicate_content": [],
    "very_short": [],
    "hyphenated_words": [],
}

seen_content = {}
found_spells = set()

for key, entry in entries.items():
    title = entry["title"]
    source = entry["source"]
    pages = entry["pages"]
    combined = entry["combined"]
    num_chunks = len(entry["chunks"])
    label = f"{title} (p.{pages}, {num_chunks} chunks)"

    config = load_validation_config(source)
    validation = config.get("validation", {})
    req = validation.get("required_metadata", [
        "Range", "Component", "Duration", "Casting Time",
        "Area of Effect", "Saving Throw",
    ])
    # Strip colons from config values for prefix matching
    req = [f.rstrip(":").strip() for f in req]

    if title.lower() in all_known:
        found_spells.add(title.lower())

    # Missing metadata — prefix match (handles Component/Components, etc.)
    combined_lower = combined.lower()
    missing = [f for f in req if f.lower() not in combined_lower]

    # Skip entries with no metadata at all — not a spell
    if len(missing) == len(req):
        continue

    if missing:
        issues["missing_metadata"].append(f"{label} — missing: {', '.join(missing)}")

    # No description — find last metadata line, check for text after it
    last_meta_pos = -1
    for field in req:
        # Search for "Field:" at start of line to avoid matching mid-word
        pattern = re.compile(r"^" + re.escape(field) + r"\w*\s*:", re.IGNORECASE | re.MULTILINE)
        for m in pattern.finditer(combined):
            if m.start() > last_meta_pos:
                last_meta_pos = m.start()
    if last_meta_pos > 0:
        after_meta = combined[last_meta_pos:].split("\n", 1)
        if len(after_meta) < 2 or len(after_meta[1].strip()) < 20:
            issues["no_description"].append(label)

    # Orphan chunks
    orphan_count = sum(1 for c in entry["chunks"][1:]
                       if c.split("\n")[0].strip() and c.split("\n")[0].strip()[0].islower())
    if orphan_count:
        issues["orphan_chunks"].append(f"{label} — {orphan_count} orphan chunk(s)")

    # Duplicate content
    content_key = combined[:200]
    if content_key in seen_content:
        issues["duplicate_content"].append(f"{label} — duplicate of {seen_content[content_key]}")
    else:
        seen_content[content_key] = label

    # Very short
    if len(combined) < 50:
        issues["very_short"].append(f"{label} — only {len(combined)} chars")

    # Hyphenated words
    hyph = re.findall(r"\w+- \w+", combined)
    if hyph:
        issues["hyphenated_words"].append(f"{label} — {hyph[:3]}")

# Missing from index
if all_known:
    missing_spells = sorted(all_known - found_spells)
    if missing_spells:
        issues["missing_from_index"] = missing_spells[:20]
        if len(missing_spells) > 20:
            issues["missing_from_index"].append(f"... and {len(missing_spells) - 20} more")

# Report
total_issues = sum(len(v) for v in issues.values())
print(f"Found {total_issues} issues across {total_entries} entries\n")

for category, items in issues.items():
    if items:
        print(f"{'='*60}")
        print(f"{category.upper()} ({len(items)} issues)")
        print(f"{'='*60}")
        for item in items[:15]:
            print(f"  {item}")
        if len(items) > 15:
            print(f"  ... and {len(items) - 15} more")
        print()

if total_issues == 0:
    print("All entries passed validation!")
