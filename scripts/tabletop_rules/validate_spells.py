"""Validate spell/entry parsing in the database.
Uses config-driven ToC names and the spell index as ground truth.
Reads validation settings from the book's YAML config."""

import re
import sys
import duckdb
import fitz
import yaml
from pathlib import Path

DB_PATH = "/workspace/db/lakehouse.duckdb"
PDF_DIR = Path("/workspace/documents/tabletop_rules/raw")
CONFIGS_DIR = Path("/workspace/documents/tabletop_rules/configs")


def load_validation_config(source_file: str) -> dict:
    """Load validation settings from the book's config."""
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


def extract_spell_index_names(source_file: str) -> set[str]:
    """Extract known spell names from excluded index sections."""
    conn = duckdb.connect(DB_PATH, read_only=True)
    index_rows = conn.execute("""
        SELECT title, page_start, page_end
        FROM documents_tabletop_rules.toc
        WHERE source_file = ?
        AND is_excluded
    """, [source_file]).fetchall()
    conn.close()

    if not index_rows:
        return set()

    filepath = PDF_DIR / source_file
    if not filepath.exists():
        return set()

    config = load_validation_config(source_file)
    page_pattern = config.get("toc", {}).get("page_number_pattern", r"^\d{1,3}$")

    doc = fitz.open(str(filepath))
    names = set()
    for title, page_start, page_end in index_rows:
        for page_idx in range(len(doc)):
            page = doc[page_idx]
            text = page.get_text("text")
            # Read printed page number
            printed = None
            for line in reversed(text.split("\n")):
                stripped = line.strip()
                if stripped and re.match(page_pattern, stripped):
                    printed = int(re.search(r"\d+", stripped).group())
                    break
            if printed is None:
                printed = page_idx
            if not (page_start <= printed <= page_end):
                continue
            for line in text.split("\n"):
                stripped = line.strip()
                if not stripped or len(stripped) < 3:
                    continue
                if re.match(page_pattern, stripped):
                    continue
                clean = re.sub(r"\s*(?:\.[\s.]*){2,}.*", "", stripped).strip()
                clean = re.sub(r"\s+\d+\s*$", "", clean).strip()
                if clean and 3 <= len(clean) <= 50 and clean[0].isupper():
                    names.add(clean.lower())
    doc.close()
    return names


# ── Main ─────────────────────────────────────────────────────────

conn = duckdb.connect(DB_PATH, read_only=True)

# Get all source files
source_files = conn.execute(
    "SELECT DISTINCT source_file FROM documents_tabletop_rules.files"
).fetchall()

# Build known spells per source
all_known = set()
for (sf,) in source_files:
    config = load_validation_config(sf)
    validation = config.get("validation", {})
    toc_match = validation.get("spell_toc_patterns", ["spell", "wizard spell", "priest spell"])
    names = extract_spell_index_names(sf)
    if names:
        print(f"  Spell index from {sf}: {len(names)} spell names")
    all_known.update(names)

# Get all spell chunks using config-driven ToC patterns
all_toc_patterns = set()
for (sf,) in source_files:
    config = load_validation_config(sf)
    validation = config.get("validation", {})
    patterns = validation.get("spell_toc_patterns", ["spell"])
    all_toc_patterns.update(p.lower() for p in patterns)

# Build WHERE clause for ToC title matching
toc_where = " OR ".join([f"LOWER(t.title) LIKE '%{p}%'" for p in all_toc_patterns])
if not toc_where:
    toc_where = "LOWER(t.title) LIKE '%spell%'"

rows = conn.execute(f"""
    SELECT c.entry_title, c.section_title, c.content, c.page_numbers,
           t.title as toc_title, c.source_file
    FROM documents_tabletop_rules.chunks c
    JOIN documents_tabletop_rules.toc t ON c.toc_id = t.toc_id
    WHERE ({toc_where})
    AND c.entry_title IS NOT NULL
    AND NOT t.is_excluded
    ORDER BY t.title, c.section_title, c.entry_title
""").fetchall()
conn.close()

print(f"\nValidating {len(rows)} spell chunks against {len(all_known)} known spell names...\n")

issues = {
    "not_a_spell": [],
    "missing_from_export": [],
    "missing_metadata": [],
    "hyphenated_words": [],
    "duplicate_content": [],
    "toc_title_as_entry": [],
    "very_short": [],
    "no_description": [],
    "orphan_continuation": [],
}

seen_content = {}
found_spells = set()

for row in rows:
    title = row[0] or ""
    section = row[1] or ""
    content = row[2] or ""
    pages = row[3] or ""
    toc = row[4] or ""
    source = row[5] or ""
    label = f"{title} (p.{pages}, {source})"

    config = load_validation_config(source)
    validation = config.get("validation", {})
    req = validation.get("required_metadata", [
        "Range:", "Components:", "Duration:", "Casting Time:",
        "Area of Effect:", "Saving Throw:",
    ])

    if title.lower() in all_known:
        found_spells.add(title.lower())

    # Not a known spell
    if all_known and title.lower() not in all_known:
        has_any_meta = any(f.lower() in content.lower() for f in ["Range:", "Casting Time:", "Duration:"])
        if not has_any_meta:
            issues["not_a_spell"].append(f"{label} — '{title}' not in spell index")

    # ToC title as entry
    if title.lower().startswith("appendix") or title.lower().startswith("chapter"):
        issues["toc_title_as_entry"].append(label)

    # Missing metadata
    missing = [f for f in req if f.lower() not in content.lower()]
    if len(missing) >= 4:
        issues["missing_metadata"].append(f"{label} — missing: {', '.join(missing)}")

    # Hyphenated words
    hyph = re.findall(r"\w+- \w+", content)
    if hyph:
        issues["hyphenated_words"].append(f"{label} — {hyph[:3]}")

    # Duplicate content
    content_key = content[:200]
    if content_key in seen_content:
        issues["duplicate_content"].append(f"{label} — duplicate of {seen_content[content_key]}")
    else:
        seen_content[content_key] = label

    # Very short
    if len(content) < 50:
        issues["very_short"].append(f"{label} — only {len(content)} chars: {content[:60]}")

    # No description
    last_meta_pos = -1
    for field in req:
        idx = content.lower().rfind(field.lower())
        if idx > last_meta_pos:
            last_meta_pos = idx
    if last_meta_pos > 0:
        after_meta = content[last_meta_pos:].split("\n", 1)
        if len(after_meta) < 2 or len(after_meta[1].strip()) < 20:
            issues["no_description"].append(label)

    # Orphan continuation
    first_line = content.split("\n")[0].strip()
    if first_line and first_line[0].islower():
        issues["orphan_continuation"].append(f"{label} — starts with: '{first_line[:50]}'")

# Missing spells
if all_known:
    missing_spells = all_known - found_spells
    for name in sorted(missing_spells):
        issues["missing_from_export"].append(name)

# Report
total_issues = sum(len(v) for v in issues.values())
print(f"Found {total_issues} issues across {len(rows)} chunks\n")

for category, items in issues.items():
    if items:
        print(f"{'='*60}")
        print(f"{category.upper()} ({len(items)} issues)")
        print(f"{'='*60}")
        for item in items[:10]:
            print(f"  {item}")
        if len(items) > 10:
            print(f"  ... and {len(items) - 10} more")
        print()

if total_issues == 0:
    print("All entries passed validation!")
