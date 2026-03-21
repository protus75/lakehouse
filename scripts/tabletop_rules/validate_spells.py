"""Validate spell entries in the database for common parsing issues.
Uses the Spell Index (excluded appendix) as ground truth for valid spell names."""

import re
import duckdb
import fitz
from pathlib import Path

DB_PATH = "/workspace/db/lakehouse.duckdb"
PDF_DIR = Path("/workspace/documents/tabletop_rules/raw")

REQUIRED_PRIEST_METADATA = [
    "Sphere:", "Range:", "Components:", "Duration:",
    "Casting Time:", "Area of Effect:", "Saving Throw:",
]

REQUIRED_WIZARD_METADATA = [
    "Range:", "Components:", "Duration:",
    "Casting Time:", "Area of Effect:", "Saving Throw:",
]


def extract_spell_index_names(source_file: str) -> set[str]:
    """Extract known spell names from the Spell Index appendix in the PDF.
    These are the ground truth for what IS a spell."""
    conn = duckdb.connect(DB_PATH, read_only=True)
    # Get excluded ToC entries that are spell indexes
    index_rows = conn.execute("""
        SELECT title, page_start, page_end
        FROM documents_tabletop_rules.toc
        WHERE source_file = ?
        AND is_excluded
        AND (LOWER(title) LIKE '%spell index%'
             OR LOWER(title) LIKE '%spell list%')
    """, [source_file]).fetchall()
    conn.close()

    if not index_rows:
        return set()

    # Read spell names from those pages in the PDF
    filepath = PDF_DIR / source_file
    if not filepath.exists():
        return set()

    doc = fitz.open(str(filepath))
    names = set()
    for title, page_start, page_end in index_rows:
        for page_idx in range(page_start, min(page_end + 1, len(doc))):
            text = doc[page_idx].get_text("text")
            for line in text.split("\n"):
                stripped = line.strip()
                # Spell index lines: spell name followed by dots and page number
                # or just a spell name (short, capitalized)
                m = re.match(r"^([A-Z][A-Za-z' /]+?)(?:\s*\.{2,}|\s+\d+\s*$)", stripped)
                if m:
                    names.add(m.group(1).strip().lower())
                elif 3 <= len(stripped) <= 40 and stripped[0].isupper() and "." not in stripped:
                    names.add(stripped.lower())
    doc.close()
    return names


conn = duckdb.connect(DB_PATH, read_only=True)

# Get all spell chunks
rows = conn.execute("""
    SELECT c.entry_title, c.section_title, c.content, c.page_numbers,
           t.title as toc_title, c.source_file
    FROM documents_tabletop_rules.chunks c
    JOIN documents_tabletop_rules.toc t ON c.toc_id = t.toc_id
    WHERE (LOWER(t.title) LIKE '%priest spell%' OR LOWER(t.title) LIKE '%wizard spell%')
    AND c.entry_title IS NOT NULL
    AND NOT t.is_excluded
    ORDER BY t.title, c.section_title, c.entry_title
""").fetchall()

# Get source files for spell index lookup
source_files = conn.execute("""
    SELECT DISTINCT source_file FROM documents_tabletop_rules.files
""").fetchall()
conn.close()

# Build ground truth spell names from all spell indexes
known_spells = set()
for (sf,) in source_files:
    names = extract_spell_index_names(sf)
    if names:
        print(f"  Spell index from {sf}: {len(names)} spell names")
    known_spells.update(names)

print(f"\nValidating {len(rows)} spell chunks against {len(known_spells)} known spell names...\n")

issues = {
    "not_a_spell": [],
    "missing_from_export": [],
    "missing_metadata": [],
    "hyphenated_words": [],
    "duplicate_content": [],
    "toc_title_as_entry": [],
    "very_short": [],
    "no_description": [],
    "broken_lines": [],
    "orphan_continuation": [],
}

seen_content = {}
found_spells = set()
toc_names = {"appendix", "chapter", "priest spells", "wizard spells",
             "first-level", "second-level", "third-level", "fourth-level",
             "fifth-level", "sixth-level", "seventh-level"}

for row in rows:
    title = row[0] or ""
    section = row[1] or ""
    content = row[2] or ""
    pages = row[3] or ""
    toc = row[4] or ""
    source = row[5] or ""
    label = f"{title} (p.{pages}, {source})"

    # Track which known spells we found
    if title.lower() in known_spells:
        found_spells.add(title.lower())

    # Check: entry title is not a known spell
    if known_spells and title.lower() not in known_spells:
        # Could be a legitimate heading within a spell section that isn't a spell
        has_any_meta = any(f.lower() in content.lower() for f in ["Range:", "Casting Time:", "Duration:"])
        if not has_any_meta:
            issues["not_a_spell"].append(f"{label} — '{title}' not in spell index")

    # Check: entry title looks like a ToC section name
    if title.lower() in toc_names or title.lower().startswith("appendix"):
        issues["toc_title_as_entry"].append(label)

    # Check: missing required metadata fields
    req = REQUIRED_PRIEST_METADATA if "priest" in toc.lower() else REQUIRED_WIZARD_METADATA
    missing = [f for f in req if f.lower() not in content.lower()]
    if len(missing) >= 4:
        issues["missing_metadata"].append(f"{label} — missing: {', '.join(missing)}")

    # Check: hyphenated word breaks still present
    hyph = re.findall(r"\w+- \w+", content)
    if hyph:
        issues["hyphenated_words"].append(f"{label} — {hyph[:3]}")

    # Check: duplicate content across entries
    content_key = content[:200]
    if content_key in seen_content:
        issues["duplicate_content"].append(f"{label} — duplicate of {seen_content[content_key]}")
    else:
        seen_content[content_key] = label

    # Check: very short content (likely split incorrectly)
    if len(content) < 50:
        issues["very_short"].append(f"{label} — only {len(content)} chars: {content[:60]}")

    # Check: has metadata but no description text after it
    last_meta_pos = -1
    for field in REQUIRED_METADATA:
        idx = content.lower().rfind(field.lower())
        if idx > last_meta_pos:
            last_meta_pos = idx
    if last_meta_pos > 0:
        after_meta = content[last_meta_pos:].split("\n", 1)
        if len(after_meta) < 2 or len(after_meta[1].strip()) < 20:
            issues["no_description"].append(label)

    # Check: lines that start with lowercase (broken sentence continuation)
    lines = content.split("\n")
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped and stripped[0].islower() and i > 0 and len(stripped) < 30:
            prev = lines[i-1].strip() if i > 0 else ""
            if prev and not prev.endswith(":"):
                issues["broken_lines"].append(f"{label} — line {i}: '{stripped[:40]}'")
                break

    # Check: content starts with lowercase (orphan continuation from previous page)
    first_line = content.split("\n")[0].strip()
    if first_line and first_line[0].islower():
        issues["orphan_continuation"].append(f"{label} — starts with: '{first_line[:50]}'")

# Check: known spells missing from the export
if known_spells:
    missing_spells = known_spells - found_spells
    # Filter out wizard-only spells if we're only checking priest, etc.
    for spell_name in sorted(missing_spells):
        issues["missing_from_export"].append(spell_name)

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
