"""Gold: cross-reference index for structured queries.

Extracts entry_type, spell_level from section context.
School/sphere come from silver_entries (extracted before cleanup strips them).
Other metadata fields parsed from content.

Enables queries like: "all 3rd level wizard necromancy spells"
"""
import sys
sys.path.insert(0, "/workspace")


def _extract_field(content: str, field_name: str) -> str | None:
    """Extract a metadata field value from entry content (case-insensitive)."""
    field_lower = field_name.lower() + ":"
    for line in content.split("\n"):
        stripped = line.strip()
        if stripped.lower().startswith(field_lower):
            return stripped[len(field_name) + 1:].strip() or None
    return None


def _get_entry_type(toc_title: str, mapping: dict) -> str:
    """Determine entry_type from ToC title using config mapping."""
    toc_lower = toc_title.lower()
    for pattern, entry_type in mapping.items():
        if pattern.lower() in toc_lower:
            return entry_type
    return "rule"


def _get_spell_level(section_title: str, toc_title: str, level_mapping: dict) -> int | None:
    """Extract spell level from section_title like 'First-Level Spells'.
    Spells before the first sub-heading default to level 1."""
    if not section_title:
        return None
    section_lower = section_title.lower()
    for word, level in level_mapping.items():
        if word in section_lower:
            return level
    # If section_title is the main section (no sub-section matched), default to L1
    if "spell" in section_lower and "level" not in section_lower:
        return 1
    return None


def _is_valid_spell(entry_title: str | None, content: str) -> bool:
    """Filter out junk entries that aren't actual spells."""
    if not entry_title or entry_title == "None":
        return False
    junk_titles = {"combat", "divination", "compiled character tables", "spell index",
                   "wizard spells by school", "priest spells by sphere"}
    if entry_title.lower() in junk_titles:
        return False
    content_lower = content.lower()
    return any(f in content_lower for f in
               ["range:", "duration:", "casting time:", "components:", "sphere:"])


def model(dbt, session):
    dbt.config(materialized="table")

    from dlt.lib.tabletop_cleanup import load_config
    from pathlib import Path
    import pandas as pd

    configs_dir = Path("/workspace/documents/tabletop_rules/configs")
    entries_df = dbt.ref("silver_entries").df()

    all_rows = []

    for sf in entries_df["source_file"].unique():
        config = load_config(Path(sf), configs_dir)
        type_mapping = config.get("entry_type_mapping", {})
        level_mapping = config.get("spell_level_mapping", {})

        sf_entries = entries_df[entries_df["source_file"] == sf]

        for _, row in sf_entries.iterrows():
            content = row["content"]
            toc_title = row["toc_title"]
            section_title = row["section_title"]
            entry_title = row["entry_title"]

            entry_type = _get_entry_type(toc_title, type_mapping)

            # Filter junk entries in spell sections
            if entry_type == "spell" and not _is_valid_spell(entry_title, content):
                entry_type = "rule"

            spell_level = None
            spell_class = None

            if entry_type == "spell":
                spell_level = _get_spell_level(section_title, toc_title, level_mapping)
                toc_lower = toc_title.lower()
                school = row.get("school")
                sphere = row.get("sphere")
                if "wizard" in toc_lower:
                    spell_class = "wizard"
                elif "priest" in toc_lower:
                    spell_class = "priest"
                    # Correction: priest spells must have a sphere.
                    # If a "priest" spell has school but no sphere, it's a wizard spell
                    # misassigned by page-position anchors.
                    if school and not sphere:
                        spell_class = "wizard"

            all_rows.append({
                "entry_id": int(row["entry_id"]),
                "source_file": sf,
                "entry_title": entry_title,
                "entry_type": entry_type,
                "spell_level": spell_level,
                "spell_class": spell_class,
                "school": row.get("school"),      # from silver (extracted before strip)
                "sphere": row.get("sphere"),      # from silver (extracted before strip)
                "components": _extract_field(content, "Components") or _extract_field(content, "Component"),
                "saving_throw": _extract_field(content, "Saving Throw"),
                "range_text": _extract_field(content, "Range"),
                "duration_text": _extract_field(content, "Duration"),
                "casting_time": _extract_field(content, "Casting Time"),
            })

    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame(
        columns=["entry_id", "source_file", "entry_title", "entry_type",
                 "spell_level", "spell_class", "school", "sphere",
                 "components", "saving_throw", "range_text", "duration_text",
                 "casting_time"]
    )
