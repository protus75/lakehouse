"""Gold: cross-reference index for structured queries.

Extracts entry_type, spell_level, school, sphere, components, etc.
from silver_entries content using string parsing. No LLM needed.

Enables queries like: "all 3rd level wizard necromancy spells"
"""
import sys
sys.path.insert(0, "/workspace")


def _extract_field(content: str, field_name: str) -> str | None:
    """Extract a metadata field value from entry content.
    Looks for 'Field: value' on its own line."""
    field_lower = field_name.lower() + ":"
    for line in content.split("\n"):
        stripped = line.strip().lower()
        if stripped.startswith(field_lower):
            value = line.strip()[len(field_name) + 1:].strip()
            return value if value else None
    return None


def _extract_school(content: str) -> str | None:
    """Extract spell school from parenthetical after heading.
    e.g. '#### **Fireball** (Evocation)' → 'Evocation'"""
    for line in content.split("\n"):
        stripped = line.strip()
        if stripped.startswith("#"):
            # Find parenthetical
            paren_start = stripped.rfind("(")
            paren_end = stripped.rfind(")")
            if paren_start > 0 and paren_end > paren_start:
                school = stripped[paren_start + 1:paren_end].strip()
                # Clean markdown bold
                school = school.replace("*", "").strip()
                if school and len(school) < 40:
                    return school
            break
    return None


def _get_entry_type(toc_title: str, mapping: dict) -> str:
    """Determine entry_type from ToC title using config mapping."""
    toc_lower = toc_title.lower()
    for pattern, entry_type in mapping.items():
        if pattern.lower() in toc_lower:
            return entry_type
    return "rule"


def _get_spell_level(section_title: str, level_mapping: dict) -> int | None:
    """Extract spell level from section_title like 'First-Level Spells'."""
    if not section_title:
        return None
    section_lower = section_title.lower()
    for word, level in level_mapping.items():
        if word in section_lower:
            return level
    return None


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

            spell_level = None
            spell_class = None
            school = None
            sphere = None

            if entry_type == "spell":
                spell_level = _get_spell_level(section_title, level_mapping)
                toc_lower = toc_title.lower()
                if "wizard" in toc_lower:
                    spell_class = "wizard"
                elif "priest" in toc_lower:
                    spell_class = "priest"
                school = _extract_school(content)
                sphere = _extract_field(content, "Sphere")

            all_rows.append({
                "entry_id": int(row["entry_id"]),
                "source_file": sf,
                "entry_title": entry_title,
                "entry_type": entry_type,
                "spell_level": spell_level,
                "spell_class": spell_class,
                "school": school,
                "sphere": sphere,
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
