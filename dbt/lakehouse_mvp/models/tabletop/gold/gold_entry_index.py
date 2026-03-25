"""Gold: cross-reference index for structured queries.

Spell data comes from silver_spell_crosscheck (authoritative, fuzzy-matched across all appendixes).
Non-spell entry_type comes from entry_type_mapping config.
Other metadata fields parsed from content for non-spell entries.
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


def model(dbt, session):
    dbt.config(materialized="table")

    from dlt.lib.tabletop_cleanup import load_config
    from pathlib import Path
    import pandas as pd

    configs_dir = Path("/workspace/documents/tabletop_rules/configs")
    entries_df = dbt.ref("silver_entries").df()
    crosscheck_df = dbt.ref("silver_spell_crosscheck").df()

    # Build spell lookup from crosscheck (authoritative)
    spell_lookup = {}
    for _, row in crosscheck_df.iterrows():
        key = (row["source_file"], row["entry_name"])
        # Store by (name, class) — some spells are both wizard and priest
        spell_lookup[(row["source_file"], row["entry_name"], row["entry_class"])] = {
            "spell_class": row["entry_class"],
            "spell_level": int(row["entry_level"]) if pd.notna(row["entry_level"]) else None,
            "school": row["school"],
            "sphere": row["sphere"],
            "is_reversible": row["is_reversible"],
            "ref_page": int(row["ref_page"]) if pd.notna(row["ref_page"]) else None,
        }

    all_rows = []

    for sf in entries_df["source_file"].unique():
        config = load_config(Path(sf), configs_dir)
        type_mapping = config.get("entry_type_mapping", {})

        sf_entries = entries_df[entries_df["source_file"] == sf]

        for _, row in sf_entries.iterrows():
            content = row["content"]
            toc_title = row["toc_title"]
            entry_title = row["entry_title"]
            entry_name = entry_title.lower().strip() if entry_title else ""

            entry_type = _get_entry_type(toc_title, type_mapping)

            spell_class = None
            spell_level = None
            school = row.get("school")   # from silver (extracted before strip)
            sphere = row.get("sphere")   # from silver
            is_reversible = None
            ref_page = None

            if entry_type == "spell" and entry_name:
                # Look up from crosscheck — try with parsed class first, then either
                parsed_class = row.get("spell_class")
                xcheck = None
                if parsed_class:
                    xcheck = spell_lookup.get((sf, entry_name, parsed_class))
                if not xcheck:
                    # Try wizard then priest
                    xcheck = spell_lookup.get((sf, entry_name, "wizard"))
                if not xcheck:
                    xcheck = spell_lookup.get((sf, entry_name, "priest"))

                if xcheck:
                    spell_class = xcheck["spell_class"]
                    spell_level = xcheck["spell_level"]
                    school = xcheck["school"] or school
                    sphere = xcheck["sphere"] or sphere
                    is_reversible = xcheck["is_reversible"]
                    ref_page = xcheck["ref_page"]
                else:
                    # Not in crosscheck — use parsed values from silver
                    spell_class = parsed_class
                    spell_level = int(row["spell_level"]) if pd.notna(row.get("spell_level")) else None
                    # Spell not in any index — likely junk, demote to rule
                    if not spell_class:
                        entry_type = "rule"

            all_rows.append({
                "entry_id": int(row["entry_id"]),
                "source_file": sf,
                "entry_title": entry_title,
                "entry_type": entry_type,
                "spell_level": spell_level,
                "spell_class": spell_class,
                "school": school,
                "sphere": sphere,
                "is_reversible": is_reversible,
                "ref_page": ref_page,
                "components": _extract_field(content, "Components") or _extract_field(content, "Component"),
                "saving_throw": _extract_field(content, "Saving Throw"),
                "range_text": _extract_field(content, "Range"),
                "duration_text": _extract_field(content, "Duration"),
                "casting_time": _extract_field(content, "Casting Time"),
            })

    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame(
        columns=["entry_id", "source_file", "entry_title", "entry_type",
                 "spell_level", "spell_class", "school", "sphere", "is_reversible",
                 "ref_page", "components", "saving_throw", "range_text",
                 "duration_text", "casting_time"]
    )
