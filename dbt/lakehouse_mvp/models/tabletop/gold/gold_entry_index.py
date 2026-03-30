"""Gold: cross-reference index for structured queries.

Spell data comes from silver_spell_crosscheck (authoritative, fuzzy-matched across all appendixes).
Non-spell entry_type from config: class_names (with sub-section inheritance),
proficiency from authority whitelist. Default is rule.
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


def _build_class_toc_ids(toc_df, class_names_lower: set) -> set:
    """Walk ToC by sort_order. When a class name is hit, mark it and all
    deeper entries after it as class, until next entry at same or shallower depth."""
    toc_sorted = toc_df.sort_values("sort_order")
    class_toc_ids = set()
    current_class_depth = None

    for _, row in toc_sorted.iterrows():
        title = row["title"]
        depth = int(row["depth"])

        if current_class_depth is not None:
            if depth > current_class_depth:
                class_toc_ids.add(int(row["toc_id"]))
                continue
            else:
                current_class_depth = None

        if title.lower().strip() in class_names_lower:
            class_toc_ids.add(int(row["toc_id"]))
            current_class_depth = depth

    return class_toc_ids


def model(dbt, session):
    dbt.config(materialized="table")

    from dlt.lib.tabletop_cleanup import load_config
    from pathlib import Path
    import pandas as pd

    configs_dir = Path("/workspace/documents/tabletop_rules/configs")
    entries_df = dbt.ref("silver_entries").df()
    toc_df = dbt.ref("silver_toc_sections").df()
    crosscheck_df = dbt.ref("silver_spell_crosscheck").df()

    # Build spell lookup from crosscheck (authoritative)
    spell_lookup = {}
    for _, row in crosscheck_df.iterrows():
        # Store by (name, class) — some spells are both wizard and priest
        spell_lookup[(row["source_file"], row["entry_name"], row["entry_class"])] = {
            "spell_class": row["entry_class"],
            "spell_level": int(row["entry_level"]) if pd.notna(row["entry_level"]) else None,
            "school": row["school"],
            "sphere": row["sphere"],
            "is_reversible": row["is_reversible"],
            "ref_page": int(row["ref_page"]) if pd.notna(row["ref_page"]) else None,
        }

    # Load authority table entries for proficiency whitelist
    authority_df = session.execute(
        "SELECT source_file, entry_name, entry_type FROM bronze_tabletop.authority_table_entries"
    ).df()
    authority_whitelist = {}
    for _, arow in authority_df.iterrows():
        key = (arow["source_file"], arow["entry_type"])
        if key not in authority_whitelist:
            authority_whitelist[key] = set()
        authority_whitelist[key].add(arow["entry_name"].lower().strip())

    all_rows = []

    for sf in entries_df["source_file"].unique():
        config = load_config(Path(sf), configs_dir)

        # Build class toc_ids from config class_names
        class_names = config.get("class_names", [])
        class_names_lower = {n.lower().strip() for n in class_names}
        sf_toc = toc_df[toc_df["source_file"] == sf]
        class_toc_ids = _build_class_toc_ids(sf_toc, class_names_lower)

        # Proficiency whitelist for this file
        prof_whitelist = authority_whitelist.get((sf, "proficiency"), set())

        sf_entries = entries_df[entries_df["source_file"] == sf]

        for _, row in sf_entries.iterrows():
            content = row["content"]
            entry_title = row["entry_title"]
            entry_name = entry_title.lower().strip() if entry_title else ""
            toc_id = int(row["toc_id"])

            # Determine entry_type
            entry_type = "rule"

            if row.get("spell_class"):
                entry_type = "spell"
            elif toc_id in class_toc_ids:
                entry_type = "class"
            elif entry_name and entry_name in prof_whitelist:
                entry_type = "proficiency"

            # Entries with no title are always rules
            if not entry_name:
                entry_type = "rule"

            spell_class = None
            spell_level = None
            school = row.get("school")
            sphere = row.get("sphere")
            is_reversible = None
            ref_page = None

            if entry_type == "spell" and entry_name:
                # Look up from crosscheck — try with parsed class first, then either
                parsed_class = row.get("spell_class")
                xcheck = None
                if parsed_class:
                    xcheck = spell_lookup.get((sf, entry_name, parsed_class))
                if not xcheck:
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
                    spell_class = parsed_class
                    spell_level = int(row["spell_level"]) if pd.notna(row.get("spell_level")) else None
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
