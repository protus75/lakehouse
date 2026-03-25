"""Silver spell cross-check: reconcile all 4 appendix sources with fuzzy name matching.

Produces one authoritative row per spell with class, level, school, sphere, is_reversible.
Uses rapidfuzz to match names across appendixes that differ in formatting
(e.g. "10' radius" vs "10-foot radius", "tasha's uncontrollable hideous laughter" vs "hideous laughter").
"""
import sys
sys.path.insert(0, "/workspace")


def _fuzzy_match(name: str, candidates: dict, threshold: int = 80) -> str | None:
    """Find the best fuzzy match for name in candidates dict (name → row).
    Returns matched name or None."""
    from rapidfuzz import fuzz
    best_score = 0
    best_match = None
    for candidate in candidates:
        score = fuzz.ratio(name, candidate)
        if score > best_score and score >= threshold:
            best_score = score
            best_match = candidate
        # Also try partial matching for abbreviated names
        partial = fuzz.partial_ratio(name, candidate)
        if partial > best_score and partial >= threshold + 10:
            best_score = partial
            best_match = candidate
    return best_match


def model(dbt, session):
    dbt.config(materialized="table")

    import pandas as pd
    from dlt.lib.tabletop_cleanup import load_config
    from pathlib import Path

    configs_dir = Path("/workspace/documents/tabletop_rules/configs")

    # Load all sources
    known_df = dbt.ref("silver_known_entries").df()
    spell_list_df = dbt.source("bronze_tabletop", "spell_list_entries").df()

    # Index spells (Appendix 7) — primary source for name + class + level
    index_spells = known_df[known_df["entry_class"].notna()].copy()

    # School data (Appendix 5)
    school_df = dbt.source("bronze_tabletop", "known_entries_raw").df()
    school_data = school_df[school_df["school"].notna()][["source_file", "entry_name", "school"]].copy()
    school_lookup = {}
    for _, row in school_data.iterrows():
        school_lookup[row["entry_name"]] = row["school"]

    # Sphere data (Appendix 6)
    sphere_data = school_df[school_df["sphere"].notna()][["source_file", "entry_name", "sphere"]].copy()
    sphere_lookup = {}
    for _, row in sphere_data.iterrows():
        sphere_lookup[row["entry_name"]] = row["sphere"]

    # Spell list (Appendix 1) — keyed by (name, class)
    list_lookup = {}
    for _, row in spell_list_df.iterrows():
        key = (row["entry_name"], row["entry_class"])
        list_lookup[key] = {
            "entry_level": row["entry_level"],
            "is_reversible": bool(row["is_reversible"]),
        }
    list_names = {row["entry_name"] for _, row in spell_list_df.iterrows()}

    # Load config for excludes
    source_files = index_spells["source_file"].unique()
    exclude_names = set()
    for sf in source_files:
        config = load_config(Path(sf), configs_dir)
        exclude_names.update(config.get("exclude_entry_names", []))

    all_rows = []
    for _, spell in index_spells.iterrows():
        name = spell["entry_name"]
        cls = spell["entry_class"]
        level = spell["entry_level"]
        ref_page = spell.get("ref_page")
        sf = spell["source_file"]

        if name in exclude_names:
            continue

        # Exact match first, then fuzzy
        school = school_lookup.get(name)
        if not school:
            match = _fuzzy_match(name, school_lookup, threshold=80)
            if match:
                school = school_lookup[match]

        sphere = sphere_lookup.get(name)
        if not sphere:
            match = _fuzzy_match(name, sphere_lookup, threshold=80)
            if match:
                sphere = sphere_lookup[match]

        # Spell list match
        list_key = (name, cls)
        list_entry = list_lookup.get(list_key)
        in_spell_list = list_entry is not None
        if not in_spell_list:
            # Fuzzy match against spell list names
            match = _fuzzy_match(name, {n: None for n in list_names}, threshold=80)
            if match:
                list_entry = list_lookup.get((match, cls))
                in_spell_list = list_entry is not None

        is_reversible = list_entry["is_reversible"] if list_entry else None
        level_mismatch = False
        if list_entry and list_entry["entry_level"] is not None and level is not None:
            try:
                level_mismatch = int(list_entry["entry_level"]) != int(level)
            except (ValueError, TypeError):
                pass

        all_rows.append({
            "source_file": sf,
            "entry_name": name,
            "entry_class": cls,
            "entry_level": int(level) if pd.notna(level) else None,
            "ref_page": int(ref_page) if pd.notna(ref_page) else None,
            "school": school,
            "sphere": sphere,
            "is_reversible": is_reversible,
            "in_spell_list": in_spell_list,
            "in_school_index": school is not None,
            "in_sphere_index": sphere is not None,
            "level_mismatch": level_mismatch,
        })

    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame(
        columns=["source_file", "entry_name", "entry_class", "entry_level",
                 "ref_page", "school", "sphere", "is_reversible",
                 "in_spell_list", "in_school_index", "in_sphere_index", "level_mismatch"]
    )
