"""Tabletop Compendium — filterable card grid of spells, proficiencies, and more."""
import sys
sys.path.insert(0, "/workspace/streamlit")
sys.path.insert(0, "/workspace")

import streamlit as st
from lib.tabletop import get_compendium, get_filter_options, get_entry_by_id, get_summary

st.title("Compendium")

ENTRY_TYPES = ["spell", "proficiency", "class", "race", "equipment", "rule"]
TYPE_COLORS = {
    "spell": "#4a90d9", "proficiency": "#7cb342", "class": "#f4511e",
    "race": "#ab47bc", "equipment": "#ff8f00", "rule": "#546e7a",
}

# ── Sidebar Filters ──────────────────────────────────────────

entry_type = st.sidebar.selectbox("Entry Type", ENTRY_TYPES, index=0)
search = st.sidebar.text_input("Search by name")

# Type-specific filters
spell_classes = None
level_range = None
school = None
sphere = None
is_reversible = None

if entry_type == "spell":
    opts = get_filter_options("spell")

    if opts["classes"]:
        spell_classes = st.sidebar.multiselect("Class", opts["classes"])
        if not spell_classes:
            spell_classes = None

    level_range = st.sidebar.slider("Spell Level", 1, 9, (1, 9))

    if opts["schools"]:
        school_options = ["All"] + opts["schools"]
        school_sel = st.sidebar.selectbox("School", school_options)
        school = None if school_sel == "All" else school_sel

    if opts["spheres"]:
        sphere_options = ["All"] + opts["spheres"]
        sphere_sel = st.sidebar.selectbox("Sphere", sphere_options)
        sphere = None if sphere_sel == "All" else sphere_sel

    is_rev = st.sidebar.checkbox("Reversible only")
    is_reversible = True if is_rev else None

st.sidebar.divider()
combat_only = st.sidebar.checkbox("Combat-relevant only")
popular_only = st.sidebar.checkbox("Popular only")

# ── Query ────────────────────────────────────────────────────

df = get_compendium(
    entry_type=entry_type,
    spell_classes=spell_classes,
    level_min=level_range[0] if level_range else None,
    level_max=level_range[1] if level_range else None,
    school=school,
    sphere=sphere,
    is_reversible=is_reversible,
    combat_only=combat_only,
    popular_only=popular_only,
    search=search if search else None,
)

st.markdown(f"**{len(df)} entries**")

if df.is_empty():
    st.info("No entries match the current filters.")
    st.stop()

# ── Card Grid ────────────────────────────────────────────────

CARDS_PER_ROW = 3

for row_start in range(0, len(df), CARDS_PER_ROW):
    cols = st.columns(CARDS_PER_ROW)
    for col_idx, col in enumerate(cols):
        idx = row_start + col_idx
        if idx >= len(df):
            break

        row = df.row(idx, named=True)
        entry_id = row["entry_id"]
        title = row["entry_title"]

        # Build card badges
        badges = []
        if row.get("spell_level"):
            badges.append(f"Lvl {row['spell_level']}")
        if row.get("spell_class"):
            badges.append(row["spell_class"])
        if row.get("school"):
            badges.append(row["school"])
        if row.get("sphere"):
            badges.append(row["sphere"])
        if row.get("is_reversible"):
            badges.append("Rev")
        if row.get("is_combat"):
            badges.append("Combat")
        if row.get("is_popular"):
            badges.append("Popular")

        badge_str = " | ".join(badges) if badges else ""

        # Summary snippet
        summary = row.get("summary") or ""
        snippet = summary[:120] + "..." if len(summary) > 120 else summary

        with col:
            with st.container(border=True):
                st.markdown(f"**{title}**")
                if badge_str:
                    st.caption(badge_str)
                if snippet:
                    st.markdown(f"*{snippet}*")

                if st.button("View", key=f"view_{entry_id}"):
                    st.session_state["compendium_detail"] = entry_id

# ── Detail Panel ─────────────────────────────────────────────

if "compendium_detail" in st.session_state:
    detail_id = st.session_state["compendium_detail"]
    entry = get_entry_by_id(detail_id)

    if entry:
        st.divider()
        st.header(entry["entry_title"])

        summary = get_summary(detail_id)
        if summary:
            st.info(f"**AI Summary:** {summary}")

        st.caption(f"{entry['toc_title']} | {entry['source_file']}")
        st.markdown(entry["content"])

        if st.button("Close"):
            del st.session_state["compendium_detail"]
            st.rerun()
