"""Tabletop Rules Browser — navigate ToC, search entries, view content with AI summaries."""
import sys
sys.path.insert(0, "/workspace/streamlit")
sys.path.insert(0, "/workspace")

import streamlit as st
from lib.tabletop import (
    get_books, get_toc, get_entry_list, get_entry_content,
    get_entry_by_id, get_entry_index, get_summary, get_annotations,
    search_entries,
)

st.title("Rules Browser")

# ── Sidebar ──────────────────────────────────────────────────
books = get_books()
if not books:
    st.warning("No books found in gold layer. Run the pipeline first.")
    st.stop()

selected_book = st.sidebar.selectbox("Book", books)

# Search
search_query = st.sidebar.text_input("Search entries")
if search_query:
    results = search_entries(search_query)
    if results.is_empty():
        st.sidebar.info("No results found.")
    else:
        st.sidebar.markdown(f"**{len(results)} results**")
        for row in results.iter_rows(named=True):
            label = f"{row['entry_title']}  ({row['entry_type']})"
            if st.sidebar.button(label, key=f"search_{row['entry_id']}"):
                st.session_state["view_entry_id"] = row["entry_id"]
                st.rerun()

st.sidebar.divider()

# ToC tree
toc = get_toc(selected_book)
if toc.is_empty():
    st.info("No table of contents for this book.")
    st.stop()

for row in toc.iter_rows(named=True):
    entries = get_entry_list(row["toc_id"])
    entry_count = len(entries)
    label = f"{row['title']} ({entry_count})"

    with st.sidebar.expander(label, expanded=False):
        for entry_title in entries:
            entry_label = str(entry_title) if entry_title else "(untitled)"
            if st.button(entry_label, key=f"entry_{row['toc_id']}_{entry_title}"):
                st.session_state["view_toc_id"] = row["toc_id"]
                st.session_state["view_toc_title"] = row["title"]
                st.session_state["view_entry_title"] = entry_title
                st.session_state.pop("view_entry_id", None)
                st.rerun()

# ── Main Content ─────────────────────────────────────────────


def _show_entry(entry_id, entry_title, source_file, toc_title, content):
    """Render an entry with badges, AI summary, and content."""
    idx = get_entry_index(entry_title, source_file)

    badges = []
    if idx:
        entry_id = entry_id or idx["entry_id"]
        badges.append(f"**{idx['entry_type']}**")
        if idx.get("spell_level"):
            badges.append(f"Level {idx['spell_level']}")
        if idx.get("spell_class"):
            badges.append(idx["spell_class"])
        if idx.get("school"):
            badges.append(idx["school"])
        if idx.get("sphere"):
            badges.append(idx["sphere"])

    if entry_id:
        annotations = get_annotations(entry_id)
        if annotations:
            if annotations.get("is_combat"):
                badges.append("Combat")
            if annotations.get("is_popular"):
                badges.append("Popular")

    st.header(entry_title)
    if badges:
        st.markdown(" | ".join(badges))

    if entry_id:
        summary = get_summary(entry_id)
        if summary:
            st.info(f"**AI Summary:** {summary}")

    st.caption(f"{toc_title} | {source_file}")
    st.divider()
    st.markdown(content)


# Handle search result click (by entry_id)
if "view_entry_id" in st.session_state:
    entry = get_entry_by_id(st.session_state["view_entry_id"])
    if entry:
        _show_entry(
            entry["entry_id"], entry["entry_title"],
            entry["source_file"], entry["toc_title"], entry["content"],
        )
    else:
        st.warning("Entry not found.")

# Handle ToC browse click
elif "view_entry_title" in st.session_state:
    toc_id = st.session_state["view_toc_id"]
    toc_title = st.session_state["view_toc_title"]
    entry_title = st.session_state["view_entry_title"]
    content = get_entry_content(toc_id, entry_title)

    idx = get_entry_index(entry_title, selected_book)
    entry_id = idx["entry_id"] if idx else None

    _show_entry(entry_id, entry_title, selected_book, toc_title, content)

else:
    st.markdown("Select a section from the sidebar to browse, or search for an entry.")
