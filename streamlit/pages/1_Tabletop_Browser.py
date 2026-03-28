"""Tabletop Rules Browser — full scrollable book with ToC sidebar navigation."""
import sys
sys.path.insert(0, "/workspace/streamlit")
sys.path.insert(0, "/workspace")

import streamlit as st
from lib.tabletop import (
    get_books, get_toc, get_full_book, get_entry_index,
    get_summary, get_description, get_annotations, search_entries,
)

# Compact styling
st.markdown("""
<style>
    .toc-line { line-height: 1.5; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .toc-chapter { font-weight: 600; font-size: 1rem; margin-top: 0.3rem; }
    .toc-section { }
    .toc-table-entry { font-style: italic; }
    .toc-line a { color: #4a9eff !important; text-decoration: none !important; }
    .toc-line a:hover { text-decoration: underline !important; }
    .book-chapter { margin-top: 2rem; padding-top: 1rem; border-top: 2px solid #333; }
    .book-entry { margin-top: 1rem; }
    .book-entry-title { font-weight: 600; font-size: 1.05rem; color: #4a9eff; margin-bottom: 0.2rem; }
    .book-badges { font-size: 0.8rem; color: #888; margin-bottom: 0.3rem; }
    .book-summary { background: #1a1a2e; border-left: 3px solid #4a9eff; padding: 0.5rem 0.8rem;
                    margin: 0.3rem 0; font-size: 0.9rem; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ──────────────────────────────────────────────────

books = get_books()
if not books:
    st.warning("No books found in gold layer. Run the pipeline first.")
    st.stop()

selected_book = st.sidebar.selectbox("Book", books)

st.sidebar.divider()
st.sidebar.markdown("**Display**")
show_summary = st.sidebar.toggle("AI Summary", value=True)
show_meta = st.sidebar.toggle("Entry Details", value=True)
st.sidebar.divider()

# Search
search_query = st.sidebar.text_input("Search entries", placeholder="e.g. Fireball, THAC0...")
if search_query:
    results = search_entries(search_query)
    if results.is_empty():
        st.sidebar.info("No results found.")
    else:
        st.sidebar.markdown(f"**{len(results)} results**")
        for row in results.iter_rows(named=True):
            st.sidebar.markdown(
                f'<div class="toc-line toc-section">'
                f'<a href="#entry-{row["entry_id"]}" target="_self">'
                f'{row["entry_title"]} ({row["entry_type"]})</a></div>',
                unsafe_allow_html=True,
            )

st.sidebar.divider()

# ToC navigation in sidebar
toc = get_toc(selected_book)
if toc.is_empty():
    st.info("No table of contents for this book.")
    st.stop()

toc_rows = list(toc.iter_rows(named=True))

st.sidebar.markdown("**Table of Contents**")
toc_html = []
for row in toc_rows:
    depth = int(row.get("depth", 0))
    is_table = bool(row.get("is_table", False))
    is_chapter = bool(row.get("is_chapter", False))
    indent = depth * 1.2

    if is_chapter:
        css = "toc-chapter"
    elif is_table:
        css = "toc-table-entry"
    else:
        css = "toc-section"

    toc_html.append(
        f'<div class="toc-line {css}" style="padding-left:{indent}rem">'
        f'<a href="#toc-{row["toc_id"]}" target="_self">{row["title"]}</a></div>'
    )
st.sidebar.markdown("\n".join(toc_html), unsafe_allow_html=True)

# Build set of ToC titles so we can add toc anchors to matching entries
toc_title_to_id = {row["title"]: row["toc_id"] for row in toc_rows}


# ── Main content: full book ──────────────────────────────────

book_name = selected_book.replace(".pdf", "").replace("_", " ")
st.title(book_name)

# Load full book content
book_data = get_full_book(selected_book)
if book_data.is_empty():
    st.info("No content found. Run the pipeline first.")
    st.stop()

# Build the document: group chunks by toc section, then by entry
current_toc_id = None
current_entry = None
content_buffer = []
skip_entry_content = False


def _flush_entry():
    """Render accumulated entry content."""
    global content_buffer, current_entry
    if not content_buffer:
        return
    content = "\n".join(content_buffer)
    if content.strip():
        st.markdown(content)
    content_buffer = []


def _render_badges(entry_title, source_file):
    """Render entry badges and description/summary content.

    Badges always show. For spell entries, the AI Summary toggle swaps
    between the original clean description and the AI summary.
    """
    if not show_meta:
        return None
    idx = get_entry_index(entry_title, source_file)
    if not idx:
        return None
    badges = []
    badges.append(idx["entry_type"])
    if idx.get("spell_level"):
        badges.append(f"Level {idx['spell_level']}")
    if idx.get("spell_class"):
        badges.append(idx["spell_class"])
    if idx.get("school"):
        badges.append(idx["school"])
    if idx.get("sphere"):
        badges.append(idx["sphere"])

    entry_id = idx["entry_id"]
    is_spell = idx["entry_type"] == "spell"
    annotations = get_annotations(entry_id)
    if annotations:
        if annotations.get("is_combat"):
            badges.append("Combat")
        if annotations.get("is_popular"):
            badges.append("Popular")

    if badges:
        st.markdown(f'<div class="book-badges">{" · ".join(badges)}</div>',
                    unsafe_allow_html=True)

    if is_spell:
        if show_summary:
            summary = get_summary(entry_id)
            if summary:
                st.markdown(f'<div class="book-summary">{summary}</div>',
                            unsafe_allow_html=True)
                return "skip_content"
        else:
            description = get_description(entry_id)
            if description:
                st.markdown(description)
                return "skip_content"
    else:
        if show_summary:
            summary = get_summary(entry_id)
            if summary:
                st.markdown(f'<div class="book-summary">{summary}</div>',
                            unsafe_allow_html=True)

    return None


for row in book_data.iter_rows(named=True):
    toc_id = row["toc_id"]
    toc_title = row["toc_title"]
    entry_title = row["entry_title"]
    is_chapter = row["is_chapter"]

    # New ToC section — render chapter/section heading
    if toc_id != current_toc_id:
        _flush_entry()
        current_entry = None
        skip_entry_content = False
        current_toc_id = toc_id

        depth = row["depth"]
        anchor = f"toc-{toc_id}"

        if is_chapter:
            st.markdown(f'<div class="book-chapter" id="{anchor}"></div>',
                        unsafe_allow_html=True)
            heading_level = min(depth + 1, 4)
            st.markdown(f"{'#' * heading_level} {toc_title}")
        else:
            st.markdown(f'<div id="{anchor}"></div>', unsafe_allow_html=True)

    # New entry within section
    if entry_title and entry_title != current_entry:
        _flush_entry()
        current_entry = entry_title

        idx = get_entry_index(entry_title, selected_book)
        entry_id = idx["entry_id"] if idx else None
        anchor_id = f"entry-{entry_id}" if entry_id else ""

        # Add toc anchor if this entry title matches a ToC section title
        toc_anchor = ""
        matched_toc_id = toc_title_to_id.get(entry_title)
        if matched_toc_id:
            toc_anchor = f'<div id="toc-{matched_toc_id}"></div>'

        st.markdown(
            f'{toc_anchor}'
            f'<div class="book-entry" id="{anchor_id}">'
            f'<div class="book-entry-title">{entry_title}</div></div>',
            unsafe_allow_html=True,
        )
        result = _render_badges(entry_title, selected_book)
        skip_entry_content = result == "skip_content"

    # Accumulate content (skip if description/summary already rendered)
    if not skip_entry_content:
        content_buffer.append(row["content"])

# Flush last entry
_flush_entry()
