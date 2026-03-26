"""Tabletop Rules Browser — book-style ToC with drill-down to entries."""
import sys
sys.path.insert(0, "/workspace/streamlit")
sys.path.insert(0, "/workspace")

import streamlit as st
import re
from lib.tabletop import (
    get_books, get_toc, get_entry_list, get_entry_content,
    get_entry_by_id, get_entry_index, get_summary, get_annotations,
    get_chapter_sections, search_entries, get_table_lookup,
)

# Compact styles — kill all Streamlit spacing bloat
st.markdown("""
<style>
    /* Kill whitespace between elements in main area */
    div[data-testid="stMainBlockContainer"] .stElementContainer {
        margin: 0 !important; padding: 0 !important;
    }
    div[data-testid="stMainBlockContainer"] .stMarkdown { margin: 0 !important; padding: 0 !important; }
    /* All buttons → text links */
    div[data-testid="stMainBlockContainer"] button[kind="secondary"] {
        background: none !important; border: none !important;
        padding: 0 !important; margin: 0 !important;
        color: #4a9eff !important; cursor: pointer !important;
        text-align: left !important; min-height: 0 !important;
        line-height: 1.6 !important; font-size: 0.9rem !important;
    }
    div[data-testid="stMainBlockContainer"] button[kind="secondary"]:hover {
        text-decoration: underline !important; background: none !important;
    }
    div[data-testid="stMainBlockContainer"] button[kind="secondary"]:focus {
        box-shadow: none !important;
    }
    div[data-testid="stMainBlockContainer"] button[kind="secondary"] p {
        font-size: inherit !important; margin: 0 !important;
    }
    /* ToC entry styles — inline in HTML */
    .toc-line { line-height: 1.5; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .toc-chapter { font-weight: 600; font-size: 1rem; margin-top: 0.3rem; }
    .toc-line a, .toc-chapter a, .toc-section a, .toc-table-entry a {
        color: #4a9eff !important; text-decoration: none !important;
    }
    .toc-line a:hover, .toc-chapter a:hover, .toc-section a:hover, .toc-table-entry a:hover {
        text-decoration: underline !important;
    }
    .toc-table-entry a { font-style: italic !important; }
</style>
""", unsafe_allow_html=True)


# ── Navigation via session state ─────────────────────────────
# Maintains a history stack so "Back" works within the page.

if "nav_history" not in st.session_state:
    st.session_state["nav_history"] = []
if "nav_current" not in st.session_state:
    st.session_state["nav_current"] = {"view": "toc"}


def _nav_push(new_state):
    """Push current state to history and navigate to new state."""
    st.session_state["nav_history"].append(st.session_state["nav_current"].copy())
    st.session_state["nav_current"] = new_state


def _nav_back():
    """Go back to previous state."""
    if st.session_state["nav_history"]:
        st.session_state["nav_current"] = st.session_state["nav_history"].pop()


def _nav_to_toc():
    _nav_push({"view": "toc"})

def _nav_to_chapter(toc_id, title):
    _nav_push({"view": "chapter", "toc_id": toc_id, "toc_title": title})

def _nav_to_entry(toc_id, toc_title, entry_title):
    _nav_push({"view": "entry", "toc_id": toc_id, "toc_title": toc_title,
               "entry_title": entry_title})

def _nav_to_entry_id(entry_id):
    _nav_push({"view": "entry_by_id", "entry_id": entry_id})


nav = st.session_state["nav_current"]
has_history = len(st.session_state["nav_history"]) > 0


# ── Sidebar: book selector + search ─────────────────────────

books = get_books()
if not books:
    st.warning("No books found in gold layer. Run the pipeline first.")
    st.stop()

selected_book = st.sidebar.selectbox("Book", books)

if st.sidebar.button("Table of Contents", key="sidebar_toc"):
    _nav_to_toc()
    st.rerun()

st.sidebar.divider()
st.sidebar.markdown("**Display**")
show_summary = st.sidebar.toggle("AI Summary", value=True)
show_content = st.sidebar.toggle("Full Content", value=True)
show_meta = st.sidebar.toggle("Entry Details", value=True)
st.sidebar.divider()

search_query = st.sidebar.text_input("Search entries", placeholder="e.g. Fireball, THAC0...")
if search_query:
    results = search_entries(search_query)
    if results.is_empty():
        st.sidebar.info("No results found.")
    else:
        st.sidebar.markdown(f"**{len(results)} results**")
        for row in results.iter_rows(named=True):
            label = f"{row['entry_title']}  *({row['entry_type']})*"
            if st.sidebar.button(label, key=f"search_{row['entry_id']}"):
                _nav_to_entry_id(row["entry_id"])
                st.rerun()


# ── Load ToC ─────────────────────────────────────────────────

toc = get_toc(selected_book)
if toc.is_empty():
    st.info("No table of contents for this book.")
    st.stop()

toc_rows = list(toc.iter_rows(named=True))
toc_lookup = {r["toc_id"]: r for r in toc_rows}

# ── Handle HTML link clicks (query params) ───────────────────

if "nav_toc_id" in st.query_params:
    clicked_id = int(st.query_params["nav_toc_id"])
    st.query_params.clear()
    clicked_row = toc_lookup.get(clicked_id)
    if clicked_row:
        _nav_to_chapter(clicked_id, clicked_row["title"])
        st.rerun()

if "nav_entry" in st.query_params:
    val = st.query_params["nav_entry"]
    st.query_params.clear()
    parts = val.split(":", 1)
    if len(parts) == 2:
        entry_toc_id = int(parts[0])
        entry_title = parts[1]
        entry_toc = toc_lookup.get(entry_toc_id, {})
        _nav_to_entry(entry_toc_id, entry_toc.get("title", ""), entry_title)
        st.rerun()


# ── Back button (always visible when there's history) ────────

def _show_back():
    if has_history:
        if st.button("\u2190 Back", key="nav_back"):
            _nav_back()
            st.rerun()


# ── Content renderer with table links ────────────────────────

_TABLE_REF = re.compile(r"\bTable\s+(\d+)\b")

def _render_content_with_table_links(content: str):
    """Render markdown content, turning 'Table N' references into clickable links."""
    table_lookup = get_table_lookup()

    # Split content into chunks: text vs table references
    parts = _TABLE_REF.split(content)
    # parts alternates: [text, table_num, text, table_num, ...]
    i = 0
    while i < len(parts):
        if i % 2 == 0:
            # Regular text
            text = parts[i]
            if text.strip():
                st.markdown(text)
        else:
            # Table number
            table_num = int(parts[i])
            info = table_lookup.get(table_num)
            if info:
                if st.button(
                    f"Table {table_num}: {info['entry_title'].split(':', 1)[-1].strip()}"
                    if ':' in info['entry_title'] else f"Table {table_num}",
                    key=f"tref_{table_num}_{i}",
                ):
                    _nav_to_entry(info["toc_id"], "", info["entry_title"])
                    st.rerun()
            else:
                st.markdown(f"Table {table_num}")
        i += 1


# ── Entry renderer ───────────────────────────────────────────

def _show_entry(entry_id, entry_title, source_file, toc_title, content, toc_id=None):
    _show_back()

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
    if toc_title:
        st.caption(toc_title)
    if show_meta and badges:
        st.markdown(" | ".join(badges))

    if show_summary and entry_id:
        summary = get_summary(entry_id)
        if summary:
            st.info(f"**AI Summary:** {summary}")

    st.divider()
    if show_content:
        _render_content_with_table_links(content)

    # Prev/Next
    if toc_id:
        all_entries = get_entry_list(toc_id)
        if entry_title in all_entries:
            current_idx = all_entries.index(entry_title)
            st.divider()
            c1, c2 = st.columns(2)
            with c1:
                if current_idx > 0:
                    prev_t = all_entries[current_idx - 1]
                    if st.button(f"\u2190 {prev_t}", key="nav_prev"):
                        # Replace current instead of pushing (sequential reading)
                        st.session_state["nav_current"] = {
                            "view": "entry", "toc_id": toc_id,
                            "toc_title": toc_title, "entry_title": prev_t,
                        }
                        st.rerun()
            with c2:
                if current_idx < len(all_entries) - 1:
                    next_t = all_entries[current_idx + 1]
                    if st.button(f"{next_t} \u2192", key="nav_next"):
                        st.session_state["nav_current"] = {
                            "view": "entry", "toc_id": toc_id,
                            "toc_title": toc_title, "entry_title": next_t,
                        }
                        st.rerun()


# ── View: Search result (by entry_id) ───────────────────────

if nav["view"] == "entry_by_id":
    entry = get_entry_by_id(nav["entry_id"])
    if entry:
        toc_id = None
        toc_title = entry["toc_title"]
        for r in toc_rows:
            if r["title"] == toc_title:
                toc_id = r["toc_id"]
                break
        _show_entry(
            entry["entry_id"], entry["entry_title"],
            entry["source_file"], toc_title, entry["content"],
            toc_id=toc_id,
        )
    else:
        st.warning("Entry not found.")

# ── View: Entry from ToC browse ──────────────────────────────

elif nav["view"] == "entry":
    toc_id = nav["toc_id"]
    toc_title = nav.get("toc_title") or toc_lookup.get(toc_id, {}).get("title", "")
    entry_title = nav["entry_title"]
    content = get_entry_content(toc_id, entry_title)

    idx = get_entry_index(entry_title, selected_book)
    entry_id = idx["entry_id"] if idx else None

    _show_entry(entry_id, entry_title, selected_book, toc_title, content,
                toc_id=toc_id)

# ── View: Chapter drill-down ─────────────────────────────────

elif nav["view"] == "chapter":
    toc_id = nav["toc_id"]
    chapter = toc_lookup.get(toc_id)
    if not chapter:
        st.warning("Chapter not found.")
        st.stop()

    _show_back()

    st.header(chapter["title"])
    st.divider()

    sections = get_chapter_sections(toc_id)

    if not sections:
        st.info("No entries in this chapter.")
    else:
        html = []
        for sec in sections:
            if len(sections) > 1:
                sec_label = sec["section"] if sec["section"] else chapter["title"]
                html.append(f'<div style="font-weight:600; margin-top:0.5rem;">{sec_label}</div>')
            for entry_title in sec["entries"]:
                indent = "1.5rem" if len(sections) > 1 else "0"
                html.append(
                    f'<div class="toc-line toc-section" style="padding-left:{indent}">'
                    f'<a href="?nav_entry={toc_id}:{entry_title}" target="_self">{entry_title}</a></div>'
                )
        st.markdown("\n".join(html), unsafe_allow_html=True)


# ── View: Table of Contents (default) ────────────────────────

else:
    book_name = selected_book.replace(".pdf", "").replace("_", " ")
    st.title(book_name)
    st.markdown("### Table of Contents")
    st.divider()

    # Render entire ToC as one compact HTML block with clickable links
    html = []
    for row in toc_rows:
        depth = int(row.get("depth", 0))
        is_table = bool(row.get("is_table", False))
        is_chapter = bool(row.get("is_chapter", False))
        indent = depth * 1.5

        if is_chapter:
            css = "toc-chapter"
        elif is_table:
            css = "toc-table-entry"
        else:
            css = "toc-section"

        html.append(
            f'<div class="toc-line {css}" style="padding-left:{indent}rem">'
            f'<a href="?nav_toc_id={row["toc_id"]}" target="_self">{row["title"]}</a></div>'
        )
    st.markdown("\n".join(html), unsafe_allow_html=True)
