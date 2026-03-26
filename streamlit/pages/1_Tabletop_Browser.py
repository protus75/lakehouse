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

# Style buttons to look like text links
st.markdown("""
<style>
    div[data-testid="stMainBlockContainer"] button[kind="secondary"] {
        background: none !important;
        border: none !important;
        padding: 0 !important;
        margin: 0 !important;
        color: #4a9eff !important;
        font-size: inherit !important;
        cursor: pointer !important;
        text-align: left !important;
        min-height: 0 !important;
        line-height: 1.4 !important;
    }
    div[data-testid="stMainBlockContainer"] button[kind="secondary"]:hover {
        text-decoration: underline !important;
        background: none !important;
        border: none !important;
    }
    div[data-testid="stMainBlockContainer"] button[kind="secondary"]:focus {
        box-shadow: none !important;
    }
    div[data-testid="stMainBlockContainer"] button[kind="secondary"] p {
        font-size: inherit !important;
    }
    .chapter-link button[kind="secondary"] {
        font-size: 1.05rem !important;
        font-weight: 600 !important;
        color: inherit !important;
    }
    .chapter-link button[kind="secondary"]:hover { color: #4a9eff !important; }
    .entry-link { padding-left: 1.5rem; }
    .entry-link button[kind="secondary"] { font-size: 0.92rem !important; }
    .section-entry-link { padding-left: 2.5rem; }
    .section-entry-link button[kind="secondary"] { font-size: 0.9rem !important; }
    div[data-testid="stMainBlockContainer"] .stElementContainer { margin-bottom: -0.6rem; }
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
        if idx.get("ref_page"):
            badges.append(f"p.{idx['ref_page']}")

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
    st.caption(f"Pages {chapter['page_start']}\u2013{chapter['page_end']}")
    st.divider()

    sections = get_chapter_sections(toc_id)

    if not sections:
        st.info("No entries in this chapter.")
    elif len(sections) == 1:
        for entry_title in sections[0]["entries"]:
            st.markdown('<div class="entry-link">', unsafe_allow_html=True)
            if st.button(entry_title, key=f"ce_{toc_id}_{entry_title}"):
                _nav_to_entry(toc_id, chapter["title"], entry_title)
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)
    else:
        for sec in sections:
            sec_label = sec["section"] if sec["section"] else chapter["title"]
            st.markdown(f"**{sec_label}**")
            for entry_title in sec["entries"]:
                st.markdown('<div class="section-entry-link">', unsafe_allow_html=True)
                if st.button(entry_title, key=f"ce_{toc_id}_{sec_label}_{entry_title}"):
                    _nav_to_entry(toc_id, chapter["title"], entry_title)
                    st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)

# ── View: Table of Contents (default) ────────────────────────

else:
    book_name = selected_book.replace(".pdf", "").replace("_", " ")
    st.title(book_name)
    st.markdown("### Table of Contents")
    st.divider()

    for row in toc_rows:
        entries = get_entry_list(row["toc_id"])
        entry_count = len(entries)

        st.markdown('<div class="chapter-link">', unsafe_allow_html=True)
        if st.button(
            f"{row['title']}  \u2014  pp. {row['page_start']}\u2013{row['page_end']}  ({entry_count})",
            key=f"toc_{row['toc_id']}",
        ):
            _nav_to_chapter(row["toc_id"], row["title"])
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

        for entry_title in entries:
            st.markdown('<div class="entry-link">', unsafe_allow_html=True)
            if st.button(entry_title, key=f"toce_{row['toc_id']}_{entry_title}"):
                _nav_to_entry(row["toc_id"], row["title"], entry_title)
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)
