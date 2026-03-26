"""Tabletop RPG rules — query functions for browser and compendium pages.

All functions use lib.connection.query() for data access.
"""
import streamlit as st
import polars as pl
from lib.connection import query


# ── Books & ToC ──────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_books() -> list[str]:
    df = query("SELECT source_file FROM gold_tabletop.gold_files ORDER BY source_file")
    return df["source_file"].to_list()


@st.cache_data(ttl=300)
def get_toc(source_file: str) -> pl.DataFrame:
    return query(
        "SELECT toc_id, title, page_start, page_end "
        "FROM gold_tabletop.gold_toc "
        "WHERE source_file = ? AND is_excluded = false "
        "ORDER BY page_start",
        [source_file],
    )


# ── Entries ──────────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_entry_list(toc_id: int) -> list[str]:
    df = query(
        "SELECT DISTINCT entry_title FROM gold_tabletop.gold_chunks "
        "WHERE toc_id = ? ORDER BY entry_title",
        [toc_id],
    )
    return df["entry_title"].to_list()


@st.cache_data(ttl=300)
def get_entry_content(toc_id: int, entry_title: str) -> str:
    df = query(
        "SELECT content FROM gold_tabletop.gold_chunks "
        "WHERE toc_id = ? AND entry_title = ? ORDER BY chunk_id",
        [toc_id, entry_title],
    )
    if df.is_empty():
        return ""
    chunks = df["content"].to_list()
    # Deduplicate overlapping chunks (200-char overlap)
    result = chunks[0]
    for chunk in chunks[1:]:
        overlap = min(200, len(result), len(chunk))
        if overlap > 0 and result[-overlap:] in chunk[:overlap * 2]:
            start = chunk.index(result[-overlap:]) + overlap
            result += chunk[start:]
        else:
            result += "\n" + chunk
    return result


@st.cache_data(ttl=300)
def get_entry_by_id(entry_id: int) -> dict | None:
    df = query(
        "SELECT entry_id, source_file, entry_title, toc_title, content "
        "FROM silver_tabletop.silver_entries WHERE entry_id = ?",
        [entry_id],
    )
    if df.is_empty():
        return None
    return df.row(0, named=True)


@st.cache_data(ttl=300)
def get_entry_index(entry_title: str, source_file: str) -> dict | None:
    df = query(
        "SELECT entry_id, entry_type, spell_level, spell_class, school, sphere, "
        "is_reversible, ref_page "
        "FROM gold_tabletop.gold_entry_index "
        "WHERE entry_title = ? AND source_file = ?",
        [entry_title, source_file],
    )
    if df.is_empty():
        return None
    return df.row(0, named=True)


# ── AI Enrichments ───────────────────────────────────────────

@st.cache_data(ttl=300)
def get_summary(entry_id: int) -> str | None:
    df = query(
        "SELECT summary FROM gold_tabletop.gold_ai_summaries WHERE entry_id = ?",
        [entry_id],
    )
    if df.is_empty():
        return None
    return df["summary"][0]


@st.cache_data(ttl=300)
def get_annotations(entry_id: int) -> dict | None:
    df = query(
        "SELECT is_combat, is_popular FROM gold_tabletop.gold_ai_annotations "
        "WHERE entry_id = ?",
        [entry_id],
    )
    if df.is_empty():
        return None
    return df.row(0, named=True)


# ── Search ───────────────────────────────────────────────────

@st.cache_data(ttl=60)
def search_entries(search_query: str, limit: int = 20) -> pl.DataFrame:
    like = f"%{search_query}%"
    return query(
        "SELECT DISTINCT e.entry_id, e.entry_title, e.toc_title, i.entry_type "
        "FROM silver_tabletop.silver_entries e "
        "JOIN gold_tabletop.gold_entry_index i ON e.entry_id = i.entry_id "
        "WHERE e.entry_title ILIKE ? OR e.content ILIKE ? "
        "LIMIT ?",
        [like, like, limit],
    )


# ── Compendium ───────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_compendium(
    entry_type: str,
    spell_classes: list[str] | None = None,
    level_min: int | None = None,
    level_max: int | None = None,
    school: str | None = None,
    sphere: str | None = None,
    is_reversible: bool | None = None,
    combat_only: bool = False,
    popular_only: bool = False,
    search: str | None = None,
) -> pl.DataFrame:
    sql = """
        SELECT i.entry_id, i.entry_title, i.entry_type, i.spell_level,
               i.spell_class, i.school, i.sphere, i.is_reversible, i.ref_page,
               s.summary, a.is_combat, a.is_popular
        FROM gold_tabletop.gold_entry_index i
        LEFT JOIN gold_tabletop.gold_ai_summaries s ON i.entry_id = s.entry_id
        LEFT JOIN gold_tabletop.gold_ai_annotations a ON i.entry_id = a.entry_id
        WHERE i.entry_type = ?
    """
    params: list = [entry_type]

    if spell_classes:
        placeholders = ",".join(["?"] * len(spell_classes))
        sql += f" AND i.spell_class IN ({placeholders})"
        params.extend(spell_classes)

    if level_min is not None:
        sql += " AND i.spell_level >= ?"
        params.append(level_min)
    if level_max is not None:
        sql += " AND i.spell_level <= ?"
        params.append(level_max)

    if school:
        sql += " AND i.school = ?"
        params.append(school)
    if sphere:
        sql += " AND i.sphere = ?"
        params.append(sphere)
    if is_reversible is not None:
        sql += " AND i.is_reversible = ?"
        params.append(is_reversible)
    if combat_only:
        sql += " AND a.is_combat = true"
    if popular_only:
        sql += " AND a.is_popular = true"
    if search:
        sql += " AND i.entry_title ILIKE ?"
        params.append(f"%{search}%")

    sql += " ORDER BY i.entry_title"
    return query(sql, params)


@st.cache_data(ttl=300)
def get_filter_options(entry_type: str) -> dict:
    """Get distinct values for filter dropdowns."""
    df = query(
        "SELECT DISTINCT spell_class, school, sphere "
        "FROM gold_tabletop.gold_entry_index WHERE entry_type = ?",
        [entry_type],
    )
    return {
        "classes": sorted([v for v in df["spell_class"].unique().to_list() if v]),
        "schools": sorted([v for v in df["school"].unique().to_list() if v]),
        "spheres": sorted([v for v in df["sphere"].unique().to_list() if v]),
    }
