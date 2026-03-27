"""Shared database connection for all Streamlit apps.

Creates a DuckDB connection with Iceberg views. Cached per Streamlit
script run via st.cache_data with a short TTL. On each new browser
session or after TTL expires, a fresh connection is created that
reads current Iceberg metadata from S3.
"""
import sys
sys.path.insert(0, "/workspace")

import streamlit as st
import polars as pl
from dlt.lib.duckdb_reader import get_reader


@st.cache_resource(ttl=30)
def _conn():
    return get_reader(namespaces=["silver_tabletop", "gold_tabletop"])


def reset_connection():
    """Force connection refresh (call after pipeline runs)."""
    _conn.clear()


def query(sql: str, params: list | None = None) -> pl.DataFrame:
    """Execute SQL and return Polars DataFrame."""
    conn = _conn()
    if params:
        result = conn.execute(sql, params).fetchdf()
    else:
        result = conn.execute(sql).fetchdf()
    return pl.from_pandas(result)
