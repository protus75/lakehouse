"""Shared database connection for all Streamlit apps.

Provides a cached DuckDB connection with Iceberg views and a query helper
that returns Polars DataFrames. All project-specific query modules import
from here.
"""
import sys
sys.path.insert(0, "/workspace")

import streamlit as st
import polars as pl
from dlt.lib.duckdb_reader import get_reader


@st.cache_resource
def _conn():
    return get_reader(namespaces=["silver_tabletop", "gold_tabletop"])


def query(sql: str, params: list | None = None) -> pl.DataFrame:
    """Execute SQL and return Polars DataFrame."""
    conn = _conn()
    if params:
        result = conn.execute(sql, params).fetchdf()
    else:
        result = conn.execute(sql).fetchdf()
    return pl.from_pandas(result)
