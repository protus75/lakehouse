"""Lakehouse Gold Layer — Streamlit multi-page app."""
import streamlit as st

st.set_page_config(
    page_title="Lakehouse",
    page_icon=":",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("Lakehouse")
st.markdown(
    "Browse tabletop RPG rules, spells, and reference material "
    "from the gold layer."
)
st.markdown("Use the sidebar to navigate between pages.")
