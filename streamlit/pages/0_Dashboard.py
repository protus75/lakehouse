import sys
sys.path.insert(0, "/workspace/streamlit")
sys.path.insert(0, "/workspace")

import streamlit as st
import polars as pl
import plotly.express as px
from lib.connection import query

st.title("Sales Dashboard")


@st.cache_data(ttl=60)
def load_data() -> pl.DataFrame:
    df = query("SELECT * FROM marts.daily_revenue")
    df = df.with_columns(pl.col("order_date").cast(pl.Date))
    return df


df = load_data()

# ── Sidebar Filters ───────────────────────────────────────────
st.sidebar.header("Filters")

regions = ["All"] + sorted(df["region"].unique().to_list())
selected_region = st.sidebar.selectbox("Region", regions)

date_min = df["order_date"].min()
date_max = df["order_date"].max()
date_range = st.sidebar.date_input("Date Range", [date_min, date_max])

# Apply filters
filtered = df
if selected_region != "All":
    filtered = filtered.filter(pl.col("region") == selected_region)
if len(date_range) == 2:
    start, end = date_range
    filtered = filtered.filter(
        (pl.col("order_date") >= start) & (pl.col("order_date") <= end)
    )

# ── KPI Metrics ───────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Revenue",    f"${filtered['revenue'].sum():,.2f}")
c2.metric("Total Orders",     f"{filtered['order_count'].sum():,}")
c3.metric("Unique Customers", f"{filtered['customer_count'].sum():,}")
c4.metric("Avg Order Value",  f"${filtered['avg_order_value'].mean():,.2f}")

st.divider()

# ── Charts ────────────────────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    daily = filtered.group_by("order_date").agg(pl.col("revenue").sum()).sort("order_date")
    st.plotly_chart(
        px.line(daily, x="order_date", y="revenue", title="Daily Revenue Trend"),
        use_container_width=True,
    )

with col_right:
    by_region = filtered.group_by("region").agg(pl.col("revenue").sum()).sort("region")
    st.plotly_chart(
        px.bar(by_region, x="region", y="revenue", title="Revenue by Region", color="region"),
        use_container_width=True,
    )

by_product = (
    filtered.group_by("product")
    .agg(pl.col("revenue").sum())
    .sort("revenue", descending=True)
)
st.plotly_chart(
    px.bar(by_product, x="product", y="revenue", title="Revenue by Product", color="product"),
    use_container_width=True,
)

st.subheader("Detailed Data")
st.dataframe(filtered.sort("order_date", descending=True), use_container_width=True)
