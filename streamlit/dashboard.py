import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Lakehouse Sales Dashboard", layout="wide")
st.title("Sales Dashboard")

DB_PATH = "/workspace/db/lakehouse.duckdb"


@st.cache_data(ttl=60)
def load_data() -> pd.DataFrame:
    conn = duckdb.connect(DB_PATH, read_only=True)
    df = conn.execute("SELECT * FROM marts.daily_revenue").df()
    conn.close()
    df["order_date"] = pd.to_datetime(df["order_date"])
    return df


df = load_data()

# ── Sidebar Filters ───────────────────────────────────────────
st.sidebar.header("Filters")

regions = ["All"] + sorted(df["region"].unique().tolist())
selected_region = st.sidebar.selectbox("Region", regions)

date_min = df["order_date"].min().date()
date_max = df["order_date"].max().date()
date_range = st.sidebar.date_input("Date Range", [date_min, date_max])

# Apply filters
filtered = df.copy()
if selected_region != "All":
    filtered = filtered[filtered["region"] == selected_region]
if len(date_range) == 2:
    start = pd.Timestamp(date_range[0])
    end   = pd.Timestamp(date_range[1])
    filtered = filtered[(filtered["order_date"] >= start) & (filtered["order_date"] <= end)]

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
    daily = filtered.groupby("order_date")["revenue"].sum().reset_index()
    st.plotly_chart(
        px.line(daily, x="order_date", y="revenue", title="Daily Revenue Trend"),
        use_container_width=True,
    )

with col_right:
    by_region = filtered.groupby("region")["revenue"].sum().reset_index()
    st.plotly_chart(
        px.bar(by_region, x="region", y="revenue", title="Revenue by Region", color="region"),
        use_container_width=True,
    )

by_product = (
    filtered.groupby("product")["revenue"].sum()
    .sort_values(ascending=False)
    .reset_index()
)
st.plotly_chart(
    px.bar(by_product, x="product", y="revenue", title="Revenue by Product", color="product"),
    use_container_width=True,
)

st.subheader("Detailed Data")
st.dataframe(filtered.sort_values("order_date", ascending=False), use_container_width=True)
