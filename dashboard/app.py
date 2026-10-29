"""Local dashboard over Gold Delta. Production reporting target is Power BI."""

from __future__ import annotations

import sys
from pathlib import Path

import plotly.express as px
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dashboard.load import kpis, lake_root, read_table  # noqa: E402

st.set_page_config(page_title="Northline retail gold", layout="wide")
st.title("Northline Outfitters — local gold dashboard")
st.caption(
    "Local dashboard over Delta gold written by the medallion job. "
    "Power BI is the production target (see docs/powerbi.md). "
    f"Lake: {lake_root()}"
)

try:
    sales = read_table("fact_sales")
    variance = read_table("inventory_variance")
    health = read_table("mart_inventory_health")
    rfm = read_table("mart_customer_rfm")
    stockout = read_table("mart_stockout")
except FileNotFoundError as exc:
    st.error(f"Gold tables are not in the lake yet ({exc}). Run `make pipeline` first.")
    st.stop()

metrics = kpis(sales)
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Net sales", f"${metrics['net_sales']:,.2f}")
c2.metric("Orders", f"{metrics['orders']:,}")
c3.metric("AOV", "—" if metrics["aov"] is None else f"${metrics['aov']:,.2f}")
c4.metric(
    "Unit return rate",
    "—" if metrics["return_rate_units"] is None else f"{metrics['return_rate_units']:.1%}",
)
c5.metric("Cancelled orders", f"{metrics['cancelled_orders']:,}")

by_day = (
    sales.groupby(["order_date", "channel_id"], as_index=False)["net_sales_amt"]
    .sum()
    .sort_values("order_date")
)
st.plotly_chart(
    px.line(
        by_day,
        x="order_date",
        y="net_sales_amt",
        color="channel_id",
        markers=True,
        title="Net sales by channel",
    ),
    use_container_width=True,
)

left, right = st.columns(2)
by_cat = (
    sales.groupby("category", as_index=False)["net_sales_amt"]
    .sum()
    .sort_values("net_sales_amt", ascending=False)
)
left.plotly_chart(
    px.bar(by_cat, x="category", y="net_sales_amt", title="Net sales by category"),
    use_container_width=True,
)

breaks = variance.loc[variance["variance_qty"] != 0].copy()
right.subheader("Inventory variance (non-zero)")
if breaks.empty:
    right.write("No quantity breaks.")
else:
    right.dataframe(breaks, use_container_width=True, hide_index=True)

latest = health.sort_values("snapshot_date").groupby(["sku", "store_id"], as_index=False).tail(1)
short = latest.dropna(subset=["days_of_supply"]).nsmallest(12, "days_of_supply")
st.plotly_chart(
    px.bar(
        short,
        x="sku",
        y="days_of_supply",
        color="store_id",
        title="Lowest days of supply on the latest snapshot (trailing window)",
    ),
    use_container_width=True,
)

s1, s2 = st.columns(2)
s1.subheader("Stock-out days")
s1.dataframe(
    stockout.sort_values("stockout_days", ascending=False),
    use_container_width=True,
    hide_index=True,
)
s2.subheader("Customer RFM")
show = rfm.sort_values("monetary", ascending=False).head(15)
s2.dataframe(show, use_container_width=True, hide_index=True)

st.caption(
    "Amounts are synthetic. Days of supply uses the configured trailing window, not an annualized forecast."
)
