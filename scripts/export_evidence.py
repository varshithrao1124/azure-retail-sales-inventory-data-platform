"""Build docs/images from the local gold lake and captured transcripts.

Every number comes from Delta files or from a text file produced by a real command.
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from deltalake import DeltaTable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.render_text_image import render  # noqa: E402

LAKE = ROOT / "data" / "lake"
IMG = ROOT / "docs" / "images"
OUT = ROOT / "docs" / "run_output"


def table(name: str) -> pd.DataFrame:
    return DeltaTable(str(LAKE / name)).to_pandas()


def write_text(name: str, text: str, title: str) -> None:
    path = OUT / name
    path.write_text(text, encoding="utf-8")
    render(text, IMG / (Path(name).stem + ".png"), title)


def main() -> None:
    IMG.mkdir(parents=True, exist_ok=True)
    sales = table("gold/fact_sales")
    var = table("gold/inventory_variance")
    cust = table("silver/customer_scd")
    orders = table("silver/orders")
    q = table("silver/quarantine")
    stock = table("gold/mart_stockout")

    breaks = var.loc[var["variance_qty"] != 0, ["snapshot_date", "sku", "store_id", "snapshot_qty", "rolled_qty", "variance_qty"]]
    write_text(
        "inventory_variance.txt",
        breaks.sort_values("snapshot_date").to_string(index=False),
        "local gold.inventory_variance (non-zero)",
    )
    tier = cust.loc[cust["customer_id"] == "C000001", ["customer_id", "loyalty_tier", "effective_start", "effective_end", "is_current"]]
    late = cust.loc[cust["customer_id"].isin(["C000999", "C000998"]), ["customer_id", "full_name", "loyalty_tier", "effective_start", "effective_end", "is_current", "is_inferred"]]
    write_text(
        "scd2_customers.txt",
        tier.to_string(index=False) + "\n\n" + late.sort_values(["customer_id", "effective_start"]).to_string(index=False),
        "local silver.customer_scd",
    )
    cancel = orders.loc[orders["order_id"] == "O-CAN-0001", ["order_id", "status", "sequence_number", "is_deleted"]]
    write_text(
        "cdc_cancel.txt",
        cancel.to_string(index=False),
        "local silver.orders after CDC cancel and stale replay",
    )
    rules = q.groupby("rule_id").size().rename("rows").reset_index().sort_values("rule_id")
    write_text("quarantine.txt", rules.to_string(index=False) + f"\n\nquarantine_rows={len(q)}", "local silver.quarantine")
    write_text(
        "stockout.txt",
        stock.sort_values("stockout_days", ascending=False).head(8).to_string(index=False),
        "local gold.mart_stockout",
    )

    by_day = sales.groupby(["order_date", "channel_id"], as_index=False)["net_sales_amt"].sum()
    fig, ax = plt.subplots(figsize=(8, 4.2))
    for channel, frame in by_day.groupby("channel_id"):
        ax.plot(frame["order_date"].astype(str), frame["net_sales_amt"], marker="o", label=channel)
    ax.set_title("Local gold — net sales by channel")
    ax.set_ylabel("Net sales ($)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(IMG / "dashboard_net_sales.png", dpi=140)
    plt.close(fig)

    by_cat = sales.groupby("category", as_index=False)["net_sales_amt"].sum().sort_values("net_sales_amt", ascending=False)
    fig, ax = plt.subplots(figsize=(8, 4.2))
    ax.bar(by_cat["category"], by_cat["net_sales_amt"], color="#2a6f7f")
    ax.set_title("Local gold — net sales by category")
    ax.set_ylabel("Net sales ($)")
    fig.tight_layout()
    fig.savefig(IMG / "dashboard_category.png", dpi=140)
    plt.close(fig)

    for transcript, title in (
        ("pipeline.txt", "local CLI run — small sample"),
        ("pipeline_rerun.txt", "local CLI rerun — batches skipped"),
    ):
        source = OUT / transcript
        if source.exists():
            body = source.read_text(encoding="utf-8")
            # Drop the Ivy preamble so the image is the CLI summary from the same file.
            marker = "Northline Outfitters"
            if marker in body:
                body = body[body.index(marker) :]
            render(body, IMG / (source.stem + ".png"), title)


if __name__ == "__main__":
    main()
