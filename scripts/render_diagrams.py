"""Draw the architecture and star-schema diagrams to PNG and SVG."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

OUT = Path(__file__).resolve().parents[1] / "docs" / "diagrams"


def _box(ax, x, y, w, h, text, face, edge="#1f2933"):
    patch = FancyBboxPatch(
        (x, y),
        w,
        h,
        boxstyle="round,pad=0.02,rounding_size=0.08",
        linewidth=1.2,
        facecolor=face,
        edgecolor=edge,
    )
    ax.add_patch(patch)
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=8, color="#102027", wrap=True)


def architecture(path_stem: Path) -> None:
    fig, ax = plt.subplots(figsize=(12.5, 7.2))
    ax.set_xlim(0, 12.5)
    ax.set_ylim(0, 7.2)
    ax.axis("off")
    ax.set_title("Northline Outfitters — sales and inventory lakehouse", loc="left", fontsize=13, pad=12)

    _box(ax, 0.3, 5.6, 2.3, 1.1, "POS, web, marketplace\nWMS movements\nCDC feeds", "#d7ece4")
    _box(ax, 3.0, 5.6, 2.4, 1.1, "ADF orchestrator\nmetadata ForEach\nKey Vault refs", "#f6e7c1")
    _box(ax, 5.8, 5.6, 2.6, 1.1, "ADLS Gen2\nlanding / bronze\nsilver / gold", "#d6e4f0")
    _box(ax, 8.8, 5.6, 3.2, 1.1, "Databricks job\nPySpark + Delta\nUnity Catalog", "#e4d7f0")

    _box(ax, 0.3, 3.3, 3.6, 1.5, "Runs locally and in CI\nseeded CSV batches\nPySpark medallion\nStreamlit on Gold", "#e7f5ee")
    _box(ax, 4.3, 3.3, 3.6, 1.5, "Synapse\nserverless OPENROWSET\ndedicated HASH + CCI\nctl.ingest_watermark", "#fde8d0")
    _box(ax, 8.3, 3.3, 3.6, 1.5, "Power BI\nstar schema + DAX\nnet sales, AOV,\nreturn rate, RFM", "#f8d7da")

    _box(ax, 0.3, 0.8, 11.6, 1.6, "Silver: DQ quarantine, CDC MERGE, customer & product SCD2,\nlate-arriving inferred members, inventory negative-on-hand guard\nGold: fact_sales, fact_returns, fact_payments,\nfact_inventory_movement, fact_inventory_snapshot + marts", "#f4f6f8")

    ax.annotate("", xy=(2.95, 6.15), xytext=(2.65, 6.15), arrowprops={"arrowstyle": "->", "color": "#334155"})
    ax.annotate("", xy=(5.75, 6.15), xytext=(5.45, 6.15), arrowprops={"arrowstyle": "->", "color": "#334155"})
    ax.annotate("", xy=(8.75, 6.15), xytext=(8.45, 6.15), arrowprops={"arrowstyle": "->", "color": "#334155"})
    fig.tight_layout()
    fig.savefig(path_stem.with_suffix(".png"), dpi=140)
    fig.savefig(path_stem.with_suffix(".svg"))
    plt.close(fig)


def erd(path_stem: Path) -> None:
    fig, ax = plt.subplots(figsize=(12.5, 7.4))
    ax.set_xlim(0, 12.5)
    ax.set_ylim(0, 7.4)
    ax.axis("off")
    ax.set_title("Gold star schema (as-of customer and product)", loc="left", fontsize=13)

    dims = [
        (0.3, 5.7, "dim_date"),
        (2.6, 5.7, "dim_customer\nSCD2"),
        (4.9, 5.7, "dim_product\nSCD2 price\n& category"),
        (7.4, 5.7, "dim_store"),
        (9.7, 5.7, "dim_channel\ndim_promotion"),
    ]
    for x, y, label in dims:
        _box(ax, x, y, 2.1, 1.2, label, "#d6e4f0")

    facts = [
        (0.4, 2.6, "fact_sales\norder line"),
        (3.2, 2.6, "fact_returns\nline partials"),
        (6.0, 2.6, "fact_payments\ncapture / void"),
        (8.8, 2.6, "fact_inventory\nmovement + snapshot"),
    ]
    for x, y, label in facts:
        _box(ax, x, y, 2.5, 1.3, label, "#f6e7c1")

    _box(ax, 0.4, 0.4, 11.5, 1.6, "Marts: sales performance (net sales, AOV, return rate),\ninventory health (sell-through, days of supply, turnover, stock-out days),\ncustomer RFM and cohort, inventory variance", "#e7f5ee")
    fig.tight_layout()
    fig.savefig(path_stem.with_suffix(".png"), dpi=140)
    fig.savefig(path_stem.with_suffix(".svg"))
    plt.close(fig)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    architecture(OUT / "architecture")
    erd(OUT / "erd")
    print(f"wrote diagrams under {OUT}")


if __name__ == "__main__":
    main()
