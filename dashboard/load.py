"""Read Gold Delta tables for the local dashboard.

Uses the same files the medallion job writes. Power BI is the production
surface; this module exists so a laptop can open the model without a workspace.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from deltalake import DeltaTable


def lake_root() -> Path:
    override = os.environ.get("NL_LOCAL_ROOT")
    if override:
        return Path(override)
    here = Path(__file__).resolve()
    for candidate in [here, *here.parents]:
        if (candidate / "pyproject.toml").exists():
            return candidate / "data" / "lake"
    return Path("data/lake")


def read_table(name: str) -> pd.DataFrame:
    path = lake_root() / "gold" / name
    if not (path / "_delta_log").exists():
        raise FileNotFoundError(path)
    return DeltaTable(str(path)).to_pandas()


def kpis(sales: pd.DataFrame) -> dict:
    active = sales.loc[~sales["is_cancelled"].astype(bool)]
    net = float(sales["net_sales_amt"].sum())
    orders = int(active["order_id"].nunique())
    units = float(active["qty_ordered"].sum())
    returned = float(sales["qty_returned"].sum())
    return {
        "net_sales": net,
        "orders": orders,
        "aov": (net / orders) if orders else None,
        "return_rate_units": (returned / units) if units else None,
        "cancelled_orders": int(
            sales.loc[sales["is_cancelled"].astype(bool), "order_id"].nunique()
        ),
    }
