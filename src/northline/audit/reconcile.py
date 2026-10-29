"""Control checks executed as SQL, plus the manifest variance comparison."""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from northline.config import Settings
from northline.errors import ReconciliationError
from northline.io_delta import delta_exists, read_delta, write_delta


def _register(spark: SparkSession, settings: Settings) -> None:
    mapping = {
        "silver_orders": ("silver", "orders"),
        "silver_order_lines": ("silver", "order_lines"),
        "silver_payments": ("silver", "payments"),
        "silver_returns": ("silver", "returns"),
        "silver_customer_scd": ("silver", "customer_scd"),
        "silver_product_scd": ("silver", "product_scd"),
        "silver_movements": ("silver", "inventory_movement"),
        "silver_snapshots": ("silver", "inventory_snapshot"),
        "fact_sales": ("gold", "fact_sales"),
        "fact_inventory_movement": ("gold", "fact_inventory_movement"),
        "fact_inventory_snapshot": ("gold", "fact_inventory_snapshot"),
        "inventory_variance": ("gold", "inventory_variance"),
    }
    for view, (layer, table) in mapping.items():
        path = settings.table_path(layer, table)
        if not delta_exists(path):
            raise FileNotFoundError(f"Missing {layer}.{table} required by reconciliation")
        read_delta(spark, path).createOrReplaceTempView(view)


def reconcile(spark: SparkSession, settings: Settings, run_id: str) -> list[dict]:
    _register(spark, settings)
    checks: list[dict] = []
    sql_dir = Path(settings.project_root) / "sql" / "checks"
    for path in sorted(sql_dir.glob("*.sql")):
        row = spark.sql(path.read_text(encoding="utf-8")).collect()[0].asDict()
        row["run_id"] = run_id
        row["source"] = path.name
        checks.append(row)

    manifest_path = Path(settings.landing_dir) / "manifest.json"
    expected_rows = []
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        expected_rows = manifest.get("variances", [])
    expected = {
        (row["sku"], row["store_id"], str(row["snapshot_date"])[:10], int(row["variance_qty"]))
        for row in expected_rows
    }
    actual_rows = (
        spark.table("inventory_variance")
        .filter(F.col("variance_qty") != 0)
        .select("sku", "store_id", "snapshot_date", "variance_qty")
        .collect()
    )
    actual = {
        (row.sku, row.store_id, str(row.snapshot_date)[:10], int(row.variance_qty))
        for row in actual_rows
    }
    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)
    status = "PASS" if not missing and not unexpected else "FAIL"
    checks.append(
        {
            "check_name": "inventory_variance_matches_manifest",
            "status": status,
            "expected": str(len(expected)),
            "actual": str(len(actual)),
            "detail": f"missing={missing}; unexpected={unexpected}",
            "run_id": run_id,
            "source": "manifest",
        }
    )

    frame = spark.createDataFrame(checks).select(
        "run_id", "check_name", "status", "expected", "actual", "detail", "source"
    )
    write_delta(
        frame,
        settings.table_path("audit", "reconciliation"),
        settings,
        mode="append",
        operation="reconciliation",
    )
    failed = [row["check_name"] for row in checks if row["status"] != "PASS"]
    if failed:
        raise ReconciliationError("Reconciliation failed: " + ", ".join(failed))
    return checks
