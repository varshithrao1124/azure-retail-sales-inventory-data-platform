"""Apply one landed batch to silver.

File order matters: dimensions and CDC before facts, facts before the
late-arriving infer pass, snapshots before the movement guard so the opening
balance is already on the ledger.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from northline.config import Settings
from northline.io_delta import delta_exists, read_delta, write_delta
from northline.logging_utils import get_logger
from northline.schemas import CUSTOMER_TRACKED, PRODUCT_TRACKED
from northline.silver.cdc import merge_current
from northline.silver.inventory import merge_snapshots, post_movements
from northline.silver.quality import extra_quarantine, split_rules
from northline.silver.scd import apply_scd2, bootstrap_snapshot, infer_members

LOGGER = get_logger("silver")

CUSTOMER_FILLS = {
    "full_name": "UNKNOWN",
    "email": "unknown@northline.invalid",
    "segment": "UNKNOWN",
    "loyalty_tier": "UNKNOWN",
    "home_store_id": "UNKNOWN",
    "city": "UNKNOWN",
    "state": "UN",
    "postal_code": "UNKNOWN",
    "status": "UNKNOWN",
}
PRODUCT_FILLS = {
    "product_name": "UNKNOWN",
    "category": "UNKNOWN",
    "subcategory": "UNKNOWN",
    "brand": "UNKNOWN",
    "unit_cost": None,
    "list_price": None,
    "status": "UNKNOWN",
}


def _blank_to_null(df: DataFrame, column: str) -> DataFrame:
    return df.withColumn(
        column,
        F.when(
            F.col(column).isNull() | (F.length(F.trim(F.col(column))) == 0), F.lit(None)
        ).otherwise(F.col(column)),
    )


def _lineage(df: DataFrame, batch_ts: str, run_id: str, from_cdc: bool) -> DataFrame:
    op = F.col("op") if from_cdc else F.lit("I")
    return (
        df.withColumn("op", op)
        .withColumn("batch_ts", F.lit(batch_ts))
        .withColumn("run_id", F.lit(run_id))
    )


def _append_quarantine(spark: SparkSession, settings: Settings, frame: DataFrame) -> int:
    if frame.limit(1).count() == 0:
        return 0
    path = settings.table_path("silver", "quarantine")
    fresh = frame
    if delta_exists(path):
        existing = read_delta(spark, path).select("quarantine_id")
        fresh = fresh.join(existing, "quarantine_id", "left_anti")
    added = fresh.count()
    if added:
        stamped = fresh.withColumn("ingest_ts", F.current_timestamp().cast("string"))
        write_delta(stamped, path, settings, mode="append", operation="quarantine")
    return added


def _dq(
    spark: SparkSession,
    settings: Settings,
    entity: str,
    batch_ts: str,
    run_id: str,
    frame: DataFrame,
    metrics: list[dict],
) -> DataFrame:
    good, quarantine, seen, failed = split_rules(frame, entity, batch_ts, run_id)
    _append_quarantine(spark, settings, quarantine)
    rate = (failed / seen) if seen else 0.0
    metrics.append(
        {
            "run_id": run_id,
            "batch_ts": batch_ts,
            "entity": entity,
            "rows_seen": seen,
            "rows_failed": failed,
            "fail_rate": float(rate),
        }
    )
    if seen and rate >= settings.dq_fail_rate_alert_threshold:
        LOGGER.warning(
            "dq fail rate %.3f on %s %s",
            rate,
            entity,
            batch_ts,
            extra={"batch_ts": batch_ts, "entity": entity, "run_id": run_id},
        )
    return good


def _union(frames: list[DataFrame]) -> DataFrame | None:
    present = [frame for frame in frames if frame is not None and frame.limit(1).count() > 0]
    if not present:
        return None
    out = present[0]
    for frame in present[1:]:
        out = out.unionByName(frame)
    return out


def _merge_type1(
    spark: SparkSession,
    path: str,
    frame: DataFrame,
    key: str,
    columns: list[str],
    settings: Settings,
) -> None:
    if frame.limit(1).count() == 0:
        return
    from pyspark.sql.window import Window

    window = Window.partitionBy(key).orderBy(F.col("extract_ts").desc())
    source = (
        frame.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")
    )
    source = source.select(key, *columns, "extract_ts", "batch_ts", "run_id")
    if not delta_exists(path):
        write_delta(source, path, settings, mode="append", operation=f"type1-{key}")
        return
    from delta.tables import DeltaTable

    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(source.alias("s"), f"t.{key} = s.{key}")
        .whenMatchedUpdate(
            set={column: f"s.{column}" for column in columns + ["extract_ts", "batch_ts", "run_id"]}
        )
        .whenNotMatchedInsert(
            values={
                column: f"s.{column}"
                for column in [key, *columns, "extract_ts", "batch_ts", "run_id"]
            }
        )
        .execute()
    )


def apply_silver_batch(
    spark: SparkSession,
    settings: Settings,
    batch_ts: str,
    run_id: str,
    tables: dict[str, DataFrame],
    metrics: list[dict],
) -> None:
    def good(entity: str) -> DataFrame | None:
        frame = tables.get(entity)
        if frame is None:
            return None
        return _dq(spark, settings, entity, batch_ts, run_id, frame, metrics)

    stores = good("stores")
    if stores is not None:
        stores = _lineage(stores, batch_ts, run_id, False)
        _merge_type1(
            spark,
            settings.table_path("silver", "store"),
            stores,
            "store_id",
            ["store_name", "store_type", "region", "state", "city", "square_feet", "open_date"],
            settings,
        )
    channels = good("channels")
    if channels is not None:
        channels = _lineage(channels, batch_ts, run_id, False)
        _merge_type1(
            spark,
            settings.table_path("silver", "channel"),
            channels,
            "channel_id",
            ["channel_name"],
            settings,
        )
    promos = good("promotions")
    if promos is not None:
        promos = _lineage(promos, batch_ts, run_id, False)
        _merge_type1(
            spark,
            settings.table_path("silver", "promotion"),
            promos,
            "promo_id",
            ["promo_name", "applies_to_category", "discount_pct", "start_date", "end_date"],
            settings,
        )

    customers = good("customers")
    if customers is not None:
        bootstrap_snapshot(
            spark,
            settings.table_path("silver", "customer_scd"),
            customers,
            "customer_id",
            CUSTOMER_TRACKED,
            settings,
            batch_ts,
            run_id,
        )
    products = good("products")
    if products is not None:
        bootstrap_snapshot(
            spark,
            settings.table_path("silver", "product_scd"),
            products,
            "sku",
            PRODUCT_TRACKED,
            settings,
            batch_ts,
            run_id,
        )

    cdc_customers = good("cdc_customers")
    if cdc_customers is not None:
        apply_scd2(
            spark,
            settings.table_path("silver", "customer_scd"),
            cdc_customers,
            "customer_id",
            CUSTOMER_TRACKED,
            settings,
            batch_ts,
            run_id,
        )
    cdc_products = good("cdc_products")
    if cdc_products is not None:
        apply_scd2(
            spark,
            settings.table_path("silver", "product_scd"),
            cdc_products,
            "sku",
            PRODUCT_TRACKED,
            settings,
            batch_ts,
            run_id,
        )

    def _orders(frame: DataFrame | None, from_cdc: bool) -> DataFrame | None:
        if frame is None:
            return None
        out = _lineage(_blank_to_null(frame, "promo_id"), batch_ts, run_id, from_cdc)
        return out.select(
            "op",
            "sequence_number",
            "order_id",
            "order_ts",
            "customer_id",
            "store_id",
            "channel_id",
            "promo_id",
            "status",
            "batch_ts",
            "run_id",
        )

    order_frame = _union([_orders(good("orders"), False), _orders(good("cdc_orders"), True)])
    if order_frame is not None:
        merge_current(
            spark,
            settings.table_path("silver", "orders"),
            order_frame,
            ["order_id"],
            [
                "order_ts",
                "customer_id",
                "store_id",
                "channel_id",
                "promo_id",
                "status",
                "batch_ts",
                "run_id",
            ],
            settings,
        )

    def _lines(frame: DataFrame | None, from_cdc: bool) -> DataFrame | None:
        if frame is None:
            return None
        out = _lineage(frame, batch_ts, run_id, from_cdc)
        return out.select(
            "op",
            "sequence_number",
            "order_id",
            "line_number",
            F.col("sku"),
            F.col("qty").cast("int").alias("qty"),
            F.col("unit_price").cast("double").alias("unit_price"),
            F.col("discount_amt").cast("double").alias("discount_amt"),
            F.col("tax_amt").cast("double").alias("tax_amt"),
            "line_status",
            "batch_ts",
            "run_id",
        )

    line_frame = _union([_lines(good("order_lines"), False), _lines(good("cdc_order_lines"), True)])
    if line_frame is not None:
        merge_current(
            spark,
            settings.table_path("silver", "order_lines"),
            line_frame,
            ["order_id", "line_number"],
            [
                "sku",
                "qty",
                "unit_price",
                "discount_amt",
                "tax_amt",
                "line_status",
                "batch_ts",
                "run_id",
            ],
            settings,
        )

    def _payments(frame: DataFrame | None, from_cdc: bool) -> DataFrame | None:
        if frame is None:
            return None
        out = _lineage(frame, batch_ts, run_id, from_cdc)
        return out.select(
            "op",
            "sequence_number",
            "payment_id",
            "order_id",
            "tender_type",
            F.col("amount").cast("double").alias("amount"),
            "status",
            "paid_ts",
            "batch_ts",
            "run_id",
        )

    pay_frame = _union([_payments(good("payments"), False), _payments(good("cdc_payments"), True)])
    if pay_frame is not None and delta_exists(settings.table_path("silver", "orders")):
        orders = (
            read_delta(spark, settings.table_path("silver", "orders")).select("order_id").distinct()
        )
        orphan = pay_frame.join(orders, "order_id", "left_anti")
        if orphan.limit(1).count() > 0:
            _append_quarantine(
                spark,
                settings,
                extra_quarantine(
                    orphan, "payments", batch_ts, run_id, "ORPHAN_PAYMENT", ["payment_id"]
                ),
            )
            seen_pay = pay_frame.count()
            failed_pay = orphan.count()
            metrics.append(
                {
                    "run_id": run_id,
                    "batch_ts": batch_ts,
                    "entity": "payments_orphan",
                    "rows_seen": seen_pay,
                    "rows_failed": failed_pay,
                    "fail_rate": (failed_pay / seen_pay) if seen_pay else 0.0,
                }
            )
        pay_frame = pay_frame.join(orders, "order_id", "left_semi")
    if pay_frame is not None:
        merge_current(
            spark,
            settings.table_path("silver", "payments"),
            pay_frame,
            ["payment_id"],
            ["order_id", "tender_type", "amount", "status", "paid_ts", "batch_ts", "run_id"],
            settings,
        )

    returns = good("returns")
    if returns is not None and delta_exists(settings.table_path("silver", "order_lines")):
        prepared = _lineage(returns, batch_ts, run_id, False).select(
            "op",
            "sequence_number",
            "return_id",
            "order_id",
            "line_number",
            "sku",
            F.col("qty_returned").cast("int").alias("qty_returned"),
            F.col("refund_amt").cast("double").alias("refund_amt"),
            "reason_code",
            "return_ts",
            "batch_ts",
            "run_id",
        )
        lines = (
            read_delta(spark, settings.table_path("silver", "order_lines"))
            .select(
                "order_id",
                "line_number",
                F.col("qty").alias("ordered_qty"),
                "is_deleted",
            )
            .filter(~F.col("is_deleted"))
        )
        joined = prepared.join(lines, ["order_id", "line_number"], "left")
        bad = joined.filter(
            F.col("ordered_qty").isNull() | (F.col("qty_returned") > F.col("ordered_qty"))
        )
        if bad.limit(1).count() > 0:
            _append_quarantine(
                spark,
                settings,
                extra_quarantine(
                    bad.drop("ordered_qty", "is_deleted"),
                    "returns",
                    batch_ts,
                    run_id,
                    "RETURN_EXCEEDS_SALE",
                    ["return_id"],
                ),
            )
        keep = joined.filter(
            F.col("ordered_qty").isNotNull() & (F.col("qty_returned") <= F.col("ordered_qty"))
        )
        keep = keep.drop("ordered_qty", "is_deleted")
        merge_current(
            spark,
            settings.table_path("silver", "returns"),
            keep,
            ["return_id"],
            [
                "order_id",
                "line_number",
                "sku",
                "qty_returned",
                "refund_amt",
                "reason_code",
                "return_ts",
                "batch_ts",
                "run_id",
            ],
            settings,
        )

    snapshots = good("inventory_snapshots")
    if snapshots is not None:
        merge_snapshots(
            spark,
            settings.table_path("silver", "inventory_snapshot"),
            _lineage(snapshots, batch_ts, run_id, False),
            settings,
        )

    movements = good("inventory_movements")
    if movements is not None:
        prepared = _lineage(movements, batch_ts, run_id, False)
        posted, rejected = post_movements(
            spark,
            settings.table_path("silver", "inventory_movement"),
            settings.table_path("silver", "inventory_snapshot"),
            prepared,
            settings,
        )
        LOGGER.info(
            "posted %s movements for %s",
            posted,
            batch_ts,
            extra={"batch_ts": batch_ts, "run_id": run_id},
        )
        if rejected.limit(1).count() > 0:
            for rule_id in [
                row.reject_rule for row in rejected.select("reject_rule").distinct().collect()
            ]:
                subset = rejected.filter(F.col("reject_rule") == rule_id).drop("reject_rule")
                _append_quarantine(
                    spark,
                    settings,
                    extra_quarantine(
                        subset, "inventory_movements", batch_ts, run_id, rule_id, ["movement_id"]
                    ),
                )

    if delta_exists(settings.table_path("silver", "orders")):
        infer_members(
            spark,
            settings.table_path("silver", "customer_scd"),
            read_delta(spark, settings.table_path("silver", "orders")).select("customer_id"),
            "customer_id",
            CUSTOMER_TRACKED,
            CUSTOMER_FILLS,
            settings,
            batch_ts,
            run_id,
        )
    sku_frames = []
    for table, column in (
        ("order_lines", "sku"),
        ("inventory_movement", "sku"),
        ("inventory_snapshot", "sku"),
    ):
        path = settings.table_path("silver", table)
        if delta_exists(path):
            sku_frames.append(read_delta(spark, path).select(F.col(column).alias("sku")))
    if sku_frames:
        skus = sku_frames[0]
        for frame in sku_frames[1:]:
            skus = skus.unionByName(frame)
        infer_members(
            spark,
            settings.table_path("silver", "product_scd"),
            skus,
            "sku",
            PRODUCT_TRACKED,
            PRODUCT_FILLS,
            settings,
            batch_ts,
            run_id,
        )
