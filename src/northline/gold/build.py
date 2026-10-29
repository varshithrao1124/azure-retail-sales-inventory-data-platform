"""Rebuild the gold star schema from current silver.

Gold is a full refresh. Silver is incremental. As-of dimension joins and the
inventory rollforward are easier to prove when gold is derived in one pass,
and the fact volume here is orders rather than clickstream.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from northline.config import Settings
from northline.io_delta import delta_exists, optimize_zorder, read_delta, write_delta

TS = "yyyy-MM-dd'T'HH:mm:ss'Z'"


def _require(spark: SparkSession, settings: Settings, table: str) -> DataFrame:
    path = settings.table_path("silver", table)
    if not delta_exists(path):
        raise FileNotFoundError(path)
    return read_delta(spark, path)


def build_gold(spark: SparkSession, settings: Settings) -> dict[str, int]:
    orders = _require(spark, settings, "orders").filter(~F.col("is_deleted")).alias("o")
    lines = _require(spark, settings, "order_lines").filter(~F.col("is_deleted")).alias("l")
    returns = (
        read_delta(spark, settings.table_path("silver", "returns"))
        if delta_exists(settings.table_path("silver", "returns"))
        else spark.createDataFrame(
            [],
            "return_id string, order_id string, line_number string, qty_returned int, refund_amt double, is_deleted boolean",
        )
    )
    returns = returns.filter(~F.col("is_deleted"))
    returned = returns.groupBy("order_id", "line_number").agg(
        F.sum("qty_returned").alias("qty_returned"),
        F.sum("refund_amt").alias("refund_amt"),
    )

    customer = (
        _require(spark, settings, "customer_scd")
        .select(
            "customer_id",
            "loyalty_tier",
            "segment",
            "effective_start",
            "effective_end",
            "is_inferred",
            F.xxhash64("customer_id", "effective_start").alias("customer_sk"),
        )
        .alias("c")
    )
    product = (
        _require(spark, settings, "product_scd")
        .select(
            "sku",
            "category",
            "subcategory",
            F.col("list_price").cast("double").alias("list_price_asof"),
            F.col("unit_cost").cast("double").alias("unit_cost_asof"),
            "effective_start",
            "effective_end",
            "is_inferred",
            F.xxhash64("sku", "effective_start").alias("product_sk"),
        )
        .alias("p")
    )
    store = _require(spark, settings, "store").select(
        "store_id",
        "store_name",
        "store_type",
        "region",
        "state",
        "city",
        F.xxhash64("store_id").alias("store_sk"),
    )
    channel = _require(spark, settings, "channel").select(
        "channel_id",
        "channel_name",
        F.xxhash64("channel_id").alias("channel_sk"),
    )
    promo = _require(spark, settings, "promotion").select(
        "promo_id",
        "promo_name",
        "applies_to_category",
        F.col("discount_pct").cast("double").alias("discount_pct"),
        F.xxhash64("promo_id").alias("promo_sk"),
    )

    base = (
        lines.select(
            F.col("order_id"),
            F.col("line_number"),
            F.col("sku"),
            F.col("qty"),
            F.col("unit_price"),
            F.col("discount_amt"),
            F.col("tax_amt"),
            F.col("line_status"),
        )
        .join(
            orders.select(
                F.col("order_id"),
                F.col("order_ts"),
                F.col("customer_id"),
                F.col("store_id"),
                F.col("channel_id"),
                F.col("promo_id"),
                F.col("status").alias("order_status"),
            ),
            "order_id",
            "inner",
        )
        .join(returned, ["order_id", "line_number"], "left")
        .withColumn("order_ts_parsed", F.to_timestamp(F.col("order_ts"), TS))
        .withColumn("qty_returned", F.coalesce(F.col("qty_returned"), F.lit(0)))
        .withColumn("refund_amt", F.coalesce(F.col("refund_amt"), F.lit(0.0)))
    )

    customer_dim = customer.select(
        F.col("customer_id"),
        F.col("loyalty_tier"),
        F.col("segment"),
        F.col("effective_start").alias("c_start"),
        F.col("effective_end").alias("c_end"),
        F.col("is_inferred").alias("is_inferred_customer"),
        F.col("customer_sk"),
    )
    product_dim = product.select(
        F.col("sku"),
        F.col("category"),
        F.col("subcategory"),
        F.col("list_price_asof"),
        F.col("unit_cost_asof"),
        F.col("effective_start").alias("p_start"),
        F.col("effective_end").alias("p_end"),
        F.col("is_inferred").alias("is_inferred_product"),
        F.col("product_sk"),
    )
    joined = base.join(
        customer_dim,
        (base.customer_id == customer_dim.customer_id)
        & (base.order_ts_parsed >= F.to_timestamp(customer_dim.c_start, TS))
        & (
            customer_dim.c_end.isNull()
            | (base.order_ts_parsed < F.to_timestamp(customer_dim.c_end, TS))
        ),
        "left",
    ).drop(customer_dim.customer_id)
    joined = joined.join(
        product_dim,
        (joined.sku == product_dim.sku)
        & (joined.order_ts_parsed >= F.to_timestamp(product_dim.p_start, TS))
        & (
            product_dim.p_end.isNull()
            | (joined.order_ts_parsed < F.to_timestamp(product_dim.p_end, TS))
        ),
        "left",
    ).drop(product_dim.sku)
    joined = joined.join(F.broadcast(store), "store_id", "left")
    joined = joined.join(F.broadcast(channel), "channel_id", "left")
    joined = joined.join(F.broadcast(promo), "promo_id", "left")

    cancelled = (F.col("order_status") == "CANCELLED") | (F.col("line_status") == "CANCELLED")
    gross = F.round(F.col("qty") * F.col("unit_price") - F.col("discount_amt"), 2)
    fact = (
        joined.withColumn("is_cancelled", cancelled)
        .withColumn("order_date", F.to_date(F.col("order_ts_parsed")))
        .withColumn(
            "order_date_key", F.date_format(F.col("order_ts_parsed"), "yyyyMMdd").cast("int")
        )
        .withColumn("qty_ordered", F.col("qty"))
        .withColumn("qty_cancelled", F.when(cancelled, F.col("qty")).otherwise(F.lit(0)))
        .withColumn(
            "qty_net",
            F.when(cancelled, F.lit(0)).otherwise(
                F.greatest(F.col("qty") - F.col("qty_returned"), F.lit(0))
            ),
        )
        .withColumn("gross_sales_amt", F.when(cancelled, F.lit(0.0)).otherwise(gross))
        .withColumn("refund_amt", F.when(cancelled, F.lit(0.0)).otherwise(F.col("refund_amt")))
        .withColumn("net_sales_amt", F.round(F.col("gross_sales_amt") - F.col("refund_amt"), 2))
        .withColumn(
            "cogs_amt",
            F.when(
                cancelled | F.col("unit_cost_asof").isNull(), F.lit(None).cast("double")
            ).otherwise(F.round(F.col("qty_net") * F.col("unit_cost_asof"), 2)),
        )
        .withColumn(
            "sales_sk",
            F.xxhash64("order_id", "line_number"),
        )
    )
    fact_sales = fact.select(
        "sales_sk",
        "order_date_key",
        "order_date",
        "order_ts",
        "customer_sk",
        "product_sk",
        "store_sk",
        "channel_sk",
        "promo_sk",
        "order_id",
        "line_number",
        "customer_id",
        "sku",
        "store_id",
        "channel_id",
        "promo_id",
        "category",
        "subcategory",
        "loyalty_tier",
        "segment",
        "is_inferred_customer",
        "is_inferred_product",
        "qty_ordered",
        "qty_cancelled",
        "qty_returned",
        "qty_net",
        "unit_price",
        "list_price_asof",
        "unit_cost_asof",
        "discount_amt",
        "gross_sales_amt",
        "refund_amt",
        "net_sales_amt",
        "cogs_amt",
        "tax_amt",
        "is_cancelled",
        "line_status",
        "order_status",
    )
    write_delta(
        fact_sales,
        settings.table_path("gold", "fact_sales"),
        settings,
        mode="overwrite",
        partition_by=["order_date"],
        operation="fact-sales",
    )

    fact_returns = (
        returns.alias("r")
        .join(
            orders.select("order_id", "customer_id", "store_id", "channel_id", "order_ts"),
            "order_id",
            "left",
        )
        .withColumn("return_date", F.to_date(F.to_timestamp(F.col("return_ts"), TS)))
        .withColumn("return_sk", F.xxhash64("return_id"))
        .select(
            "return_sk",
            "return_id",
            "return_date",
            "return_ts",
            "order_id",
            "line_number",
            "customer_id",
            "store_id",
            "channel_id",
            "sku",
            "qty_returned",
            "refund_amt",
            "reason_code",
        )
    )
    write_delta(
        fact_returns,
        settings.table_path("gold", "fact_returns"),
        settings,
        mode="overwrite",
        partition_by=["return_date"],
        operation="fact-returns",
    )

    payments = _require(spark, settings, "payments").filter(~F.col("is_deleted"))
    fact_payments = (
        payments.withColumn("paid_date", F.to_date(F.to_timestamp(F.col("paid_ts"), TS)))
        .withColumn("payment_sk", F.xxhash64("payment_id"))
        .withColumn(
            "signed_amount",
            F.when(F.col("status") == "CAPTURED", F.col("amount"))
            .when(F.col("status") == "REFUNDED", -F.col("amount"))
            .otherwise(F.lit(0.0)),
        )
        .select(
            "payment_sk",
            "payment_id",
            "paid_date",
            "paid_ts",
            "order_id",
            "tender_type",
            "status",
            "amount",
            "signed_amount",
        )
    )
    write_delta(
        fact_payments,
        settings.table_path("gold", "fact_payments"),
        settings,
        mode="overwrite",
        partition_by=["paid_date"],
        operation="fact-payments",
    )

    movements = _require(spark, settings, "inventory_movement")
    product_current = (
        _require(spark, settings, "product_scd")
        .filter(F.col("is_current"))
        .select(
            F.col("sku"),
            F.col("category").alias("current_category"),
            F.col("is_inferred").alias("current_inferred_product"),
            F.xxhash64("sku", "effective_start").alias("current_product_sk"),
        )
    )
    fact_movement = (
        movements.withColumn("movement_date", F.to_date(F.to_timestamp(F.col("movement_ts"), TS)))
        .join(F.broadcast(product_current), "sku", "left")
        .join(F.broadcast(store.select("store_id", "store_sk")), "store_id", "left")
        .select(
            "movement_id",
            "movement_ts",
            "movement_date",
            "sku",
            "store_id",
            F.col("current_product_sk").alias("product_sk"),
            "store_sk",
            F.col("current_category").alias("category"),
            "movement_type",
            "qty",
            "qty_signed",
            "unit_cost",
            "reference_id",
            "reason_code",
            F.col("current_inferred_product").alias("is_inferred_product"),
        )
    )
    write_delta(
        fact_movement,
        settings.table_path("gold", "fact_inventory_movement"),
        settings,
        mode="overwrite",
        partition_by=["movement_date"],
        operation="fact-movement",
    )

    snapshots = _require(spark, settings, "inventory_snapshot").withColumn(
        "snapshot_ts",
        F.to_timestamp(F.concat(F.col("snapshot_date"), F.lit("T23:59:59Z")), TS),
    )
    snap_dim = _require(spark, settings, "product_scd").select(
        F.col("sku"),
        F.col("category"),
        F.col("effective_start"),
        F.col("effective_end"),
        F.col("is_inferred"),
        F.xxhash64("sku", "effective_start").alias("product_sk"),
    )
    fact_snapshot = (
        snapshots.join(
            snap_dim,
            (snapshots.sku == snap_dim.sku)
            & (snapshots.snapshot_ts >= F.to_timestamp(snap_dim.effective_start, TS))
            & (
                snap_dim.effective_end.isNull()
                | (snapshots.snapshot_ts < F.to_timestamp(snap_dim.effective_end, TS))
            ),
            "left",
        )
        .drop(snap_dim.sku)
        .join(F.broadcast(store.select("store_id", "store_sk")), "store_id", "left")
        .withColumn("snapshot_date", F.to_date(F.col("snapshot_date")))
        .withColumn("is_stockout", F.col("on_hand_qty") == 0)
        .select(
            "snapshot_date",
            "sku",
            "store_id",
            "product_sk",
            "store_sk",
            "category",
            "on_hand_qty",
            "on_hand_cost",
            "is_stockout",
            F.col("is_inferred").alias("is_inferred_product"),
        )
    )
    write_delta(
        fact_snapshot,
        settings.table_path("gold", "fact_inventory_snapshot"),
        settings,
        mode="overwrite",
        partition_by=["snapshot_date"],
        operation="fact-snapshot",
    )

    dim_customer = _require(spark, settings, "customer_scd").select(
        F.xxhash64("customer_id", "effective_start").alias("customer_sk"),
        "customer_id",
        "full_name",
        "email",
        "segment",
        "loyalty_tier",
        "home_store_id",
        "city",
        "state",
        "postal_code",
        "status",
        "effective_start",
        "effective_end",
        "is_current",
        "is_inferred",
        "is_deleted",
    )
    dim_product = _require(spark, settings, "product_scd").select(
        F.xxhash64("sku", "effective_start").alias("product_sk"),
        "sku",
        "product_name",
        "category",
        "subcategory",
        "brand",
        F.col("unit_cost").cast("double").alias("unit_cost"),
        F.col("list_price").cast("double").alias("list_price"),
        "status",
        "effective_start",
        "effective_end",
        "is_current",
        "is_inferred",
        "is_deleted",
    )
    dim_date = spark.sql(
        """
        SELECT explode(sequence(to_date('2020-01-01'), to_date('2026-12-31'), interval 1 day)) AS calendar_date
        """
    ).select(
        F.date_format("calendar_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("calendar_date"),
        F.year("calendar_date").alias("year"),
        F.quarter("calendar_date").alias("quarter"),
        F.month("calendar_date").alias("month"),
        F.dayofmonth("calendar_date").alias("day"),
        F.date_format("calendar_date", "EEEE").alias("day_name"),
        F.dayofweek("calendar_date").isin(1, 7).alias("is_weekend"),
        F.weekofyear("calendar_date").alias("iso_week"),
    )
    for name, frame in {
        "dim_customer": dim_customer,
        "dim_product": dim_product,
        "dim_store": store,
        "dim_channel": channel,
        "dim_promotion": promo,
        "dim_date": dim_date,
        "dim_customer_current": dim_customer.filter(F.col("is_current")),
        "dim_product_current": dim_product.filter(F.col("is_current")),
    }.items():
        write_delta(
            frame, settings.table_path("gold", name), settings, mode="overwrite", operation=name
        )

    counts = {
        "fact_sales": fact_sales.count(),
        "fact_returns": fact_returns.count(),
        "fact_payments": fact_payments.count(),
        "fact_inventory_movement": fact_movement.count(),
        "fact_inventory_snapshot": fact_snapshot.count(),
    }
    _build_marts(spark, settings)
    return counts


def _register_gold(spark: SparkSession, settings: Settings) -> None:
    for name in (
        "fact_sales",
        "fact_returns",
        "fact_payments",
        "fact_inventory_movement",
        "fact_inventory_snapshot",
        "dim_customer",
        "dim_product",
        "dim_store",
        "dim_channel",
        "dim_promotion",
        "dim_date",
    ):
        read_delta(spark, settings.table_path("gold", name)).createOrReplaceTempView(name)


def _build_marts(spark: SparkSession, settings: Settings) -> None:
    _register_gold(spark, settings)
    sql_dir = Path(settings.project_root) / "sql" / "gold"
    for path in sorted(sql_dir.glob("*.sql")):
        text = path.read_text(encoding="utf-8").replace(
            "__WINDOW_DAYS__", str(settings.inventory_window_days)
        )
        frame = spark.sql(text)
        write_delta(
            frame,
            settings.table_path("gold", path.stem),
            settings,
            mode="overwrite",
            operation=path.stem,
        )


def optimize_gold(spark: SparkSession, settings: Settings) -> list[dict]:
    targets = {
        "fact_sales": ["store_id", "sku"],
        "fact_inventory_snapshot": ["store_id", "sku"],
        "fact_inventory_movement": ["store_id", "sku"],
    }
    results = []
    for table, columns in targets.items():
        path = settings.table_path("gold", table)
        if not delta_exists(path):
            continue
        import time

        started = time.perf_counter()
        try:
            stats = optimize_zorder(spark, path, columns)
            stats["status"] = "ok"
        except Exception as exc:
            stats = {
                "path": path,
                "zorder": columns,
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
            }
        stats["seconds"] = round(time.perf_counter() - started, 3)
        stats["table"] = table
        results.append(stats)
    return results
