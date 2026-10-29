"""Full local medallion run on the tiny seeded drop."""

from pyspark.sql import functions as F

from northline.config import load_settings
from northline.errors import InducedFailure
from northline.generator import catalog as C
from northline.generator.synthesize import generate
from northline.pipeline.runner import run_pipeline


def _fact(spark, settings):
    return spark.read.format("delta").load(settings.table_path("gold", "fact_sales"))


def test_tiny_pipeline_is_idempotent_and_keeps_planted_cases(spark, tmp_path, monkeypatch):
    landing = tmp_path / "landing"
    lake = tmp_path / "lake"
    logs = tmp_path / "logs"
    generate("tiny", landing, 42)
    monkeypatch.setenv("NL_LANDING", str(landing))
    monkeypatch.setenv("NL_LOCAL_ROOT", str(lake))
    monkeypatch.setenv("NL_LOG_DIR", str(logs))
    monkeypatch.setenv("NL_STORAGE_MODE", "local")
    settings = load_settings("test")

    summary = run_pipeline(settings, spark, skip_optimize=True)
    assert summary["status"] == "SUCCESS"
    assert summary["reconciliation_fail"] == 0
    assert summary["reconciliation_pass"] == 15
    assert len(summary["batches_processed"]) == 6
    assert summary["exact_duplicates_removed"] >= 2
    rules = {row["rule_id"] for row in summary["quarantine"]}
    assert {
        "NEGATIVE_ON_HAND",
        "BAD_EMAIL",
        "CORRUPT_ROW",
        "RETURN_EXCEEDS_SALE",
        "ORPHAN_PAYMENT",
    } <= rules

    fact = _fact(spark, settings)
    cancelled = fact.filter(F.col("order_id") == C.CANCELLED_ORDER).collect()[0]
    assert cancelled.is_cancelled
    assert cancelled.net_sales_amt == 0
    assert cancelled.order_status == "CANCELLED"

    partial = fact.filter(F.col("order_id") == C.PARTIAL_ORDER).collect()[0]
    assert partial.qty_ordered == 4
    assert partial.qty_returned == 1
    assert partial.refund_amt == 229.0
    assert partial.net_sales_amt == 687.0
    assert partial.loyalty_tier == "SILVER"
    assert partial.list_price_asof == 229.0

    price = fact.filter(F.col("order_id") == C.PRICE_ORDER).collect()[0]
    assert price.list_price_asof == 249.0
    assert price.loyalty_tier == "GOLD"
    assert price.unit_price == 249.0

    categories = {
        row.order_id: row.category
        for row in fact.filter(F.col("order_id").isin(C.CAT_FRI_ORDER, C.CAT_SUN_ORDER)).collect()
    }
    assert categories[C.CAT_FRI_ORDER] == "Apparel"
    assert categories[C.CAT_SUN_ORDER] == "Outlet"

    late = fact.filter(F.col("order_id") == C.LATE_ORDER).collect()[0]
    assert late.is_inferred_customer and late.is_inferred_product
    ghost = fact.filter(F.col("order_id") == C.GHOST_ORDER).collect()[0]
    assert ghost.is_inferred_customer and not ghost.is_inferred_product

    customers = spark.read.format("delta").load(settings.table_path("silver", "customer_scd"))
    resolved = (
        customers.filter(F.col("customer_id") == C.LATE_CUSTOMER)
        .orderBy("effective_start")
        .collect()
    )
    assert len(resolved) == 2
    assert resolved[0].is_inferred and not resolved[0].is_current
    assert resolved[1].full_name == "Rowan Ellis" and resolved[1].is_current
    ghost_dim = customers.filter(
        (F.col("customer_id") == C.GHOST_CUSTOMER) & F.col("is_current")
    ).collect()[0]
    assert ghost_dim.is_inferred
    email = customers.filter(F.col("customer_id") == C.EMAIL_CUSTOMER).collect()
    assert len(email) == 1
    assert email[0].email == "samir.cole@northline.example"
    deleted = customers.filter(
        (F.col("customer_id") == C.DELETE_CUSTOMER) & F.col("is_current")
    ).collect()[0]
    assert deleted.is_deleted and deleted.status == "DELETED"

    products = spark.read.format("delta").load(settings.table_path("silver", "product_scd"))
    prices = products.filter(F.col("sku") == C.PRICE_SKU).orderBy("effective_start").collect()
    assert [row.list_price for row in prices] == ["229.00", "249.00"]
    assert (
        products.filter((F.col("sku") == C.DELETE_SKU) & F.col("is_current"))
        .collect()[0]
        .is_deleted
    )

    orders = spark.read.format("delta").load(settings.table_path("silver", "orders"))
    assert orders.filter(F.col("order_id") == C.DUP_ORDER).count() == 1
    assert orders.filter(F.col("order_id") == C.CANCELLED_ORDER).collect()[0].status == "CANCELLED"
    moves = spark.read.format("delta").load(settings.table_path("silver", "inventory_movement"))
    assert moves.filter(F.col("movement_id") == C.REPLAY_MOVEMENT).count() == 1
    assert moves.filter(F.col("movement_id") == C.NEGATIVE_MOVEMENT).count() == 0

    plan = (
        fact.join(
            F.broadcast(spark.read.format("delta").load(settings.table_path("gold", "dim_store"))),
            "store_id",
        )
        .limit(5)
        ._jdf.queryExecution()
        .executedPlan()
        .toString()
    )
    assert "Broadcast" in plan

    rerun = run_pipeline(settings, spark, skip_optimize=True)
    assert rerun["batches_processed"] == []
    assert len(rerun["batches_skipped"]) == 6
    assert rerun["reconciliation_fail"] == 0
    assert _fact(spark, settings).count() == fact.count()

    forced = run_pipeline(settings, spark, force=True, skip_optimize=True)
    assert len(forced["batches_processed"]) == 6
    assert forced["reconciliation_fail"] == 0
    assert _fact(spark, settings).count() == summary["gold_counts"]["fact_sales"]
    assert (
        spark.read.format("delta")
        .load(settings.table_path("silver", "orders"))
        .filter(F.col("order_id") == C.CANCELLED_ORDER)
        .collect()[0]
        .status
        == "CANCELLED"
    )


def test_fail_after_silver_recovers_on_the_next_run(spark, tmp_path, monkeypatch):
    landing = tmp_path / "landing"
    lake = tmp_path / "lake"
    logs = tmp_path / "logs"
    generate("tiny", landing, 42)
    monkeypatch.setenv("NL_LANDING", str(landing))
    monkeypatch.setenv("NL_LOCAL_ROOT", str(lake))
    monkeypatch.setenv("NL_LOG_DIR", str(logs))
    monkeypatch.setenv("NL_STORAGE_MODE", "local")
    settings = load_settings("test")
    try:
        run_pipeline(settings, spark, fail_after="silver", skip_optimize=True)
        raise AssertionError("expected InducedFailure")
    except InducedFailure:
        pass
    assert not (lake / "gold" / "fact_sales" / "_delta_log").exists()
    recovered = run_pipeline(settings, spark, skip_optimize=True)
    assert recovered["status"] == "SUCCESS"
    assert len(recovered["batches_skipped"]) == 6
    assert recovered["gold_counts"]["fact_sales"] > 0
    assert recovered["reconciliation_fail"] == 0
