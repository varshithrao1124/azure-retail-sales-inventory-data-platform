"""SCD2 and current-state MERGE without the full generator."""

from pyspark.sql import functions as F

from northline.config import load_settings
from northline.silver.cdc import merge_current
from northline.silver.scd import apply_scd2, bootstrap_snapshot, infer_members


def test_scd_versions_noop_and_stale_replay(spark, tmp_path, monkeypatch):
    monkeypatch.setenv("NL_LOCAL_ROOT", str(tmp_path / "lake"))
    monkeypatch.setenv("NL_LOG_DIR", str(tmp_path / "logs"))
    settings = load_settings("test")
    path = settings.table_path("silver", "customer_scd")
    tracked = ["loyalty_tier", "status"]
    snapshot = spark.createDataFrame(
        [{"customer_id": "C000001", "loyalty_tier": "SILVER", "status": "ACTIVE"}]
    )
    assert (
        bootstrap_snapshot(spark, path, snapshot, "customer_id", tracked, settings, "b1", "r1") == 1
    )
    assert (
        bootstrap_snapshot(spark, path, snapshot, "customer_id", tracked, settings, "b1", "r2") == 0
    )

    same = spark.createDataFrame(
        [
            {
                "customer_id": "C000001",
                "op": "U",
                "sequence_number": "2",
                "change_ts": "2024-11-09T06:00:00Z",
                "loyalty_tier": "SILVER",
                "status": "ACTIVE",
            }
        ]
    )
    apply_scd2(spark, path, same, "customer_id", tracked, settings, "b2", "r2")
    rows = spark.read.format("delta").load(path)
    assert rows.count() == 1
    assert rows.collect()[0].applied_sequence == 2

    change = spark.createDataFrame(
        [
            {
                "customer_id": "C000001",
                "op": "U",
                "sequence_number": "5",
                "change_ts": "2024-11-09T06:00:00Z",
                "loyalty_tier": "GOLD",
                "status": "ACTIVE",
            },
            {
                "customer_id": "C000001",
                "op": "U",
                "sequence_number": "3",
                "change_ts": "2024-11-08T06:00:00Z",
                "loyalty_tier": "TRAIL",
                "status": "ACTIVE",
            },
        ]
    )
    apply_scd2(spark, path, change, "customer_id", tracked, settings, "b3", "r3")
    history = rows = (
        spark.read.format("delta").load(path).orderBy("effective_start", "applied_sequence")
    )
    assert history.count() == 3
    current = history.filter(F.col("is_current")).collect()[0]
    assert current.loyalty_tier == "GOLD"
    assert current.applied_sequence == 5
    stale = spark.createDataFrame(
        [
            {
                "customer_id": "C000001",
                "op": "U",
                "sequence_number": "4",
                "change_ts": "2024-11-09T07:00:00Z",
                "loyalty_tier": "NONE",
                "status": "ACTIVE",
            }
        ]
    )
    apply_scd2(spark, path, stale, "customer_id", tracked, settings, "b4", "r4")
    current = spark.read.format("delta").load(path).filter(F.col("is_current")).collect()[0]
    assert current.loyalty_tier == "GOLD"
    assert current.applied_sequence == 5


def test_infer_then_real_insert_closes_the_member(spark, tmp_path, monkeypatch):
    monkeypatch.setenv("NL_LOCAL_ROOT", str(tmp_path / "lake2"))
    monkeypatch.setenv("NL_LOG_DIR", str(tmp_path / "logs2"))
    settings = load_settings("test")
    path = settings.table_path("silver", "customer_scd")
    tracked = ["loyalty_tier", "status"]
    keys = spark.createDataFrame([{"customer_id": "C000999"}])
    infer_members(
        spark,
        path,
        keys,
        "customer_id",
        tracked,
        {"loyalty_tier": "UNKNOWN", "status": "UNKNOWN"},
        settings,
        "b1",
        "r1",
    )
    real = spark.createDataFrame(
        [
            {
                "customer_id": "C000999",
                "op": "I",
                "sequence_number": "8",
                "change_ts": "2024-11-10T18:00:00Z",
                "loyalty_tier": "TRAIL",
                "status": "ACTIVE",
            }
        ]
    )
    apply_scd2(spark, path, real, "customer_id", tracked, settings, "b2", "r2")
    rows = spark.read.format("delta").load(path).orderBy("effective_start").collect()
    assert len(rows) == 2
    assert rows[0].is_inferred and not rows[0].is_current
    assert rows[1].loyalty_tier == "TRAIL" and rows[1].is_current and not rows[1].is_inferred


def test_merge_ignores_stale_order_status(spark, tmp_path, monkeypatch):
    monkeypatch.setenv("NL_LOCAL_ROOT", str(tmp_path / "lake3"))
    monkeypatch.setenv("NL_LOG_DIR", str(tmp_path / "logs3"))
    settings = load_settings("test")
    path = settings.table_path("silver", "orders")
    first = spark.createDataFrame(
        [
            {
                "op": "I",
                "sequence_number": 5,
                "order_id": "O-CAN-0001",
                "status": "OPEN",
                "batch_ts": "b1",
                "run_id": "r1",
            }
        ]
    )
    merge_current(spark, path, first, ["order_id"], ["status", "batch_ts", "run_id"], settings)
    cancel = spark.createDataFrame(
        [
            {
                "op": "U",
                "sequence_number": 9,
                "order_id": "O-CAN-0001",
                "status": "CANCELLED",
                "batch_ts": "b2",
                "run_id": "r2",
            }
        ]
    )
    merge_current(spark, path, cancel, ["order_id"], ["status", "batch_ts", "run_id"], settings)
    stale = first.withColumn("sequence_number", F.lit(5).cast("long"))
    merge_current(spark, path, stale, ["order_id"], ["status", "batch_ts", "run_id"], settings)
    rows = spark.read.format("delta").load(path).collect()
    assert len(rows) == 1
    assert rows[0].status == "CANCELLED"
    assert rows[0].sequence_number == 9
