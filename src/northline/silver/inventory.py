"""Inventory snapshot upsert and the negative-on-hand guard.

On-hand is opening snapshot plus accepted movements, walked in timestamp order.
A movement that would cross below zero is quarantined and does not post.
Replays of a movement id that is already on the ledger are ignored.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
from pyspark.sql.window import Window

from northline.config import Settings
from northline.io_delta import delta_exists, read_delta, write_delta
from northline.measures import signed_qty

MOVEMENT_SCHEMA = StructType(
    [
        StructField("movement_id", StringType(), False),
        StructField("sku", StringType(), False),
        StructField("store_id", StringType(), False),
        StructField("movement_type", StringType(), False),
        StructField("qty", IntegerType(), False),
        StructField("sign", IntegerType(), False),
        StructField("qty_signed", IntegerType(), False),
        StructField("unit_cost", DoubleType(), False),
        StructField("movement_ts", StringType(), False),
        StructField("reference_id", StringType(), True),
        StructField("reason_code", StringType(), True),
        StructField("batch_ts", StringType(), False),
        StructField("run_id", StringType(), False),
    ]
)


def merge_snapshots(
    spark: SparkSession, path: str, incoming: DataFrame, settings: Settings
) -> None:
    if incoming.limit(1).count() == 0:
        return
    window = Window.partitionBy("snapshot_date", "sku", "store_id").orderBy(
        F.col("extract_ts").desc()
    )
    source = (
        incoming.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .select(
            "snapshot_date",
            "sku",
            "store_id",
            F.col("on_hand_qty").cast("int").alias("on_hand_qty"),
            F.col("on_hand_cost").cast("double").alias("on_hand_cost"),
            "extract_ts",
            "batch_ts",
            "run_id",
        )
    )
    if not delta_exists(path):
        write_delta(source, path, settings, mode="append", operation="snapshot-init")
        return
    from delta.tables import DeltaTable

    keys = ["snapshot_date", "sku", "store_id"]
    condition = " AND ".join(f"t.{key} = s.{key}" for key in keys)
    values = [
        "on_hand_qty",
        "on_hand_cost",
        "extract_ts",
        "batch_ts",
        "run_id",
    ]
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(source.alias("s"), condition)
        .whenMatchedUpdate(set={column: f"s.{column}" for column in values})
        .whenNotMatchedInsert(values={column: f"s.{column}" for column in keys + values})
        .execute()
    )


def post_movements(
    spark: SparkSession,
    path: str,
    snapshot_path: str,
    incoming: DataFrame,
    settings: Settings,
) -> tuple[int, DataFrame]:
    """Append movements that keep on-hand non-negative. Returns (posted, rejected rows)."""

    empty_reject = incoming.limit(0).withColumn("reject_rule", F.lit(""))
    if incoming.limit(1).count() == 0:
        return 0, empty_reject

    existing: set[str] = set()
    position: dict[tuple[str, str], int] = {}
    if delta_exists(snapshot_path):
        snapshots = read_delta(spark, snapshot_path)
        min_date = snapshots.agg(F.min("snapshot_date")).collect()[0][0]
        for row in (
            snapshots.filter(F.col("snapshot_date") == min_date)
            .select("sku", "store_id", "on_hand_qty")
            .collect()
        ):
            position[(row.sku, row.store_id)] = int(row.on_hand_qty)
    if delta_exists(path):
        posted = (
            read_delta(spark, path).select("movement_id", "sku", "store_id", "qty_signed").collect()
        )
        for row in posted:
            existing.add(row.movement_id)
            key = (row.sku, row.store_id)
            position[key] = position.get(key, 0) + int(row.qty_signed)

    accepted: list[dict] = []
    rejected_ids: list[tuple[str, str]] = []
    rows = [row.asDict(recursive=True) for row in incoming.collect()]
    for row in sorted(rows, key=lambda item: (item["movement_ts"], item["movement_id"])):
        if row["movement_id"] in existing:
            continue
        try:
            signed = signed_qty(row["movement_type"], int(row["qty"]), int(row["sign"]))
        except ValueError:
            rejected_ids.append((row["movement_id"], "BAD_MOVEMENT"))
            continue
        key = (row["sku"], row["store_id"])
        if position.get(key, 0) + signed < 0:
            rejected_ids.append((row["movement_id"], "NEGATIVE_ON_HAND"))
            continue
        position[key] = position.get(key, 0) + signed
        accepted.append(
            {
                "movement_id": row["movement_id"],
                "sku": row["sku"],
                "store_id": row["store_id"],
                "movement_type": row["movement_type"],
                "qty": int(row["qty"]),
                "sign": int(row["sign"]),
                "qty_signed": signed,
                "unit_cost": float(row["unit_cost"]),
                "movement_ts": row["movement_ts"],
                "reference_id": row.get("reference_id") or "",
                "reason_code": row.get("reason_code") or "",
                "batch_ts": row["batch_ts"],
                "run_id": row["run_id"],
            }
        )
        existing.add(row["movement_id"])

    posted_count = 0
    if accepted:
        frame = spark.createDataFrame(accepted, schema=MOVEMENT_SCHEMA)
        write_delta(frame, path, settings, mode="append", operation="movement-append")
        posted_count = len(accepted)

    if not rejected_ids:
        return posted_count, empty_reject
    reject_df = spark.createDataFrame(rejected_ids, ["movement_id", "reject_rule"])
    rejected = incoming.join(reject_df, "movement_id", "inner")
    return posted_count, rejected
