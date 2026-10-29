"""SCD Type 2 for customer and product, including inferred late-arriving members.

Changes are applied one sequence wave at a time so two updates for the same
key in one file both survive. A replay with an older sequence is ignored.
An attribute-identical update moves ``applied_sequence`` and does not open a
new version. Inferred members are replaced when the real change arrives.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from northline.config import Settings
from northline.io_delta import delta_exists, read_delta, write_delta

META = [
    "effective_start",
    "effective_end",
    "is_current",
    "is_inferred",
    "is_deleted",
    "applied_sequence",
    "change_op",
    "record_hash",
    "batch_ts",
    "run_id",
]


def with_hash(df: DataFrame, tracked: list[str]) -> DataFrame:
    parts = [F.coalesce(F.col(column).cast("string"), F.lit("")) for column in tracked]
    return df.withColumn("record_hash", F.sha2(F.concat_ws("|", *parts), 256))


def _version(
    df: DataFrame,
    key: str,
    tracked: list[str],
    *,
    start: F.Column,
    op: F.Column,
    sequence: F.Column,
    inferred: bool,
    deleted: F.Column,
    batch_ts: str,
    run_id: str,
) -> DataFrame:
    return df.select(
        F.col(key).alias(key),
        *[F.col(column).cast("string").alias(column) for column in tracked],
        start.alias("effective_start"),
        F.lit(None).cast("string").alias("effective_end"),
        F.lit(True).alias("is_current"),
        F.lit(inferred).alias("is_inferred"),
        deleted.alias("is_deleted"),
        sequence.cast("long").alias("applied_sequence"),
        op.alias("change_op"),
        F.col("record_hash"),
        F.lit(batch_ts).alias("batch_ts"),
        F.lit(run_id).alias("run_id"),
    )


def bootstrap_snapshot(
    spark: SparkSession,
    path: str,
    snapshot: DataFrame,
    key: str,
    tracked: list[str],
    settings: Settings,
    batch_ts: str,
    run_id: str,
) -> int:
    if snapshot.limit(1).count() == 0:
        return 0
    hashed = with_hash(snapshot, tracked)
    rows = _version(
        hashed,
        key,
        tracked,
        start=F.lit(settings.scd2_epoch),
        op=F.lit("SNAPSHOT"),
        sequence=F.lit(0),
        inferred=False,
        deleted=F.lit(False),
        batch_ts=batch_ts,
        run_id=run_id,
    )
    if delta_exists(path):
        existing = read_delta(spark, path).select(key).distinct()
        rows = rows.join(existing, key, "left_anti")
    added = rows.count()
    if added:
        write_delta(rows, path, settings, mode="append", operation=f"scd-bootstrap-{key}")
    return added


def infer_members(
    spark: SparkSession,
    path: str,
    keys_df: DataFrame,
    key: str,
    tracked: list[str],
    fills: dict[str, str | None],
    settings: Settings,
    batch_ts: str,
    run_id: str,
) -> int:
    keys = (
        keys_df.select(F.col(key).cast("string").alias(key))
        .where(F.col(key).isNotNull() & (F.length(F.trim(F.col(key))) > 0))
        .distinct()
    )
    if delta_exists(path):
        current = read_delta(spark, path).filter(F.col("is_current")).select(key)
        missing = keys.join(current, key, "left_anti")
    else:
        missing = keys
    if missing.limit(1).count() == 0:
        return 0
    filled = missing
    for column in tracked:
        value = fills.get(column)
        filled = filled.withColumn(
            column,
            F.lit(value).cast("string") if value is not None else F.lit(None).cast("string"),
        )
    filled = with_hash(filled, tracked)
    rows = _version(
        filled,
        key,
        tracked,
        start=F.lit(settings.scd2_epoch),
        op=F.lit("INFER"),
        sequence=F.lit(-1),
        inferred=True,
        deleted=F.lit(False),
        batch_ts=batch_ts,
        run_id=run_id,
    )
    added = rows.count()
    if added:
        write_delta(rows, path, settings, mode="append", operation=f"scd-infer-{key}")
    return added


def apply_scd2(
    spark: SparkSession,
    path: str,
    incoming: DataFrame,
    key: str,
    tracked: list[str],
    settings: Settings,
    batch_ts: str,
    run_id: str,
) -> int:
    if incoming.limit(1).count() == 0:
        return 0
    pending = with_hash(incoming, tracked).withColumn(
        "sequence_number", F.col("sequence_number").cast("long")
    )
    pending = pending.localCheckpoint()
    applied = 0
    for _ in range(30):
        if pending.limit(1).count() == 0:
            break
        window = Window.partitionBy(key).orderBy(F.col("sequence_number").asc())
        wave = (
            pending.withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
        if not delta_exists(path):
            fresh = wave.withColumn("_cseq", F.lit(None).cast("long"))
            fresh = fresh.withColumn("_chash", F.lit(None).cast("string"))
            fresh = fresh.withColumn("_cinferred", F.lit(False))
            noop = fresh.filter(F.lit(False))
            change = fresh
            stale_keys = wave.limit(0).select(key, "sequence_number")
        else:
            current = (
                read_delta(spark, path)
                .filter(F.col("is_current"))
                .select(
                    F.col(key).alias("_ckey"),
                    F.col("record_hash").alias("_chash"),
                    F.col("applied_sequence").alias("_cseq"),
                    F.col("is_inferred").alias("_cinferred"),
                )
            )
            joined = wave.join(current, wave[key] == current["_ckey"], "left")
            stale = joined.filter(
                F.col("_cseq").isNotNull() & (F.col("_cseq") >= F.col("sequence_number"))
            )
            fresh = joined.filter(
                F.col("_cseq").isNull() | (F.col("_cseq") < F.col("sequence_number"))
            )
            noop = fresh.filter(
                (F.col("op") == "U")
                & F.col("_cseq").isNotNull()
                & (~F.coalesce(F.col("_cinferred"), F.lit(False)))
                & (F.col("_chash") == F.col("record_hash"))
            )
            change = fresh.join(
                noop.select(key, "sequence_number"), [key, "sequence_number"], "left_anti"
            )
            stale_keys = stale.select(key, "sequence_number")

        if noop.limit(1).count() > 0:
            from delta.tables import DeltaTable

            source = noop.select(
                F.col(key).alias(key), F.col("sequence_number").alias("sequence_number")
            )
            (
                DeltaTable.forPath(spark, path)
                .alias("t")
                .merge(source.alias("s"), f"t.{key} = s.{key} AND t.is_current = true")
                .whenMatchedUpdate(set={"applied_sequence": "s.sequence_number"})
                .execute()
            )
        to_close = (
            change.filter(F.col("_cseq").isNotNull())
            if "_cseq" in change.columns
            else change.limit(0)
        )
        if delta_exists(path) and to_close.limit(1).count() > 0:
            from delta.tables import DeltaTable

            source = to_close.select(F.col(key).alias(key), F.col("change_ts").alias("change_ts"))
            (
                DeltaTable.forPath(spark, path)
                .alias("t")
                .merge(source.alias("s"), f"t.{key} = s.{key} AND t.is_current = true")
                .whenMatchedUpdate(set={"is_current": "false", "effective_end": "s.change_ts"})
                .execute()
            )
        if change.limit(1).count() > 0:
            rows = _version(
                change,
                key,
                tracked,
                start=F.col("change_ts"),
                op=F.col("op"),
                sequence=F.col("sequence_number"),
                inferred=False,
                deleted=F.col("op") == "D",
                batch_ts=batch_ts,
                run_id=run_id,
            )
            write_delta(rows, path, settings, mode="append", operation=f"scd-append-{key}")
            applied += rows.count()
        done = (
            wave.select(key, "sequence_number")
            .unionByName(stale_keys)
            .dropDuplicates([key, "sequence_number"])
        )
        pending = pending.join(done, [key, "sequence_number"], "left_anti").localCheckpoint()
    return applied
