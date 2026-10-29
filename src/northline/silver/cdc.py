"""Current-state MERGE for orders, lines, payments, and returns.

The highest sequence in the micro-batch wins. An incoming sequence that is
not strictly greater than the stored sequence is a stale replay and is ignored.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from northline.config import Settings
from northline.io_delta import delta_exists, write_delta


def merge_current(
    spark: SparkSession,
    path: str,
    incoming: DataFrame,
    keys: list[str],
    columns: list[str],
    settings: Settings,
) -> None:
    if incoming.limit(1).count() == 0:
        return
    window = Window.partitionBy(*keys).orderBy(F.col("sequence_number").cast("long").desc())
    source = (
        incoming.withColumn("sequence_number", F.col("sequence_number").cast("long"))
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("is_deleted", F.col("op") == F.lit("D"))
    )
    keep = keys + columns + ["sequence_number", "op", "is_deleted"]
    source = source.select(*keep)
    if not delta_exists(path):
        write_delta(source, path, settings, mode="append", operation="cdc-init-" + "-".join(keys))
        return

    from delta.tables import DeltaTable

    condition = " AND ".join(f"t.{key} = s.{key}" for key in keys)
    assignments = {column: f"s.{column}" for column in columns}
    assignments["sequence_number"] = "s.sequence_number"
    assignments["op"] = "s.op"
    assignments["is_deleted"] = "s.is_deleted"
    inserts = {column: f"s.{column}" for column in keep}
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(source.alias("s"), condition)
        .whenMatchedUpdate(condition="s.sequence_number > t.sequence_number", set=assignments)
        .whenNotMatchedInsert(values=inserts)
        .execute()
    )
