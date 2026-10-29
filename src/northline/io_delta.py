"""Delta read/write helpers. Empty tables are created only when a schema is provided."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from northline.config import Settings
from northline.retry import call_with_retry


def delta_exists(path: str) -> bool:
    if path.startswith(("abfss://", "dbfs:", "wasbs://")):
        return False
    return (Path(path) / "_delta_log").exists()


def read_delta(spark: SparkSession, path: str, schema: StructType | None = None) -> DataFrame:
    if delta_exists(path):
        return spark.read.format("delta").load(path)
    if path.startswith(("abfss://", "dbfs:", "wasbs://")):
        try:
            return spark.read.format("delta").load(path)
        except Exception:
            if schema is None:
                raise
            return spark.createDataFrame([], schema)
    if schema is None:
        raise FileNotFoundError(path)
    return spark.createDataFrame([], schema)


def write_delta(
    df: DataFrame,
    path: str,
    settings: Settings,
    *,
    mode: str = "append",
    partition_by: list[str] | None = None,
    operation: str = "delta-write",
) -> None:
    def _write() -> None:
        writer = df.write.format("delta").mode(mode).option("mergeSchema", "true")
        if mode == "overwrite":
            writer = writer.option("overwriteSchema", "true").option(
                "partitionOverwriteMode", "static"
            )
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)

    call_with_retry(
        _write,
        attempts=settings.retry_attempts,
        backoff_seconds=settings.retry_backoff_seconds,
        operation=operation,
    )


def optimize_zorder(spark: SparkSession, path: str, columns: list[str]) -> dict:
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()[0].asDict()
    before = int(detail.get("numFiles") or 0)
    size_before = int(detail.get("sizeInBytes") or 0)
    cols = ", ".join(columns)
    spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({cols})")
    after_row = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()[0].asDict()
    return {
        "path": path,
        "zorder": columns,
        "files_before": before,
        "files_after": int(after_row.get("numFiles") or 0),
        "bytes_before": size_before,
        "bytes_after": int(after_row.get("sizeInBytes") or 0),
    }
