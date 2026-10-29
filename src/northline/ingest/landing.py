"""Spark CSV reader with an explicit string schema and a corrupt-record column."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType


def read_entity(spark: SparkSession, path: str, columns: list[str]) -> DataFrame:
    schema = StructType(
        [StructField(column, StringType(), True) for column in columns]
        + [StructField("_corrupt_record", StringType(), True)]
    )
    return (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema)
        .csv(path)
    )
