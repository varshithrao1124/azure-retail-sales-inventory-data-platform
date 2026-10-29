"""SparkSession factory. Local runs pull the Delta jar via delta-spark."""

from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from northline.config import Settings


def build_spark(settings: Settings) -> SparkSession:
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    if settings.storage_mode == "local":
        os.makedirs(settings.lake_root, exist_ok=True)
        os.makedirs(settings.log_dir, exist_ok=True)
        warehouse = os.path.join(settings.lake_root, "_warehouse")
        os.makedirs(warehouse, exist_ok=True)
    else:
        warehouse = os.path.join(settings.log_dir, "_warehouse")
        os.makedirs(warehouse, exist_ok=True)

    builder = (
        SparkSession.builder.appName(settings.spark_app_name)
        .master(settings.spark_master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.driver.memory", settings.driver_memory)
        .config("spark.sql.shuffle.partitions", str(settings.shuffle_partitions))
        .config("spark.default.parallelism", str(settings.shuffle_partitions))
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", str(settings.broadcast_threshold))
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
