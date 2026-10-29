"""End-to-end medallion run used by the CLI, tests, and the Databricks job."""

from __future__ import annotations

import json
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from northline.audit.alerts import emit_alert
from northline.audit.reconcile import reconcile
from northline.config import Settings
from northline.errors import InducedFailure, ReconciliationError
from northline.gold.build import build_gold, optimize_gold
from northline.ingest.landing import read_entity
from northline.io_delta import delta_exists, read_delta, write_delta
from northline.logging_utils import get_logger
from northline.schemas import COLUMNS
from northline.silver.apply import apply_silver_batch

LOGGER = get_logger("runner")


def new_run_id() -> str:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{uuid.uuid4().hex[:8]}"


def list_batches(settings: Settings, spark: SparkSession | None = None) -> list[tuple[str, str]]:
    """Return (batch_ts, path) sorted by batch timestamp."""

    landing = settings.landing_dir
    if settings.storage_mode == "local":
        root = Path(landing)
        found = []
        for path in sorted(root.glob("batch_ts=*")):
            if path.is_dir():
                found.append((path.name.split("=", 1)[1], str(path)))
        return found
    if spark is None:
        raise RuntimeError("ADLS batch listing needs a Spark session")
    jvm = spark._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    uri = jvm.java.net.URI(landing)
    filesystem = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
    statuses = filesystem.listStatus(jvm.org.apache.hadoop.fs.Path(landing))
    found = []
    for status in statuses:
        name = status.getPath().getName()
        if name.startswith("batch_ts=") and status.isDirectory():
            found.append((name.split("=", 1)[1], status.getPath().toString()))
    return sorted(found)


def _committed(spark: SparkSession, settings: Settings) -> set[str]:
    path = settings.table_path("audit", "batch_commit")
    if not delta_exists(path):
        return set()
    rows = (
        read_delta(spark, path).filter(F.col("status") == "committed").select("batch_ts").collect()
    )
    return {row.batch_ts for row in rows}


def _commit(spark: SparkSession, settings: Settings, batch_ts: str, run_id: str) -> None:
    frame = spark.createDataFrame(
        [{"batch_ts": batch_ts, "status": "committed", "run_id": run_id, "committed_at": ""}]
    ).withColumn("committed_at", F.current_timestamp().cast("string"))
    write_delta(
        frame,
        settings.table_path("audit", "batch_commit"),
        settings,
        mode="append",
        operation="commit",
    )


def _land_bronze(
    spark: SparkSession,
    settings: Settings,
    entity: str,
    batch_ts: str,
    frame: DataFrame,
    run_id: str,
) -> int:
    columns = [column for column in frame.columns]
    hashed = (
        frame.withColumn("_batch_ts", F.lit(batch_ts))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_ingest_ts", F.current_timestamp().cast("string"))
        .withColumn(
            "_ingest_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.lit(batch_ts),
                    F.lit(entity),
                    *[F.coalesce(F.col(column), F.lit("")) for column in columns],
                ),
                256,
            ),
        )
    )
    before = hashed.count()
    hashed = hashed.dropDuplicates(["_ingest_id"])
    dropped = before - hashed.count()
    path = settings.table_path("bronze", entity)
    if delta_exists(path):
        existing = read_delta(spark, path).select("_ingest_id")
        hashed = hashed.join(existing, "_ingest_id", "left_anti")
    appended = hashed.count()
    if appended:
        write_delta(hashed, path, settings, mode="append", operation=f"bronze-{entity}")
    return dropped


def _write_run(spark: SparkSession, settings: Settings, row: dict) -> None:
    frame = spark.createDataFrame([row])
    path = settings.table_path("audit", "pipeline_run")
    if not delta_exists(path):
        write_delta(frame, path, settings, mode="append", operation="pipeline-run")
        return
    from delta.tables import DeltaTable

    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(frame.alias("s"), "t.run_id = s.run_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _write_metrics(spark: SparkSession, settings: Settings, metrics: list[dict]) -> None:
    if not metrics:
        return
    frame = spark.createDataFrame(metrics)
    write_delta(
        frame,
        settings.table_path("audit", "dq_metric"),
        settings,
        mode="append",
        operation="dq-metric",
    )


def run_pipeline(
    settings: Settings,
    spark: SparkSession,
    *,
    force: bool = False,
    fail_after: str | None = None,
    skip_optimize: bool = False,
) -> dict:
    started = time.perf_counter()
    run_id = new_run_id()
    LOGGER.info("start %s", run_id, extra={"run_id": run_id})
    start_row = {
        "run_id": run_id,
        "pipeline_name": settings.pipeline_name,
        "environment": settings.environment,
        "status": "RUNNING",
        "started_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ended_at": "",
        "duration_seconds": 0.0,
        "batches_processed": 0,
        "batches_skipped": 0,
        "error_message": "",
    }
    _write_run(spark, settings, start_row)

    processed: list[str] = []
    skipped: list[str] = []
    exact_duplicates = 0
    metrics: list[dict] = []
    error = ""
    status = "SUCCESS"
    try:
        done = _committed(spark, settings)
        for batch_ts, folder in list_batches(settings, spark):
            if batch_ts in done and not force:
                skipped.append(batch_ts)
                LOGGER.info("skip %s", batch_ts, extra={"run_id": run_id, "batch_ts": batch_ts})
                continue
            tables: dict[str, DataFrame] = {}
            for entity, columns in COLUMNS.items():
                path = str(Path(folder) / f"{entity}.csv")
                if settings.storage_mode == "local" and not Path(path).exists():
                    continue
                if settings.storage_mode != "local":
                    path = folder.rstrip("/") + f"/{entity}.csv"
                try:
                    frame = read_entity(spark, path, columns)
                except Exception:
                    if settings.storage_mode != "local":
                        continue
                    raise
                if (
                    frame.limit(1).count() == 0
                    and frame.filter(F.col("_corrupt_record").isNotNull()).count() == 0
                ):
                    # Header-only files are ignored. A file of only corrupt rows still lands.
                    if frame.count() == 0:
                        continue
                exact_duplicates += _land_bronze(spark, settings, entity, batch_ts, frame, run_id)
                tables[entity] = frame
            apply_silver_batch(spark, settings, batch_ts, run_id, tables, metrics)
            _commit(spark, settings, batch_ts, run_id)
            processed.append(batch_ts)
            LOGGER.info("committed %s", batch_ts, extra={"run_id": run_id, "batch_ts": batch_ts})

        _write_metrics(spark, settings, metrics)
        for metric in metrics:
            if metric["rows_seen"] and metric["fail_rate"] >= settings.dq_fail_rate_alert_threshold:
                emit_alert(
                    spark,
                    settings,
                    run_id=run_id,
                    severity="WARN",
                    code="DQ_FAIL_RATE",
                    message=(
                        f"{metric['entity']} {metric['batch_ts']} "
                        f"failed {metric['rows_failed']}/{metric['rows_seen']} "
                        f"({metric['fail_rate']:.3f})"
                    ),
                )
        if fail_after == "silver":
            raise InducedFailure("Stopped after silver for recovery test")

        counts = build_gold(spark, settings)
        optimize = [] if skip_optimize else optimize_gold(spark, settings)
        checks = reconcile(spark, settings, run_id)
        for row in spark.table("inventory_variance").filter(F.col("variance_qty") != 0).collect():
            emit_alert(
                spark,
                settings,
                run_id=run_id,
                severity="INFO",
                code="INVENTORY_VARIANCE",
                message=(
                    f"{row.sku} {row.store_id} {row.snapshot_date} variance_qty={row.variance_qty}"
                ),
            )
    except ReconciliationError as exc:
        status = "FAILED"
        error = str(exc)
        optimize = locals().get("optimize", [])
        counts = locals().get("counts", {})
        checks = []
        LOGGER.error("%s", exc, extra={"run_id": run_id})
        emit_alert(
            spark, settings, run_id=run_id, severity="ERROR", code="RECONCILIATION", message=error
        )
    except InducedFailure as exc:
        status = "FAILED"
        error = str(exc)
        optimize = []
        counts = {}
        checks = []
        LOGGER.error("%s", exc, extra={"run_id": run_id})
    except Exception as exc:
        status = "FAILED"
        error = f"{type(exc).__name__}: {exc}"
        optimize = locals().get("optimize", [])
        counts = locals().get("counts", {})
        checks = []
        LOGGER.exception("pipeline failed", extra={"run_id": run_id})
        emit_alert(
            spark, settings, run_id=run_id, severity="ERROR", code="PIPELINE_FAILED", message=error
        )
        raise
    finally:
        duration = round(time.perf_counter() - started, 3)
        final = {
            "run_id": run_id,
            "pipeline_name": settings.pipeline_name,
            "environment": settings.environment,
            "status": status,
            "started_at": start_row["started_at"],
            "ended_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "duration_seconds": float(duration),
            "batches_processed": len(processed),
            "batches_skipped": len(skipped),
            "error_message": error,
        }
        try:
            _write_run(spark, settings, final)
        except Exception:
            LOGGER.exception("could not update pipeline_run", extra={"run_id": run_id})

    if status == "FAILED" and error.startswith("Stopped after silver"):
        summary = {
            "run_id": run_id,
            "status": status,
            "error_message": error,
            "batches_processed": processed,
            "batches_skipped": skipped,
            "duration_seconds": round(time.perf_counter() - started, 3),
        }
        raise InducedFailure(error)

    if status == "FAILED" and error.startswith("Reconciliation"):
        raise ReconciliationError(error)

    quarantine_path = settings.table_path("silver", "quarantine")
    quarantine = []
    if delta_exists(quarantine_path):
        quarantine = [
            {"rule_id": row.rule_id, "rows": row.rows}
            for row in read_delta(spark, quarantine_path)
            .groupBy("rule_id")
            .count()
            .withColumnRenamed("count", "rows")
            .collect()
        ]
    net_sales = None
    aov = None
    return_rate = None
    fact_path = settings.table_path("gold", "fact_sales")
    if delta_exists(fact_path):
        fact = read_delta(spark, fact_path)
        totals = fact.agg(
            F.round(F.sum("net_sales_amt"), 2).alias("net_sales"),
            F.sum(F.when(F.col("is_cancelled"), F.lit(0)).otherwise(F.col("qty_ordered"))).alias(
                "units"
            ),
            F.sum("qty_returned").alias("returned"),
        ).collect()[0]
        orders = fact.filter(~F.col("is_cancelled")).select("order_id").distinct().count()
        net_sales = float(totals.net_sales or 0)
        aov = round(net_sales / orders, 2) if orders else None
        return_rate = (
            round(float(totals.returned or 0) / float(totals.units), 4) if totals.units else None
        )

    summary = {
        "run_id": run_id,
        "status": status,
        "environment": settings.environment,
        "storage_mode": settings.storage_mode,
        "catalog_mode": settings.catalog_mode,
        "duration_seconds": round(time.perf_counter() - started, 3),
        "batches_processed": processed,
        "batches_skipped": skipped,
        "exact_duplicates_removed": exact_duplicates,
        "gold_counts": counts,
        "net_sales": net_sales,
        "aov": aov,
        "return_rate_units": return_rate,
        "quarantine": quarantine,
        "reconciliation_pass": sum(1 for row in checks if row["status"] == "PASS"),
        "reconciliation_fail": sum(1 for row in checks if row["status"] != "PASS"),
        "optimize": optimize,
        "error_message": error,
    }
    log_path = Path(settings.log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    (log_path / "latest_summary.json").write_text(
        json.dumps(summary, indent=2, default=str) + "\n", encoding="utf-8"
    )
    if status != "SUCCESS":
        raise RuntimeError(error or status)
    return summary
