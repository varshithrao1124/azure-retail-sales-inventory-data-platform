"""Alert rows plus an optional webhook. A failed webhook does not fail the run."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from urllib.parse import urlparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from northline.config import Settings
from northline.io_delta import write_delta
from northline.logging_utils import get_logger

LOGGER = get_logger("alerts")


def emit_alert(
    spark: SparkSession,
    settings: Settings,
    *,
    run_id: str,
    severity: str,
    code: str,
    message: str,
) -> None:
    frame = spark.createDataFrame(
        [
            {
                "run_id": run_id,
                "severity": severity,
                "code": code,
                "message": message,
                "created_at": "",
            }
        ]
    ).withColumn("created_at", F.current_timestamp().cast("string"))
    write_delta(
        frame, settings.table_path("audit", "alert"), settings, mode="append", operation="alert"
    )
    LOGGER.warning("%s %s", code, message, extra={"run_id": run_id})
    url = settings.alert_webhook_url
    if not url:
        return
    host = urlparse(url).netloc or "webhook"
    payload = json.dumps(
        {"run_id": run_id, "severity": severity, "code": code, "message": message}
    ).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            LOGGER.info(
                "alert webhook %s status %s", host, response.status, extra={"run_id": run_id}
            )
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        LOGGER.warning("alert webhook %s failed: %s", host, exc, extra={"run_id": run_id})
        failure = spark.createDataFrame(
            [
                {
                    "run_id": run_id,
                    "severity": "WARN",
                    "code": "WEBHOOK_FAILED",
                    "message": f"{host} {type(exc).__name__}",
                    "created_at": "",
                }
            ]
        ).withColumn("created_at", F.current_timestamp().cast("string"))
        write_delta(
            failure,
            settings.table_path("audit", "alert"),
            settings,
            mode="append",
            operation="alert-webhook",
        )
