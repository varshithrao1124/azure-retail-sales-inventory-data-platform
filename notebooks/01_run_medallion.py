# Databricks notebook source
# Same library the local CLI imports. Attach the repo to the cluster path
# and set NL_STORAGE_MODE=adls plus NL_CATALOG_MODE=unity before running.

import os
import sys

notebook_dir = os.getcwd()
for candidate in (notebook_dir, os.path.dirname(notebook_dir)):
    src = os.path.join(candidate, "src")
    if os.path.isdir(src) and src not in sys.path:
        sys.path.insert(0, src)

env = os.environ.get("NL_ENV", "dev")
if "dbutils" in globals():
    dbutils.widgets.text("env", env)  # type: ignore[name-defined]
    env = dbutils.widgets.get("env")  # type: ignore[name-defined]

from northline.config import load_settings
from northline.pipeline.runner import run_pipeline
from northline.spark_session import build_spark

settings = load_settings(env)
spark = build_spark(settings)
summary = run_pipeline(settings, spark)
print(summary["run_id"], summary["status"], summary["net_sales"])
