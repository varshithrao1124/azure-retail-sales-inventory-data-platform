"""Databricks Python task. Cluster env selects ADLS and Unity Catalog.

Example cluster env:
  NL_ENV=dev
  NL_STORAGE_MODE=adls
  NL_CATALOG_MODE=unity
  NL_STORAGE_ACCOUNT=stnlretaildev
"""

from __future__ import annotations

import argparse
import os

from northline.config import load_settings
from northline.logging_utils import configure_logging
from northline.pipeline.runner import run_pipeline
from northline.spark_session import build_spark


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=os.environ.get("NL_ENV", "dev"))
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    settings = load_settings(args.env)
    configure_logging(settings.log_dir, console=True)
    spark = build_spark(settings)
    summary = run_pipeline(settings, spark, force=args.force)
    print(summary["run_id"], summary["status"], summary["duration_seconds"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
