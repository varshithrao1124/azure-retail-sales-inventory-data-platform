"""Command line for generate, run, and the local dashboard."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys

from northline.config import load_settings, project_root
from northline.errors import ConfigError, InducedFailure, ReconciliationError


def _print_summary(summary: dict) -> None:
    print(f"Northline Outfitters medallion run {summary['run_id']}")
    print(
        f"status={summary['status']} env={summary['environment']} "
        f"storage={summary['storage_mode']} catalog={summary['catalog_mode']} "
        f"duration_s={summary['duration_seconds']}"
    )
    print(
        "batches_processed="
        + ",".join(summary["batches_processed"])
        + f" skipped={len(summary['batches_skipped'])}"
    )
    print(f"exact_duplicates_removed={summary['exact_duplicates_removed']}")
    print(f"gold_counts={json.dumps(summary['gold_counts'])}")
    print(
        f"net_sales={summary['net_sales']} aov={summary['aov']} "
        f"return_rate_units={summary['return_rate_units']}"
    )
    print("quarantine=" + json.dumps(summary["quarantine"]))
    print(
        f"reconciliation_pass={summary['reconciliation_pass']} "
        f"fail={summary['reconciliation_fail']}"
    )
    print("optimize=" + json.dumps(summary["optimize"]))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="northline")
    sub = parser.add_subparsers(dest="command", required=True)

    generate = sub.add_parser("generate", help="Write a seeded synthetic landing drop")
    generate.add_argument("--scale", default="small", choices=["tiny", "small", "medium"])
    generate.add_argument("--output", default="data/sample")
    generate.add_argument("--seed", type=int, default=42)

    run = sub.add_parser("run", help="Run bronze, silver, gold, and reconciliation")
    run.add_argument("--env", default="dev")
    run.add_argument("--force", action="store_true")
    run.add_argument("--fail-after", choices=["silver"], default=None)
    run.add_argument("--skip-optimize", action="store_true")

    dash = sub.add_parser("dashboard", help="Serve the local Streamlit dashboard")
    dash.add_argument("--port", type=int, default=8501)
    dash.add_argument("--env", default="dev")

    args = parser.parse_args(argv)
    try:
        if args.command == "generate":
            from northline.generator.synthesize import generate as generate_data

            manifest = generate_data(args.scale, args.output, args.seed)
            print(
                f"generated scale={manifest['scale']} seed={manifest['seed']} "
                f"batches={len(manifest['batches'])} files={len(manifest['file_counts'])} "
                f"variances={len(manifest['variances'])} -> {args.output}"
            )
            return 0
        if args.command == "dashboard":
            app = project_root() / "dashboard" / "app.py"
            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "streamlit",
                    "run",
                    str(app),
                    "--server.port",
                    str(args.port),
                    "--server.headless",
                    "true",
                ],
                check=False,
            )
            return completed.returncode
        settings = load_settings(args.env)
        from northline.logging_utils import configure_logging
        from northline.spark_session import build_spark

        configure_logging(settings.log_dir, console=True)
        spark = build_spark(settings)
        try:
            summary = run_pipeline_cmd(settings, spark, args)
        finally:
            spark.stop()
        _print_summary(summary)
        return 0
    except (ConfigError, InducedFailure, ReconciliationError, RuntimeError) as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        return 1


def run_pipeline_cmd(settings, spark, args):
    from northline.pipeline.runner import run_pipeline

    return run_pipeline(
        settings,
        spark,
        force=args.force,
        fail_after=args.fail_after,
        skip_optimize=args.skip_optimize,
    )


if __name__ == "__main__":
    raise SystemExit(main())
