# Troubleshooting

## `JAVA_HOME` or Spark fails to start

PySpark needs a JDK (17 or 21). This repo was run with Java 21 and Python 3.12. `SPARK_LOCAL_IP=127.0.0.1` avoids a hostname lookup failure on some cloud VMs. The Makefile exports it.

## Delta jar download

The first session calls `configure_spark_with_delta_pip`, which downloads `delta-spark_2.12` 3.2.1. That needs network once. Later runs reuse `~/.ivy2`.

## `overwriteSchema` and dynamic partitions

Gold overwrite sets `partitionOverwriteMode=static` together with `overwriteSchema`. Leaving the session on dynamic partition overwrite raises `DELTA_OVERWRITE_SCHEMA_WITH_DYNAMIC_PARTITION_OVERWRITE`. Do not flip that session config back to `dynamic` without removing `overwriteSchema`.

## Empty gold or "run make pipeline"

The dashboard raises `FileNotFoundError` when `data/lake/gold/fact_sales` has no `_delta_log`. Run `make pipeline` from the repo root so `NL_LOCAL_ROOT` is the default `data/lake`.

## Reconciliation fails on variance

Compare `manifest.json` `variances` with `gold/inventory_variance`. A generator change that moves stock without updating the planted snapshot will fail the generator's own self-check before Spark starts. A pipeline bug that posts the negative shrink or double-posts the replayed receipt fails `no_negative_on_hand` or shifts the variance.

## SCD fan-out

`fact_line_coverage` fails when `fact_sales` has more rows than silver lines, which happens if two dimension versions match one event. The predicate is `event >= start AND (end IS NULL OR event < end)`. Overlapping `effective_end` values show up in `customer_no_overlap` / `product_no_overlap` first.

## Cancelled order came back to OPEN

The stale CDC row must keep the original lower `sequence_number`. If a replay is given a new sequence, MERGE will treat it as a real update. That is the source system's bug; the platform only protects ordered sequences.

## ADF copy watermark versus Databricks

A file can be copied and still skipped by the job, or skipped by ADF and never seen by the job. If silver is missing a batch, check `ctl.ingest_watermark` (cloud) and `audit.batch_commit` (lake). They are different clocks. See `docs/adr/0002-watermark-and-cdc.md`.

## Dedicated pool cost

`DW100c` bills while online. Pause it. The CD workflow will not deploy at all until Azure, Databricks, and Synapse secrets are set.

## `databricks bundle validate` host

`databricks.yml` defaults `workspace_host` to a placeholder. Real validate/deploy needs `DATABRICKS_HOST`. CD skips that step when the secret is empty.

## Medium generate

```bash
make generate-medium
```

writes `data/landing/medium` (4,000 customers, 14 days). It is not committed. Point `NL_LANDING` at it before `make pipeline`. Expect a longer Spark run than the sample; the job count is higher because there are more batches.
