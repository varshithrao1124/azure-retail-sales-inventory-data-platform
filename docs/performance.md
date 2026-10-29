# Performance notes (local measurements only)

These numbers are from one workstation run of the committed small sample (seed 42) on PySpark 3.5.5 and delta-spark 3.2.1, Java 21, `local[2]`, shuffle partitions 4. They are not Databricks or Synapse timings.

| Step | Measured |
| --- | --- |
| First `run --env dev` | 217.403 seconds, run `20261029T172649Z-ca9832c8`, status SUCCESS |
| Second run, same lake | 72.295 seconds, run `20261029T173107Z-50563b75`, 6 batches skipped, gold rebuilt |
| `OPTIMIZE fact_sales ZORDER BY (store_id, sku)` | 4 files / 56,965 bytes → 3 files / 46,850 bytes in 2.143 seconds |
| `OPTIMIZE fact_inventory_movement ZORDER BY (store_id, sku)` | 8 files / 41,776 bytes → 3 files / 19,290 bytes in 1.703 seconds |
| `OPTIMIZE fact_inventory_snapshot ZORDER BY (store_id, sku)` | 4 files / 18,464 bytes → 4 files / 18,464 bytes in 1.487 seconds |

The snapshot optimize did not reduce file count. At this volume each date partition is already a small file, and Z-order does not combine partitions. That is a reason to compact, not a reason to drop the date partition: the partition is there for predicate pruning once a day holds more than a handful of files.

Most of the 217 seconds is Spark job startup per action (DQ counts, SCD waves, MERGE), not shuffle of the 198 sales lines. The medium generator adds batches, so wall time grows with batch count more than with row count until the files get large.

AQE, skew-join handling, and a 10 MB broadcast threshold are on in `build_spark`. Store, channel, and promotion joins in the fact build use an explicit broadcast hint. The integration test asserts the executed plan for a fact-to-store join contains `Broadcast`.

Liquid clustering was not run. OSS Delta 3.2 on this machine is operated with `OPTIMIZE ZORDER`. Clustering DDL belongs on a Databricks runtime; claiming a local clustering speedup would be false.
