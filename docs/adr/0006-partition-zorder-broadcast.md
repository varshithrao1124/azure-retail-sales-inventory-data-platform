# Partition, Z-order, broadcast, AQE

Gold facts that are filtered by day (`fact_sales`, `fact_returns`, `fact_payments`, both inventory facts) are partitioned by that date. On the local sample that produces small files, which is why the job runs `OPTIMIZE ... ZORDER BY (store_id, sku)` on sales and inventory and records `numFiles` from `DESCRIBE DETAIL`. Those file counts in the README are from the local run only.

Z-order columns are the predicates that are not already the partition: store and sku. Z-ordering the partition column would not prune further.

Spark is started with AQE, skew-join handling, and a 10 MB broadcast threshold. Store, channel, and promotion are explicit `broadcast` hints in the fact build because they stay small in production. Customer and product SCD are not forced to broadcast; AQE can still broadcast them while they fit under the threshold.

Liquid clustering (`ALTER TABLE ... CLUSTER BY`) is a Databricks Runtime feature. The local OSS Delta 3.2 run uses Z-order. Do not read a local `numFiles` change as a liquid-clustering result.
