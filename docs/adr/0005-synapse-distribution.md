# Hash on order_id and sku, not store_id

`sql/synapse/dedicated/02_ctas_gold.sql` is the dedicated-pool design. It was not executed.

Hashing facts by `store_id` would put almost all rows on a handful of distributions (8 locations in the sample). `order_id` spreads `fact_sales`, `fact_returns`, and `fact_payments` and colocates an order's lines, refunds, and tenders. Dimensions are `REPLICATE`, so the join to customer, product, store, and channel does not need the fact to be distributed by the dimension key.

Inventory facts are hashed by `sku` so the variance join between movements and snapshots is collocated on the higher-cardinality key. Dedicated SQL hash distribution is a single column, so store stays a residual predicate inside the columnstore.

`fact_sales` is range-partitioned by month on `order_date`. Inventory facts are not daily-partitioned in the pool; a daily partition scheme would over-fragment this grain. Clustered columnstore is the fact index. Replicated dimensions use a clustered index on the surrogate.
