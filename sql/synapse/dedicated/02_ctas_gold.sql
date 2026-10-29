-- Load dedicated-pool tables from the serverless views (external data source)
-- or from COPY INTO. Distribution choices:
--
-- fact_sales, fact_returns, fact_payments: HASH(order_id).
--   Store has too few values (8 in the sample, low dozens in production) and
--   would skew a HASH(store_id). order_id colocates sales, returns, and payments
--   for order-level joins. Dimensions are replicated, so the fact-to-dim join
--   does not need collocation.
-- fact_inventory_snapshot and fact_inventory_movement: HASH(sku).
--   Sku cardinality is higher than store, and the variance query joins those
--   two facts on sku. A single-column hash cannot include store_id.
-- Dimensions: REPLICATE. They are small relative to a 2 GB replicate limit
--   at this retail grain (customer SCD is the largest and still a dimension).
-- Indexes: clustered columnstore on facts. Replicated dimensions use a
--   clustered index on the surrogate key so point lookups from a report stay cheap.
-- Partition: fact_sales is monthly on order_date so a month-scoped finance
--   query can eliminate partitions. Inventory facts stay unpartitioned at this
--   grain; daily partitions on a dedicated pool would over-fragment a few
--   million snapshot rows.

CREATE TABLE gold.dim_store
WITH (DISTRIBUTION = REPLICATE, CLUSTERED INDEX (store_sk))
AS
SELECT * FROM ext.dim_store;

CREATE TABLE gold.dim_channel
WITH (DISTRIBUTION = REPLICATE, CLUSTERED INDEX (channel_sk))
AS
SELECT * FROM ext.dim_channel;

CREATE TABLE gold.dim_promotion
WITH (DISTRIBUTION = REPLICATE, CLUSTERED INDEX (promo_sk))
AS
SELECT * FROM ext.dim_promotion;

CREATE TABLE gold.dim_customer
WITH (DISTRIBUTION = REPLICATE, CLUSTERED INDEX (customer_sk))
AS
SELECT * FROM ext.dim_customer;

CREATE TABLE gold.dim_product
WITH (DISTRIBUTION = REPLICATE, CLUSTERED INDEX (product_sk))
AS
SELECT * FROM ext.dim_product;

CREATE TABLE gold.fact_sales
(
    sales_sk BIGINT NOT NULL,
    order_date_key INT NOT NULL,
    order_date DATE NOT NULL,
    order_ts VARCHAR(40) NULL,
    customer_sk BIGINT NULL,
    product_sk BIGINT NULL,
    store_sk BIGINT NULL,
    channel_sk BIGINT NULL,
    promo_sk BIGINT NULL,
    order_id VARCHAR(32) NOT NULL,
    line_number VARCHAR(8) NOT NULL,
    customer_id VARCHAR(16) NULL,
    sku VARCHAR(32) NULL,
    store_id VARCHAR(16) NULL,
    channel_id VARCHAR(32) NULL,
    promo_id VARCHAR(32) NULL,
    category VARCHAR(64) NULL,
    subcategory VARCHAR(64) NULL,
    loyalty_tier VARCHAR(32) NULL,
    segment VARCHAR(32) NULL,
    is_inferred_customer BIT NULL,
    is_inferred_product BIT NULL,
    qty_ordered INT NULL,
    qty_cancelled INT NULL,
    qty_returned INT NULL,
    qty_net INT NULL,
    unit_price DECIMAL(18, 2) NULL,
    list_price_asof DECIMAL(18, 2) NULL,
    unit_cost_asof DECIMAL(18, 2) NULL,
    discount_amt DECIMAL(18, 2) NULL,
    gross_sales_amt DECIMAL(18, 2) NULL,
    refund_amt DECIMAL(18, 2) NULL,
    net_sales_amt DECIMAL(18, 2) NULL,
    cogs_amt DECIMAL(18, 2) NULL,
    tax_amt DECIMAL(18, 2) NULL,
    is_cancelled BIT NULL,
    line_status VARCHAR(32) NULL,
    order_status VARCHAR(32) NULL
)
WITH (
    DISTRIBUTION = HASH(order_id),
    CLUSTERED COLUMNSTORE INDEX,
    PARTITION (
        order_date RANGE RIGHT FOR VALUES (
            '2024-11-01', '2024-12-01', '2025-01-01', '2025-02-01', '2025-03-01'
        )
    )
);

INSERT INTO gold.fact_sales
SELECT
    sales_sk, order_date_key, order_date, order_ts, customer_sk, product_sk, store_sk,
    channel_sk, promo_sk, order_id, line_number, customer_id, sku, store_id, channel_id,
    promo_id, category, subcategory, loyalty_tier, segment,
    CAST(is_inferred_customer AS BIT), CAST(is_inferred_product AS BIT),
    qty_ordered, qty_cancelled, qty_returned, qty_net,
    CAST(unit_price AS DECIMAL(18, 2)), CAST(list_price_asof AS DECIMAL(18, 2)),
    CAST(unit_cost_asof AS DECIMAL(18, 2)), CAST(discount_amt AS DECIMAL(18, 2)),
    CAST(gross_sales_amt AS DECIMAL(18, 2)), CAST(refund_amt AS DECIMAL(18, 2)),
    CAST(net_sales_amt AS DECIMAL(18, 2)), CAST(cogs_amt AS DECIMAL(18, 2)),
    CAST(tax_amt AS DECIMAL(18, 2)), CAST(is_cancelled AS BIT), line_status, order_status
FROM ext.fact_sales;

CREATE TABLE gold.fact_returns
WITH (DISTRIBUTION = HASH(order_id), CLUSTERED COLUMNSTORE INDEX)
AS
SELECT * FROM ext.fact_returns;

CREATE TABLE gold.fact_payments
WITH (DISTRIBUTION = HASH(order_id), CLUSTERED COLUMNSTORE INDEX)
AS
SELECT * FROM ext.fact_payments;

CREATE TABLE gold.fact_inventory_snapshot
WITH (DISTRIBUTION = HASH(sku), CLUSTERED COLUMNSTORE INDEX)
AS
SELECT * FROM ext.fact_inventory_snapshot;

CREATE TABLE gold.fact_inventory_movement
WITH (DISTRIBUTION = HASH(sku), CLUSTERED COLUMNSTORE INDEX)
AS
SELECT * FROM ext.fact_inventory_movement;
