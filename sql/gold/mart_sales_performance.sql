SELECT
    order_date,
    channel_id,
    store_id,
    category,
    COUNT(DISTINCT CASE WHEN is_cancelled THEN NULL ELSE order_id END) AS orders,
    CAST(SUM(gross_sales_amt) AS DOUBLE) AS gross_sales_amt,
    CAST(SUM(net_sales_amt) AS DOUBLE) AS net_sales_amt,
    CAST(SUM(CASE WHEN is_cancelled THEN 0 ELSE qty_ordered END) AS BIGINT) AS units_ordered,
    CAST(SUM(qty_returned) AS BIGINT) AS units_returned,
    CAST(SUM(qty_net) AS BIGINT) AS units_net,
    CASE
        WHEN SUM(CASE WHEN is_cancelled THEN 0 ELSE qty_ordered END) = 0 THEN NULL
        ELSE SUM(qty_returned) / SUM(CASE WHEN is_cancelled THEN 0 ELSE qty_ordered END)
    END AS return_rate_units,
    CASE
        WHEN COUNT(DISTINCT CASE WHEN is_cancelled THEN NULL ELSE order_id END) = 0 THEN NULL
        ELSE SUM(net_sales_amt) / COUNT(DISTINCT CASE WHEN is_cancelled THEN NULL ELSE order_id END)
    END AS aov
FROM fact_sales
GROUP BY order_date, channel_id, store_id, category
