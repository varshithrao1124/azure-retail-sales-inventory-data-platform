WITH bounds AS (
    SELECT MAX(order_date) AS as_of FROM fact_sales
),
base AS (
    SELECT
        customer_id,
        MAX(order_date) AS last_order,
        COUNT(DISTINCT order_id) AS frequency,
        SUM(net_sales_amt) AS monetary
    FROM fact_sales
    WHERE is_cancelled = false
    GROUP BY customer_id
)
SELECT
    customer_id,
    datediff(bounds.as_of, base.last_order) AS recency_days,
    base.frequency,
    base.monetary,
    CAST(6 - NTILE(5) OVER (ORDER BY datediff(bounds.as_of, base.last_order) ASC) AS INT) AS r_score,
    CAST(6 - NTILE(5) OVER (ORDER BY base.frequency DESC) AS INT) AS f_score,
    CAST(6 - NTILE(5) OVER (ORDER BY base.monetary DESC) AS INT) AS m_score
FROM base
CROSS JOIN bounds
