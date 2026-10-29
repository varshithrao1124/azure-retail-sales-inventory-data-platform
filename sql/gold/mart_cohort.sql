WITH firsts AS (
    SELECT
        customer_id,
        date_trunc('month', MIN(order_date)) AS cohort_month
    FROM fact_sales
    WHERE is_cancelled = false
    GROUP BY customer_id
)
SELECT
    firsts.cohort_month,
    date_trunc('month', fact_sales.order_date) AS activity_month,
    COUNT(DISTINCT fact_sales.customer_id) AS customers,
    SUM(fact_sales.net_sales_amt) AS net_sales_amt
FROM fact_sales
JOIN firsts ON fact_sales.customer_id = firsts.customer_id
WHERE fact_sales.is_cancelled = false
GROUP BY firsts.cohort_month, date_trunc('month', fact_sales.order_date)
