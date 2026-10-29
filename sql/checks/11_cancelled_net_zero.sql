SELECT
    'cancelled_net_sales_zero' AS check_name,
    CASE WHEN COALESCE(SUM(net_sales_amt), 0) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    '0' AS expected,
    CAST(COALESCE(SUM(net_sales_amt), 0) AS STRING) AS actual,
    'fact_sales.is_cancelled' AS detail
FROM fact_sales
WHERE is_cancelled = true
