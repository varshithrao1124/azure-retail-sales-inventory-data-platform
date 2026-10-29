SELECT
    'order_grain_unique' AS check_name,
    CASE WHEN COUNT(*) = COUNT(DISTINCT order_id) THEN 'PASS' ELSE 'FAIL' END AS status,
    CAST(COUNT(DISTINCT order_id) AS STRING) AS expected,
    CAST(COUNT(*) AS STRING) AS actual,
    'silver_orders' AS detail
FROM silver_orders
WHERE is_deleted = false
