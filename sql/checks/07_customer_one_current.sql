SELECT
    'customer_one_current' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    '0' AS expected,
    CAST(COUNT(*) AS STRING) AS actual,
    COALESCE(CONCAT_WS(',', COLLECT_LIST(customer_id)), '') AS detail
FROM (
    SELECT customer_id
    FROM silver_customer_scd
    WHERE is_current = true
    GROUP BY customer_id
    HAVING COUNT(*) <> 1
)
