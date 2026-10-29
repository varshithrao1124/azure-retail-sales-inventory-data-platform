SELECT
    'line_grain_unique' AS check_name,
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT CONCAT(order_id, '-', line_number)) THEN 'PASS'
        ELSE 'FAIL'
    END AS status,
    CAST(COUNT(DISTINCT CONCAT(order_id, '-', line_number)) AS STRING) AS expected,
    CAST(COUNT(*) AS STRING) AS actual,
    'silver_order_lines' AS detail
FROM silver_order_lines
WHERE is_deleted = false
