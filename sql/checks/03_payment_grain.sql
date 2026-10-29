SELECT
    'payment_grain_unique' AS check_name,
    CASE WHEN COUNT(*) = COUNT(DISTINCT payment_id) THEN 'PASS' ELSE 'FAIL' END AS status,
    CAST(COUNT(DISTINCT payment_id) AS STRING) AS expected,
    CAST(COUNT(*) AS STRING) AS actual,
    'silver_payments' AS detail
FROM silver_payments
WHERE is_deleted = false
