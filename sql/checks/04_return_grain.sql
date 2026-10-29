SELECT
    'return_grain_unique' AS check_name,
    CASE WHEN COUNT(*) = COUNT(DISTINCT return_id) THEN 'PASS' ELSE 'FAIL' END AS status,
    CAST(COUNT(DISTINCT return_id) AS STRING) AS expected,
    CAST(COUNT(*) AS STRING) AS actual,
    'silver_returns' AS detail
FROM silver_returns
WHERE is_deleted = false
