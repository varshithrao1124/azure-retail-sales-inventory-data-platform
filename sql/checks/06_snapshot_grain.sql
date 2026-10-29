SELECT
    'snapshot_grain_unique' AS check_name,
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT CONCAT(snapshot_date, '|', sku, '|', store_id)) THEN 'PASS'
        ELSE 'FAIL'
    END AS status,
    CAST(COUNT(DISTINCT CONCAT(snapshot_date, '|', sku, '|', store_id)) AS STRING) AS expected,
    CAST(COUNT(*) AS STRING) AS actual,
    'silver_snapshots' AS detail
FROM silver_snapshots
