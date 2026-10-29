SELECT
    'movement_grain_unique' AS check_name,
    CASE WHEN COUNT(*) = COUNT(DISTINCT movement_id) THEN 'PASS' ELSE 'FAIL' END AS status,
    CAST(COUNT(DISTINCT movement_id) AS STRING) AS expected,
    CAST(COUNT(*) AS STRING) AS actual,
    'silver_movements' AS detail
FROM silver_movements
