SELECT
    'product_one_current' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    '0' AS expected,
    CAST(COUNT(*) AS STRING) AS actual,
    COALESCE(CONCAT_WS(',', COLLECT_LIST(sku)), '') AS detail
FROM (
    SELECT sku
    FROM silver_product_scd
    WHERE is_current = true
    GROUP BY sku
    HAVING COUNT(*) <> 1
)
