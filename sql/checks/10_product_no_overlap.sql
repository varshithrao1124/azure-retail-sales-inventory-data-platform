SELECT
    'product_no_overlap' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    '0' AS expected,
    CAST(COUNT(*) AS STRING) AS actual,
    COALESCE(CONCAT_WS(',', COLLECT_LIST(sku)), '') AS detail
FROM (
    SELECT DISTINCT a.sku
    FROM silver_product_scd a
    JOIN silver_product_scd b
        ON a.sku = b.sku
        AND a.effective_start < b.effective_start
        AND (a.effective_end IS NULL OR a.effective_end > b.effective_start)
)
