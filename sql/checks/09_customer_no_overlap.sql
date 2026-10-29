SELECT
    'customer_no_overlap' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    '0' AS expected,
    CAST(COUNT(*) AS STRING) AS actual,
    COALESCE(CONCAT_WS(',', COLLECT_LIST(customer_id)), '') AS detail
FROM (
    SELECT DISTINCT a.customer_id
    FROM silver_customer_scd a
    JOIN silver_customer_scd b
        ON a.customer_id = b.customer_id
        AND a.effective_start < b.effective_start
        AND (a.effective_end IS NULL OR a.effective_end > b.effective_start)
)
