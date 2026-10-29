SELECT
    'fact_line_coverage' AS check_name,
    CASE
        WHEN facts.n = lines.n AND facts.null_customer = 0 AND facts.null_product = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status,
    CAST(lines.n AS STRING) AS expected,
    CAST(facts.n AS STRING) AS actual,
    CONCAT(
        'null_customer=', CAST(facts.null_customer AS STRING),
        ';null_product=', CAST(facts.null_product AS STRING)
    ) AS detail
FROM (
    SELECT
        COUNT(*) AS n,
        SUM(CASE WHEN customer_sk IS NULL THEN 1 ELSE 0 END) AS null_customer,
        SUM(CASE WHEN product_sk IS NULL THEN 1 ELSE 0 END) AS null_product
    FROM fact_sales
) facts
CROSS JOIN (
    SELECT COUNT(*) AS n
    FROM silver_order_lines lines
    JOIN silver_orders orders ON lines.order_id = orders.order_id
    WHERE lines.is_deleted = false AND orders.is_deleted = false
) lines
