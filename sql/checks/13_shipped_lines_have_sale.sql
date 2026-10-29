SELECT
    'shipped_lines_have_sale_movement' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    '0' AS expected,
    CAST(COUNT(*) AS STRING) AS actual,
    COALESCE(CONCAT_WS(',', COLLECT_LIST(CONCAT(order_id, '-', line_number))), '') AS detail
FROM (
    SELECT fact_sales.order_id, fact_sales.line_number
    FROM fact_sales
    LEFT JOIN fact_inventory_movement moves
        ON moves.reference_id = CONCAT(fact_sales.order_id, '-', fact_sales.line_number)
        AND moves.movement_type = 'SALE'
    WHERE fact_sales.is_cancelled = false
        AND moves.movement_id IS NULL
)
