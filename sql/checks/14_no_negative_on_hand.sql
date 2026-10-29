WITH opening AS (
    SELECT sku, store_id, on_hand_qty
    FROM silver_snapshots
    WHERE snapshot_date = (SELECT MIN(snapshot_date) FROM silver_snapshots)
),
running AS (
    SELECT
        moves.sku,
        moves.store_id,
        moves.movement_id,
        COALESCE(opening.on_hand_qty, 0) + SUM(moves.qty_signed) OVER (
            PARTITION BY moves.sku, moves.store_id
            ORDER BY moves.movement_ts, moves.movement_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS on_hand
    FROM silver_movements moves
    LEFT JOIN opening
        ON moves.sku = opening.sku
        AND moves.store_id = opening.store_id
)
SELECT
    'no_negative_on_hand' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    '0' AS expected,
    CAST(COUNT(*) AS STRING) AS actual,
    COALESCE(CONCAT_WS(',', COLLECT_LIST(movement_id)), '') AS detail
FROM running
WHERE on_hand < 0
