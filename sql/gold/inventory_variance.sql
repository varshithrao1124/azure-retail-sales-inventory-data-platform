WITH snaps AS (
    SELECT
        sku,
        store_id,
        snapshot_date,
        on_hand_qty,
        LAG(on_hand_qty) OVER (PARTITION BY sku, store_id ORDER BY snapshot_date) AS prev_on_hand,
        LAG(snapshot_date) OVER (PARTITION BY sku, store_id ORDER BY snapshot_date) AS prev_date
    FROM fact_inventory_snapshot
),
moves AS (
    SELECT
        s.sku,
        s.store_id,
        s.snapshot_date,
        SUM(m.qty_signed) AS movement_qty
    FROM snaps s
    JOIN fact_inventory_movement m
        ON m.sku = s.sku
        AND m.store_id = s.store_id
        AND m.movement_date > s.prev_date
        AND m.movement_date <= s.snapshot_date
    WHERE s.prev_date IS NOT NULL
    GROUP BY s.sku, s.store_id, s.snapshot_date
)
SELECT
    s.snapshot_date,
    s.sku,
    s.store_id,
    s.on_hand_qty AS snapshot_qty,
    s.prev_on_hand,
    COALESCE(m.movement_qty, 0) AS movement_qty,
    CAST(s.prev_on_hand + COALESCE(m.movement_qty, 0) AS INT) AS rolled_qty,
    CAST(s.on_hand_qty - (s.prev_on_hand + COALESCE(m.movement_qty, 0)) AS INT) AS variance_qty
FROM snaps s
LEFT JOIN moves m
    ON s.sku = m.sku
    AND s.store_id = m.store_id
    AND s.snapshot_date = m.snapshot_date
WHERE s.prev_on_hand IS NOT NULL
