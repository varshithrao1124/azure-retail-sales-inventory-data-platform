WITH sales AS (
    SELECT
        sku,
        store_id,
        movement_date,
        SUM(CASE WHEN movement_type = 'SALE' THEN qty ELSE 0 END) AS units,
        SUM(CASE WHEN movement_type = 'SALE' THEN qty * unit_cost ELSE 0 END) AS cogs
    FROM fact_inventory_movement
    GROUP BY sku, store_id, movement_date
),
windowed AS (
    SELECT
        s.sku,
        s.store_id,
        s.snapshot_date,
        COALESCE(SUM(sales.units), 0) AS units_sold,
        COALESCE(SUM(sales.cogs), 0) AS cogs
    FROM fact_inventory_snapshot s
    LEFT JOIN sales
        ON sales.sku = s.sku
        AND sales.store_id = s.store_id
        AND sales.movement_date > date_sub(s.snapshot_date, __WINDOW_DAYS__)
        AND sales.movement_date <= s.snapshot_date
    GROUP BY s.sku, s.store_id, s.snapshot_date
),
cost AS (
    SELECT
        s.sku,
        s.store_id,
        s.snapshot_date,
        AVG(h.on_hand_cost) AS avg_cost
    FROM fact_inventory_snapshot s
    JOIN fact_inventory_snapshot h
        ON h.sku = s.sku
        AND h.store_id = s.store_id
        AND h.snapshot_date > date_sub(s.snapshot_date, __WINDOW_DAYS__)
        AND h.snapshot_date <= s.snapshot_date
    GROUP BY s.sku, s.store_id, s.snapshot_date
)
SELECT
    w.snapshot_date,
    w.sku,
    w.store_id,
    s.category,
    s.on_hand_qty,
    s.on_hand_cost,
    w.units_sold AS units_sold_window,
    w.cogs AS cogs_window,
    CASE
        WHEN w.units_sold + s.on_hand_qty = 0 THEN NULL
        ELSE w.units_sold / (w.units_sold + s.on_hand_qty)
    END AS sell_through,
    CASE
        WHEN w.units_sold = 0 THEN NULL
        ELSE s.on_hand_qty / (w.units_sold / __WINDOW_DAYS__)
    END AS days_of_supply,
    CASE
        WHEN COALESCE(c.avg_cost, 0) = 0 THEN NULL
        ELSE w.cogs / c.avg_cost
    END AS turnover_window,
    CASE WHEN s.on_hand_qty = 0 THEN 1 ELSE 0 END AS is_stockout
FROM windowed w
JOIN fact_inventory_snapshot s
    ON s.sku = w.sku
    AND s.store_id = w.store_id
    AND s.snapshot_date = w.snapshot_date
JOIN cost c
    ON c.sku = w.sku
    AND c.store_id = w.store_id
    AND c.snapshot_date = w.snapshot_date
