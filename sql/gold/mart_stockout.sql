SELECT
    sku,
    store_id,
    COUNT(*) AS stockout_days
FROM fact_inventory_snapshot
WHERE on_hand_qty = 0
GROUP BY sku, store_id
