"""Sign rules and the pure-Python rollforward used by the generator."""

from northline.measures import compute_variances, signed_qty


def test_signed_qty_covers_every_movement_type():
    assert signed_qty("RECEIPT", 4) == 4
    assert signed_qty("SALE", 4) == -4
    assert signed_qty("TRANSFER_IN", 2) == 2
    assert signed_qty("TRANSFER_OUT", 2) == -2
    assert signed_qty("RETURN", 1) == 1
    assert signed_qty("SHRINK", 1) == -1
    assert signed_qty("ADJUSTMENT", 3, 1) == 3
    assert signed_qty("ADJUSTMENT", 3, -1) == -3


def test_adjustment_requires_a_sign():
    try:
        signed_qty("ADJUSTMENT", 1, None)
    except ValueError as exc:
        assert "sign" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_rollforward_skips_opening_and_sums_the_gap():
    snapshots = [
        {
            "sku": "SKU-SHL-0001",
            "store_id": "STR01",
            "snapshot_date": "2024-11-07",
            "on_hand_qty": 10,
        },
        {
            "sku": "SKU-SHL-0001",
            "store_id": "STR01",
            "snapshot_date": "2024-11-08",
            "on_hand_qty": 8,
        },
        {
            "sku": "SKU-SHL-0001",
            "store_id": "STR01",
            "snapshot_date": "2024-11-09",
            "on_hand_qty": 6,
        },
    ]
    movements = [
        {
            "sku": "SKU-SHL-0001",
            "store_id": "STR01",
            "movement_ts": "2024-11-08T16:00:00Z",
            "movement_type": "SALE",
            "qty": 2,
            "sign": -1,
        },
        {
            "sku": "SKU-SHL-0001",
            "store_id": "STR01",
            "movement_ts": "2024-11-09T12:00:00Z",
            "movement_type": "SALE",
            "qty": 1,
            "sign": -1,
        },
    ]
    found = compute_variances(snapshots, movements)
    assert found == [
        {
            "sku": "SKU-SHL-0001",
            "store_id": "STR01",
            "snapshot_date": "2024-11-09",
            "variance_qty": -1,
        }
    ]
