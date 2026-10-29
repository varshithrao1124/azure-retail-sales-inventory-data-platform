"""The generator balances inventory before any Spark job sees the files."""

from northline.generator import catalog as C
from northline.generator.synthesize import generate


def test_tiny_manifest_plants_the_known_variance(tmp_path):
    manifest = generate("tiny", tmp_path / "landing", 42)
    assert manifest["batches"] == [
        "2024-11-08T0600",
        "2024-11-08T1800",
        "2024-11-09T0600",
        "2024-11-09T1800",
        "2024-11-10T0600",
        "2024-11-10T1800",
    ]
    assert manifest["variances"] == [
        {
            "sku": C.VARIANCE_SKU,
            "store_id": C.VARIANCE_STORE,
            "snapshot_date": "2024-11-09",
            "variance_qty": -1,
        },
        {
            "sku": C.VARIANCE_SKU,
            "store_id": C.VARIANCE_STORE,
            "snapshot_date": "2024-11-10",
            "variance_qty": 1,
        },
    ]
    orders = (tmp_path / "landing" / "batch_ts=2024-11-08T1800" / "orders.csv").read_text()
    assert orders.count(C.DUP_ORDER) == 2
    dirty = (tmp_path / "landing" / "batch_ts=2024-11-10T0600" / "order_lines.csv").read_text()
    assert "only,three,fields" in dirty
    assert (tmp_path / "landing" / "manifest.json").exists()
