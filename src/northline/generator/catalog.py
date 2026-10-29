"""Stable identifiers planted in every scale so tests and the sample tell the same story."""

from __future__ import annotations

from decimal import Decimal

TIER_CUSTOMER = "C000001"
CANCEL_CUSTOMER = "C000002"
EMAIL_CUSTOMER = "C000003"
DELETE_CUSTOMER = "C000010"
GHOST_CUSTOMER = "C000998"
LATE_CUSTOMER = "C000999"

PRICE_SKU = "SKU-SHL-0001"
CATEGORY_SKU = "SKU-APP-0002"
DISCONTINUE_SKU = "SKU-APP-0003"
KETTLE_SKU = "SKU-CKN-0001"
STOCKOUT_SKU = "SKU-LGT-0003"
DELETE_SKU = "SKU-ERR-0001"
LATE_SKU = "SKU-NEW-0001"

VARIANCE_SKU = PRICE_SKU
VARIANCE_STORE = "STR01"
STOCKOUT_STORE = "STR02"
DC_STORE = "DC01"

CANCELLED_ORDER = "O-CAN-0001"
PARTIAL_ORDER = "O-RET-0001"
DUP_ORDER = "O-DUP-0001"
TIER_ORDER = "O-TIER-0001"
PRICE_ORDER = "O-PRICE-0001"
LATE_ORDER = "O-LATE-0001"
GHOST_ORDER = "O-GHOST-0001"
CAT_FRI_ORDER = "O-CAT-0001"
CAT_SUN_ORDER = "O-CAT-0002"
EMP_ORDER = "O-EMP-0001"

REPLAY_MOVEMENT = "M-FRI-0001"
NEGATIVE_MOVEMENT = "M-NEG-0001"
BAD_TYPE_MOVEMENT = "M-TYPE-0001"

PROMO_ID = "PROMO-TRAIL15"
EDGE_START = "2024-11-08"
VARIANCE_DATE = "2024-11-09"
REVERSAL_DATE = "2024-11-10"

TAX = {
    "WA": Decimal("0.065"),
    "OR": Decimal("0"),
    "CO": Decimal("0.029"),
    "CA": Decimal("0.0725"),
    "ID": Decimal("0.060"),
}

STORES = [
    ("STR01", "Ballard", "RETAIL", "Pacific", "WA", "Seattle", 8200, "2014-03-01"),
    ("STR02", "Hawthorne", "RETAIL", "Pacific", "OR", "Portland", 6100, "2016-11-12"),
    ("STR03", "Highlands", "RETAIL", "Mountain", "CO", "Denver", 7400, "2018-05-02"),
    ("STR04", "Mission", "RETAIL", "Pacific", "CA", "San Francisco", 5400, "2019-08-19"),
    ("STR05", "Hyde Park", "RETAIL", "Mountain", "ID", "Boise", 4800, "2021-04-07"),
    ("STR06", "Old Mill", "RETAIL", "Mountain", "OR", "Bend", 3900, "2022-09-15"),
    ("DC01", "Troutdale DC", "DC", "Pacific", "OR", "Troutdale", 42000, "2012-01-09"),
    ("MKT01", "Marketplace Node", "MARKETPLACE_NODE", "Pacific", "WA", "Seattle", 0, "2020-06-01"),
]

# sku, name, category, subcategory, brand, cost, list, status
EDGE_PRODUCTS = [
    ("SKU-SHL-0001", "Ridge 2 Tent", "Shelter", "Tents", "Northline", "96.00", "229.00", "ACTIVE"),
    ("SKU-APP-0002", "Cedar Flannel", "Apparel", "Shirts", "Northline", "22.00", "68.00", "ACTIVE"),
    ("SKU-APP-0003", "Mist Shell", "Apparel", "Shells", "Northline", "40.00", "120.00", "ACTIVE"),
    ("SKU-CKN-0001", "Kettle 1.2L", "Cook", "Stoves", "Fieldpot", "14.00", "34.00", "ACTIVE"),
    (
        "SKU-LGT-0003",
        "Beacon Lantern",
        "Light",
        "Lanterns",
        "Northline",
        "11.00",
        "28.00",
        "ACTIVE",
    ),
    (
        "SKU-ERR-0001",
        "Duplicate Stake Kit",
        "Shelter",
        "Accessories",
        "Northline",
        "4.00",
        "12.00",
        "ACTIVE",
    ),
]

LATE_PRODUCT = (
    "SKU-NEW-0001",
    "Trail Mug 12oz",
    "Cook",
    "Cups",
    "Northline",
    "6.00",
    "16.00",
    "ACTIVE",
)

CHANNELS = [
    ("RETAIL", "Retail store"),
    ("WEB", "northline.com"),
    ("MARKETPLACE", "Third-party marketplace"),
]

SCALE = {
    "tiny": {
        "customers": 12,
        "extra_skus": 0,
        "store_ids": ["STR01", "STR02", "DC01"],
        "orders_per_day": 4,
        "days": 3,
    },
    "small": {
        "customers": 100,
        "extra_skus": 24,
        "store_ids": [row[0] for row in STORES],
        "orders_per_day": 36,
        "days": 3,
    },
    "medium": {
        "customers": 4000,
        "extra_skus": 80,
        "store_ids": [row[0] for row in STORES],
        "orders_per_day": 180,
        "days": 14,
    },
}
