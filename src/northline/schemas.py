"""Landing column contracts. Bronze keeps these as strings plus ingest metadata."""

from __future__ import annotations

CUSTOMERS = [
    "customer_id",
    "full_name",
    "email",
    "segment",
    "loyalty_tier",
    "home_store_id",
    "city",
    "state",
    "postal_code",
    "status",
    "created_date",
    "extract_ts",
]
PRODUCTS = [
    "sku",
    "product_name",
    "category",
    "subcategory",
    "brand",
    "unit_cost",
    "list_price",
    "status",
    "extract_ts",
]
STORES = [
    "store_id",
    "store_name",
    "store_type",
    "region",
    "state",
    "city",
    "square_feet",
    "open_date",
    "extract_ts",
]
CHANNELS = ["channel_id", "channel_name", "extract_ts"]
PROMOTIONS = [
    "promo_id",
    "promo_name",
    "applies_to_category",
    "discount_pct",
    "start_date",
    "end_date",
    "extract_ts",
]
ORDERS = [
    "order_id",
    "order_ts",
    "customer_id",
    "store_id",
    "channel_id",
    "promo_id",
    "status",
    "sequence_number",
    "extract_ts",
]
ORDER_LINES = [
    "order_id",
    "line_number",
    "sku",
    "qty",
    "unit_price",
    "discount_amt",
    "tax_amt",
    "line_status",
    "sequence_number",
    "extract_ts",
]
PAYMENTS = [
    "payment_id",
    "order_id",
    "tender_type",
    "amount",
    "status",
    "paid_ts",
    "sequence_number",
    "extract_ts",
]
RETURNS = [
    "return_id",
    "order_id",
    "line_number",
    "sku",
    "qty_returned",
    "refund_amt",
    "reason_code",
    "return_ts",
    "sequence_number",
    "extract_ts",
]
MOVEMENTS = [
    "movement_id",
    "sku",
    "store_id",
    "movement_type",
    "qty",
    "sign",
    "unit_cost",
    "movement_ts",
    "reference_id",
    "reason_code",
    "extract_ts",
]
SNAPSHOTS = [
    "snapshot_date",
    "sku",
    "store_id",
    "on_hand_qty",
    "on_hand_cost",
    "extract_ts",
]
CDC_CUSTOMERS = [
    "sequence_number",
    "op",
    "change_ts",
    "customer_id",
    "full_name",
    "email",
    "segment",
    "loyalty_tier",
    "home_store_id",
    "city",
    "state",
    "postal_code",
    "status",
    "extract_ts",
]
CDC_PRODUCTS = [
    "sequence_number",
    "op",
    "change_ts",
    "sku",
    "product_name",
    "category",
    "subcategory",
    "brand",
    "unit_cost",
    "list_price",
    "status",
    "extract_ts",
]
CDC_ORDERS = [
    "sequence_number",
    "op",
    "change_ts",
    "order_id",
    "order_ts",
    "customer_id",
    "store_id",
    "channel_id",
    "promo_id",
    "status",
    "extract_ts",
]
CDC_ORDER_LINES = [
    "sequence_number",
    "op",
    "change_ts",
    "order_id",
    "line_number",
    "sku",
    "qty",
    "unit_price",
    "discount_amt",
    "tax_amt",
    "line_status",
    "extract_ts",
]
CDC_PAYMENTS = [
    "sequence_number",
    "op",
    "change_ts",
    "payment_id",
    "order_id",
    "tender_type",
    "amount",
    "status",
    "paid_ts",
    "extract_ts",
]

COLUMNS = {
    "customers": CUSTOMERS,
    "products": PRODUCTS,
    "stores": STORES,
    "channels": CHANNELS,
    "promotions": PROMOTIONS,
    "orders": ORDERS,
    "order_lines": ORDER_LINES,
    "payments": PAYMENTS,
    "returns": RETURNS,
    "inventory_movements": MOVEMENTS,
    "inventory_snapshots": SNAPSHOTS,
    "cdc_customers": CDC_CUSTOMERS,
    "cdc_products": CDC_PRODUCTS,
    "cdc_orders": CDC_ORDERS,
    "cdc_order_lines": CDC_ORDER_LINES,
    "cdc_payments": CDC_PAYMENTS,
}

FILE_NAMES = {name: f"{name}.csv" for name in COLUMNS}

CUSTOMER_TRACKED = [
    "full_name",
    "email",
    "segment",
    "loyalty_tier",
    "home_store_id",
    "city",
    "state",
    "postal_code",
    "status",
]
PRODUCT_TRACKED = [
    "product_name",
    "category",
    "subcategory",
    "brand",
    "unit_cost",
    "list_price",
    "status",
]

ORDER_STATUSES = ["OPEN", "SHIPPED", "CANCELLED", "PARTIALLY_RETURNED", "RETURNED"]
LINE_STATUSES = ["OPEN", "SHIPPED", "CANCELLED"]
PAYMENT_STATUSES = ["CAPTURED", "VOIDED", "REFUNDED"]
TENDERS = ["CARD", "GIFT_CARD", "PAYPAL", "STORE_CREDIT"]
MOVEMENT_TYPES = [
    "RECEIPT",
    "SALE",
    "TRANSFER_IN",
    "TRANSFER_OUT",
    "ADJUSTMENT",
    "SHRINK",
    "RETURN",
]
CUSTOMER_STATUSES = ["ACTIVE", "SUSPENDED", "DELETED"]
PRODUCT_STATUSES = ["ACTIVE", "DISCONTINUED"]
SEGMENTS = ["LOYALTY", "OCCASIONAL", "NEW", "EMPLOYEE"]
TIERS = ["NONE", "SILVER", "GOLD", "TRAIL"]
OPS = ["I", "U", "D"]
