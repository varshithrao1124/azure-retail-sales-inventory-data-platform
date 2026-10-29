"""Row-level data-quality rules.

A failing row can produce several quarantine records, one per rule. Corrupt
lines are not also scored against every null check.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from northline.schemas import (
    CUSTOMER_STATUSES,
    LINE_STATUSES,
    MOVEMENT_TYPES,
    OPS,
    ORDER_STATUSES,
    PAYMENT_STATUSES,
    PRODUCT_STATUSES,
    TENDERS,
)

TS = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"
DATE = r"^\d{4}-\d{2}-\d{2}$"
ORDER_ID = r"^(O-[A-Z]+-\d{4}|O\d{13})$"
CUSTOMER_ID = r"^C\d{6}$"
SKU = r"^SKU-[A-Z]{3}-\d{4}$"
STORE = r"^(STR|DC|MKT)\d{2}$"
MONEY = r"^\d+(\.\d+)?$"
POS_INT = r"^[1-9]\d*$"
SEQ = r"^\d+$"
EMAIL = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
STATE = r"^[A-Z]{2}$"


def _missing(column: str) -> F.Column:
    return F.col(column).isNull() | (F.length(F.trim(F.col(column))) == 0)


def _bad(column: str, pattern: str) -> F.Column:
    return _missing(column) | ~F.col(column).rlike(pattern)


def _not_in(column: str, values: list[str]) -> F.Column:
    return _missing(column) | ~F.col(column).isin(values)


def rules_for(entity: str) -> list[tuple[str, F.Column]]:
    common_ts = {
        "orders": ("order_ts", "BAD_ORDER_TS"),
        "payments": ("paid_ts", "BAD_PAID_TS"),
        "returns": ("return_ts", "BAD_RETURN_TS"),
        "inventory_movements": ("movement_ts", "BAD_MOVEMENT_TS"),
        "cdc_orders": ("change_ts", "BAD_CHANGE_TS"),
        "cdc_customers": ("change_ts", "BAD_CHANGE_TS"),
        "cdc_products": ("change_ts", "BAD_CHANGE_TS"),
        "cdc_order_lines": ("change_ts", "BAD_CHANGE_TS"),
        "cdc_payments": ("change_ts", "BAD_CHANGE_TS"),
    }
    rules: list[tuple[str, F.Column]] = []
    if entity in common_ts:
        column, rule_id = common_ts[entity]
        rules.append((rule_id, _bad(column, TS)))

    if entity in {"orders", "cdc_orders"}:
        rules.extend(
            [
                ("BAD_ORDER_ID", _bad("order_id", ORDER_ID)),
                ("BAD_CUSTOMER_ID", _bad("customer_id", CUSTOMER_ID)),
                ("BAD_STORE", _bad("store_id", STORE)),
                ("BAD_CHANNEL", _not_in("channel_id", ["RETAIL", "WEB", "MARKETPLACE"])),
                ("BAD_STATUS", _not_in("status", ORDER_STATUSES)),
                ("BAD_SEQUENCE", _bad("sequence_number", SEQ)),
            ]
        )
    elif entity in {"order_lines", "cdc_order_lines"}:
        rules.extend(
            [
                ("BAD_ORDER_ID", _bad("order_id", ORDER_ID)),
                ("BAD_LINE", _bad("line_number", POS_INT)),
                ("BAD_SKU", _bad("sku", SKU)),
                ("BAD_QTY", _bad("qty", POS_INT)),
                ("BAD_PRICE", _bad("unit_price", MONEY)),
                ("BAD_DISCOUNT", _bad("discount_amt", MONEY)),
                ("BAD_LINE_STATUS", _not_in("line_status", LINE_STATUSES)),
                ("BAD_SEQUENCE", _bad("sequence_number", SEQ)),
            ]
        )
    elif entity in {"payments", "cdc_payments"}:
        rules.extend(
            [
                ("BAD_PAYMENT_ID", _missing("payment_id")),
                ("BAD_ORDER_ID", _bad("order_id", ORDER_ID)),
                ("BAD_TENDER", _not_in("tender_type", TENDERS)),
                ("BAD_AMOUNT", _bad("amount", MONEY)),
                ("BAD_PAY_STATUS", _not_in("status", PAYMENT_STATUSES)),
                ("BAD_SEQUENCE", _bad("sequence_number", SEQ)),
            ]
        )
    elif entity == "returns":
        rules.extend(
            [
                ("BAD_RETURN_ID", _missing("return_id")),
                ("BAD_ORDER_ID", _bad("order_id", ORDER_ID)),
                ("BAD_LINE", _bad("line_number", POS_INT)),
                ("BAD_SKU", _bad("sku", SKU)),
                ("BAD_RETURN_QTY", _bad("qty_returned", POS_INT)),
                ("BAD_REFUND", _bad("refund_amt", MONEY)),
                ("BAD_SEQUENCE", _bad("sequence_number", SEQ)),
            ]
        )
    elif entity == "inventory_movements":
        rules.extend(
            [
                ("BAD_MOVEMENT_ID", _missing("movement_id")),
                ("BAD_SKU", _bad("sku", SKU)),
                ("BAD_STORE", _bad("store_id", STORE)),
                ("BAD_MOVEMENT_TYPE", _not_in("movement_type", MOVEMENT_TYPES)),
                ("BAD_QTY", _bad("qty", POS_INT)),
                ("BAD_SIGN", _not_in("sign", ["-1", "1"])),
                ("BAD_COST", _bad("unit_cost", MONEY)),
            ]
        )
    elif entity == "inventory_snapshots":
        rules.extend(
            [
                ("BAD_SNAPSHOT_DATE", _bad("snapshot_date", DATE)),
                ("BAD_SKU", _bad("sku", SKU)),
                ("BAD_STORE", _bad("store_id", STORE)),
                ("BAD_ON_HAND", _bad("on_hand_qty", r"^\d+$")),
                ("BAD_ON_HAND_COST", _bad("on_hand_cost", MONEY)),
            ]
        )
    elif entity == "customers":
        rules.extend(
            [
                ("BAD_CUSTOMER_ID", _bad("customer_id", CUSTOMER_ID)),
                ("BAD_EMAIL", _bad("email", EMAIL)),
                ("BAD_STATE", _bad("state", STATE)),
                ("BAD_STATUS", _not_in("status", CUSTOMER_STATUSES)),
            ]
        )
    elif entity == "cdc_customers":
        rules.extend(
            [
                ("BAD_OP", _not_in("op", OPS)),
                ("BAD_CUSTOMER_ID", _bad("customer_id", CUSTOMER_ID)),
                ("BAD_EMAIL", _bad("email", EMAIL)),
                ("BAD_STATE", _bad("state", STATE)),
                ("BAD_SEQUENCE", _bad("sequence_number", SEQ)),
            ]
        )
    elif entity == "products":
        rules.extend(
            [
                ("BAD_SKU", _bad("sku", SKU)),
                ("BAD_STATUS", _not_in("status", PRODUCT_STATUSES)),
                ("BAD_COST", _bad("unit_cost", MONEY)),
                ("BAD_PRICE", _bad("list_price", MONEY)),
            ]
        )
    elif entity == "cdc_products":
        rules.extend(
            [
                ("BAD_OP", _not_in("op", OPS)),
                ("BAD_SKU", _bad("sku", SKU)),
                ("BAD_SEQUENCE", _bad("sequence_number", SEQ)),
                ("BAD_COST", _bad("unit_cost", MONEY)),
                ("BAD_PRICE", _bad("list_price", MONEY)),
            ]
        )
    elif entity == "stores":
        rules.append(("BAD_STORE", _bad("store_id", STORE)))
    elif entity == "channels":
        rules.append(("BAD_CHANNEL", _missing("channel_id")))
    elif entity == "promotions":
        rules.append(("BAD_PROMO", _missing("promo_id")))
    return rules


def natural_key(entity: str, df: DataFrame) -> F.Column:
    columns = {
        "orders": ["order_id"],
        "cdc_orders": ["order_id"],
        "order_lines": ["order_id", "line_number"],
        "cdc_order_lines": ["order_id", "line_number"],
        "payments": ["payment_id"],
        "cdc_payments": ["payment_id"],
        "returns": ["return_id"],
        "inventory_movements": ["movement_id"],
        "inventory_snapshots": ["snapshot_date", "sku", "store_id"],
        "customers": ["customer_id"],
        "cdc_customers": ["customer_id"],
        "products": ["sku"],
        "cdc_products": ["sku"],
        "stores": ["store_id"],
        "channels": ["channel_id"],
        "promotions": ["promo_id"],
    }[entity]
    return F.concat_ws("|", *[F.coalesce(F.col(column), F.lit("")) for column in columns])


def split_rules(
    df: DataFrame,
    entity: str,
    batch_ts: str,
    run_id: str,
) -> tuple[DataFrame, DataFrame, int, int]:
    """Return (good rows, quarantine rows, seen, failed input rows)."""

    rules = rules_for(entity)
    corrupt = F.col("_corrupt_record").isNotNull()
    flagged = df.withColumn("_corrupt", corrupt)
    flag_names: list[tuple[str, str]] = []
    for rule_id, expr in rules:
        name = f"_fail_{rule_id}"
        flagged = flagged.withColumn(name, F.when(F.col("_corrupt"), F.lit(False)).otherwise(expr))
        flag_names.append((rule_id, name))

    good_expr = ~F.col("_corrupt")
    for _, name in flag_names:
        good_expr = good_expr & ~F.col(name)
    data_cols = [column for column in df.columns if column != "_corrupt_record"]
    good = flagged.filter(good_expr).select(*data_cols)

    raw = F.to_json(F.struct(*[F.col(column) for column in data_cols]))
    key = natural_key(entity, flagged)
    pieces = []
    corrupt_rows = flagged.filter(F.col("_corrupt")).select(
        F.sha2(
            F.concat_ws("||", F.lit(entity), F.lit(batch_ts), F.lit("CORRUPT_ROW"), key, raw), 256
        ).alias("quarantine_id"),
        F.lit(entity).alias("entity"),
        F.lit(batch_ts).alias("batch_ts"),
        F.lit("CORRUPT_ROW").alias("rule_id"),
        key.alias("natural_key"),
        raw.alias("raw_json"),
        F.lit(run_id).alias("run_id"),
    )
    pieces.append(corrupt_rows)
    for rule_id, name in flag_names:
        part = flagged.filter(F.col(name)).select(
            F.sha2(
                F.concat_ws("||", F.lit(entity), F.lit(batch_ts), F.lit(rule_id), key, raw),
                256,
            ).alias("quarantine_id"),
            F.lit(entity).alias("entity"),
            F.lit(batch_ts).alias("batch_ts"),
            F.lit(rule_id).alias("rule_id"),
            key.alias("natural_key"),
            raw.alias("raw_json"),
            F.lit(run_id).alias("run_id"),
        )
        pieces.append(part)
    quarantine = pieces[0]
    for part in pieces[1:]:
        quarantine = quarantine.unionByName(part)

    seen = df.count()
    failed = flagged.filter(~good_expr).count()
    return good, quarantine, seen, failed


def extra_quarantine(
    rows: DataFrame,
    entity: str,
    batch_ts: str,
    run_id: str,
    rule_id: str,
    key_columns: list[str],
) -> DataFrame:
    raw = F.to_json(F.struct(*[F.col(column) for column in rows.columns]))
    key = F.concat_ws(
        "|", *[F.coalesce(F.col(column).cast("string"), F.lit("")) for column in key_columns]
    )
    return rows.select(
        F.sha2(
            F.concat_ws("||", F.lit(entity), F.lit(batch_ts), F.lit(rule_id), key, raw), 256
        ).alias("quarantine_id"),
        F.lit(entity).alias("entity"),
        F.lit(batch_ts).alias("batch_ts"),
        F.lit(rule_id).alias("rule_id"),
        key.alias("natural_key"),
        raw.alias("raw_json"),
        F.lit(run_id).alias("run_id"),
    )
