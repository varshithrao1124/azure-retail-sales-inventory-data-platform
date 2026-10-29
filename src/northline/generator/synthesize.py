"""Seeded multi-batch retail generator.

Every scale includes the same planted cases (cancel, partial return, late
customer, late sku, SCD2 price and category, snapshot variance, bad rows).
Tiny and small cover 2024-11-08 through 2024-11-10. Medium prepends extra
days so the same edge cases sit on a longer calendar.
"""

from __future__ import annotations

import csv
import json
import shutil
from collections import defaultdict
from copy import deepcopy
from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import numpy as np
from faker import Faker

from northline.generator import catalog as C
from northline.measures import compute_variances
from northline.schemas import COLUMNS

TWOPL = Decimal("0.01")
EXPLICIT_ORDERS = {
    C.CANCELLED_ORDER,
    C.PARTIAL_ORDER,
    C.DUP_ORDER,
    C.TIER_ORDER,
    C.PRICE_ORDER,
    C.LATE_ORDER,
    C.GHOST_ORDER,
    C.CAT_FRI_ORDER,
    C.CAT_SUN_ORDER,
    C.EMP_ORDER,
}


def _money(value: Decimal) -> str:
    return format(value.quantize(TWOPL, rounding=ROUND_HALF_UP), "f")


def _tax(merch: Decimal, state: str) -> Decimal:
    return (merch * C.TAX[state]).quantize(TWOPL, rounding=ROUND_HALF_UP)


def _parse_product(row: tuple) -> dict:
    sku, name, category, subcategory, brand, cost, price, status = row
    return {
        "sku": sku,
        "product_name": name,
        "category": category,
        "subcategory": subcategory,
        "brand": brand,
        "unit_cost": Decimal(cost),
        "list_price": Decimal(price),
        "status": status,
    }


class _Seq:
    def __init__(self) -> None:
        self._n = 1000

    def next(self) -> int:
        self._n += 1
        return self._n


class _Ledger:
    def __init__(self) -> None:
        self.on_hand: dict[tuple[str, str], int] = {}
        self.cost: dict[str, Decimal] = {}
        self.clean: list[dict] = []

    def snapshot_rows(self, snapshot_date: str, extract_ts: str) -> list[dict]:
        rows = []
        for (sku, store_id), qty in sorted(self.on_hand.items()):
            rows.append(
                {
                    "snapshot_date": snapshot_date,
                    "sku": sku,
                    "store_id": store_id,
                    "on_hand_qty": str(qty),
                    "on_hand_cost": _money(Decimal(qty) * self.cost[sku]),
                    "extract_ts": extract_ts,
                }
            )
        return rows

    def move(
        self,
        *,
        movement_id: str,
        sku: str,
        store_id: str,
        movement_type: str,
        qty: int,
        sign: int,
        movement_ts: str,
        reference_id: str,
        reason_code: str,
        extract_ts: str,
        track: bool = True,
    ) -> dict:
        delta = qty * sign
        key = (sku, store_id)
        current = self.on_hand.get(key, 0)
        if track and current + delta < 0:
            raise RuntimeError(
                f"{movement_id} would drive {sku}/{store_id} from {current} by {delta}"
            )
        if track:
            self.on_hand[key] = current + delta
        row = {
            "movement_id": movement_id,
            "sku": sku,
            "store_id": store_id,
            "movement_type": movement_type,
            "qty": str(qty),
            "sign": str(sign),
            "unit_cost": _money(self.cost[sku]),
            "movement_ts": movement_ts,
            "reference_id": reference_id,
            "reason_code": reason_code,
            "extract_ts": extract_ts,
        }
        if track:
            self.clean.append(row)
        return row


def _calendar(scale: str) -> list[date]:
    end = date(2024, 11, 10)
    days = C.SCALE[scale]["days"]
    start = end - timedelta(days=days - 1)
    found = []
    cursor = start
    while cursor <= end:
        found.append(cursor)
        cursor += timedelta(days=1)
    return found


def _dow_mult(day: date) -> float:
    return {4: 1.0, 5: 1.75, 6: 1.35}.get(day.weekday(), 0.85)


def generate(scale: str = "small", output: str | Path = "data/sample", seed: int = 42) -> dict:
    if scale not in C.SCALE:
        raise ValueError(f"scale must be one of {sorted(C.SCALE)}")
    cfg = C.SCALE[scale]
    rng = np.random.default_rng(seed)
    fake = Faker("en_US")
    Faker.seed(seed)
    out = Path(output)
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)

    stores = []
    for raw in C.STORES:
        if raw[0] not in cfg["store_ids"]:
            continue
        stores.append(
            {
                "store_id": raw[0],
                "store_name": raw[1],
                "store_type": raw[2],
                "region": raw[3],
                "state": raw[4],
                "city": raw[5],
                "square_feet": str(raw[6]),
                "open_date": raw[7],
            }
        )
    store_by_id = {row["store_id"]: row for row in stores}
    retail_ids = [row["store_id"] for row in stores if row["store_type"] == "RETAIL"]

    products: dict[str, dict] = {}
    for raw in C.EDGE_PRODUCTS:
        item = _parse_product(raw)
        products[item["sku"]] = item
    categories = [
        ("Shelter", "Tents"),
        ("Apparel", "Shirts"),
        ("Cook", "Stoves"),
        ("Light", "Lamps"),
    ]
    for index in range(cfg["extra_skus"]):
        category, subcategory = categories[index % len(categories)]
        price = Decimal(20 + (index * 7) % 90)
        products[f"SKU-EXT-{index + 1:04d}"] = {
            "sku": f"SKU-EXT-{index + 1:04d}",
            "product_name": f"Field Item {index + 1}",
            "category": category,
            "subcategory": subcategory,
            "brand": "Northline",
            "unit_cost": (price * Decimal("0.42")).quantize(TWOPL, rounding=ROUND_HALF_UP),
            "list_price": price,
            "status": "ACTIVE",
        }
    late_product = _parse_product(C.LATE_PRODUCT)
    all_products = dict(products)
    all_products[late_product["sku"]] = late_product

    ledger = _Ledger()
    for sku, item in all_products.items():
        ledger.cost[sku] = item["unit_cost"]
        for store in stores:
            if sku == C.LATE_SKU and store["store_id"] != C.DC_STORE:
                continue
            if sku == C.STOCKOUT_SKU and store["store_id"] == C.STOCKOUT_STORE:
                qty = 0
            elif store["store_id"] == C.DC_STORE:
                qty = 80
            elif store["store_type"] == "MARKETPLACE_NODE":
                qty = 30
            elif sku == C.VARIANCE_SKU and store["store_id"] == C.VARIANCE_STORE:
                qty = 36
            else:
                qty = 28
            ledger.on_hand[(sku, store["store_id"])] = qty

    customer_ids: list[str] = []
    cursor = 1
    while len(customer_ids) < cfg["customers"]:
        cid = f"C{cursor:06d}"
        if cid not in {C.GHOST_CUSTOMER, C.LATE_CUSTOMER}:
            customer_ids.append(cid)
        cursor += 1
    customers: dict[str, dict] = {}
    for cid in customer_ids:
        home = retail_ids[int(rng.integers(0, len(retail_ids)))]
        home_store = store_by_id[home]
        customers[cid] = {
            "customer_id": cid,
            "full_name": fake.name(),
            "email": f"{cid.lower()}@northline.example",
            "segment": str(rng.choice(["LOYALTY", "OCCASIONAL", "NEW", "EMPLOYEE"])),
            "loyalty_tier": str(rng.choice(["NONE", "SILVER", "GOLD", "TRAIL"])),
            "home_store_id": home,
            "city": home_store["city"],
            "state": home_store["state"],
            "postal_code": f"{int(rng.integers(10000, 99999))}",
            "status": "ACTIVE",
            "created_date": str(date(2019, 1, 1) + timedelta(days=int(rng.integers(0, 1800)))),
        }
    customers[C.TIER_CUSTOMER].update(
        {
            "full_name": "Jordan Hale",
            "segment": "LOYALTY",
            "loyalty_tier": "SILVER",
            "home_store_id": "STR01",
            "city": "Seattle",
            "state": "WA",
            "postal_code": "98107",
            "email": "jordan.hale@northline.example",
        }
    )
    customers[C.CANCEL_CUSTOMER].update(
        {
            "full_name": "Avery Cho",
            "segment": "OCCASIONAL",
            "loyalty_tier": "NONE",
            "home_store_id": "STR01",
            "city": "Seattle",
            "state": "WA",
            "postal_code": "98103",
        }
    )
    customers[C.DELETE_CUSTOMER].update(
        {
            "full_name": "Parker Nguyen",
            "segment": "EMPLOYEE",
            "loyalty_tier": "SILVER",
            "home_store_id": "STR02",
            "city": "Portland",
            "state": "OR",
            "postal_code": "97214",
        }
    )
    if C.EMAIL_CUSTOMER in customers:
        customers[C.EMAIL_CUSTOMER].update(
            {
                "full_name": "Samir Cole",
                "email": "samir.cole@northline.example",
                "loyalty_tier": "NONE",
            }
        )
    if "C000004" in customers:
        customers["C000004"]["loyalty_tier"] = "NONE"

    late_customer = {
        "customer_id": C.LATE_CUSTOMER,
        "full_name": "Rowan Ellis",
        "email": "rowan.ellis@northline.example",
        "segment": "NEW",
        "loyalty_tier": "TRAIL",
        "home_store_id": "STR01",
        "city": "Tacoma",
        "state": "WA",
        "postal_code": "98402",
        "status": "ACTIVE",
        "created_date": "2024-11-07",
    }

    seq = _Seq()
    movement_n = 0
    order_n = 0
    payment_n = 0
    return_n = 0
    remembered: dict[str, dict] = {}
    lines: dict[tuple[str, int], dict] = {}
    payments: dict[str, dict] = {}
    returnable: list[dict] = []
    written_snapshots: list[dict] = []
    insert_images: dict[str, dict] = {}

    def next_movement_id() -> str:
        nonlocal movement_n
        movement_n += 1
        return f"M{movement_n:08d}"

    def blank() -> dict[str, list]:
        return defaultdict(list)

    def discount_for(order_ts: str, product: dict, qty: int) -> tuple[Decimal, str]:
        if order_ts[:10] >= "2024-11-09" and product["category"] == "Shelter":
            gross = Decimal(qty) * product["list_price"]
            return (gross * Decimal("0.15")).quantize(TWOPL, rounding=ROUND_HALF_UP), C.PROMO_ID
        return Decimal("0.00"), ""

    def ensure(
        sku: str, store_id: str, qty: int, movement_ts: str, extract_ts: str, into: list
    ) -> None:
        if sku == C.STOCKOUT_SKU and store_id == C.STOCKOUT_STORE:
            return
        have = ledger.on_hand.get((sku, store_id), 0)
        if have >= qty:
            return
        need = max(20, qty - have)
        into.append(
            ledger.move(
                movement_id=next_movement_id(),
                sku=sku,
                store_id=store_id,
                movement_type="RECEIPT",
                qty=need,
                sign=1,
                movement_ts=movement_ts,
                reference_id=f"PO-{sku}-{store_id}",
                reason_code="REPLENISH",
                extract_ts=extract_ts,
            )
        )

    def add_order(
        bucket: dict[str, list],
        *,
        order_id: str,
        order_ts: str,
        customer_id: str,
        store_id: str,
        channel_id: str,
        line_specs: list[tuple[str, int]],
        status: str,
        extract_ts: str,
        fulfill: bool,
        movement_ts: str | None,
        explicit: bool,
    ) -> dict:
        store = store_by_id[store_id]
        line_rows = []
        merch = Decimal("0.00")
        tax = Decimal("0.00")
        promo_id = ""
        for number, (sku, qty) in enumerate(line_specs, start=1):
            product = all_products[sku]
            if fulfill:
                ensure(
                    sku,
                    store_id,
                    qty,
                    movement_ts or order_ts,
                    extract_ts,
                    bucket["inventory_movements"],
                )
                have = ledger.on_hand.get((sku, store_id), 0)
                if have < qty:
                    raise RuntimeError(f"Not enough stock for {order_id} {sku} at {store_id}")
            discount, line_promo = discount_for(order_ts, product, qty)
            if line_promo:
                promo_id = line_promo
            gross = (Decimal(qty) * product["list_price"] - discount).quantize(
                TWOPL, rounding=ROUND_HALF_UP
            )
            line_tax = _tax(gross, store["state"])
            merch += gross
            tax += line_tax
            line_seq = seq.next()
            line_row = {
                "order_id": order_id,
                "line_number": str(number),
                "sku": sku,
                "qty": str(qty),
                "unit_price": _money(product["list_price"]),
                "discount_amt": _money(discount),
                "tax_amt": _money(line_tax),
                "line_status": "OPEN" if status == "OPEN" else "SHIPPED",
                "sequence_number": str(line_seq),
                "extract_ts": extract_ts,
            }
            line_rows.append(line_row)
            lines[(order_id, number)] = line_row
            if fulfill:
                bucket["inventory_movements"].append(
                    ledger.move(
                        movement_id=next_movement_id(),
                        sku=sku,
                        store_id=store_id,
                        movement_type="SALE",
                        qty=qty,
                        sign=-1,
                        movement_ts=movement_ts or order_ts,
                        reference_id=f"{order_id}-{number}",
                        reason_code="SALE",
                        extract_ts=extract_ts,
                    )
                )
            if explicit:
                pass
            else:
                returnable.append(
                    {
                        "order_id": order_id,
                        "line_number": number,
                        "sku": sku,
                        "store_id": store_id,
                        "qty": qty,
                        "returned": 0,
                        "unit_price": product["list_price"],
                        "discount": discount,
                        "state": store["state"],
                        "order_ts": order_ts,
                    }
                )
        order_seq = seq.next()
        order_row = {
            "order_id": order_id,
            "order_ts": order_ts,
            "customer_id": customer_id,
            "store_id": store_id,
            "channel_id": channel_id,
            "promo_id": promo_id,
            "status": status,
            "sequence_number": str(order_seq),
            "extract_ts": extract_ts,
        }
        bucket["orders"].append(order_row)
        bucket["order_lines"].extend(line_rows)
        remembered[order_id] = order_row
        insert_images[order_id] = deepcopy(order_row)
        total = merch + tax
        pay_id = f"P{order_ts[:10].replace('-', '')}{payment_n_next():05d}"
        pay = {
            "payment_id": pay_id,
            "order_id": order_id,
            "tender_type": "CARD",
            "amount": _money(total),
            "status": "CAPTURED",
            "paid_ts": order_ts,
            "sequence_number": str(seq.next()),
            "extract_ts": extract_ts,
        }
        payments[pay_id] = pay
        bucket["payments"].append(pay)
        remembered[order_id]["_payment_id"] = pay_id
        return order_row

    def payment_n_next() -> int:
        nonlocal payment_n
        payment_n += 1
        return payment_n

    def add_return(
        bucket: dict[str, list],
        *,
        return_id: str,
        order_id: str,
        line_number: int,
        qty: int,
        extract_ts: str,
        return_ts: str,
        reason: str,
    ) -> None:
        line = lines[(order_id, line_number)]
        ordered = int(line["qty"])
        if qty <= 0 or qty > ordered:
            raise RuntimeError(f"bad scripted return {return_id}")
        unit = Decimal(line["unit_price"])
        discount = Decimal(line["discount_amt"])
        alloc = (discount * Decimal(qty) / Decimal(ordered)).quantize(TWOPL, rounding=ROUND_HALF_UP)
        refund = (unit * Decimal(qty) - alloc).quantize(TWOPL, rounding=ROUND_HALF_UP)
        store = store_by_id[remembered[order_id]["store_id"]]
        bucket["returns"].append(
            {
                "return_id": return_id,
                "order_id": order_id,
                "line_number": str(line_number),
                "sku": line["sku"],
                "qty_returned": str(qty),
                "refund_amt": _money(refund),
                "reason_code": reason,
                "return_ts": return_ts,
                "sequence_number": str(seq.next()),
                "extract_ts": extract_ts,
            }
        )
        bucket["inventory_movements"].append(
            ledger.move(
                movement_id=next_movement_id(),
                sku=line["sku"],
                store_id=store["store_id"],
                movement_type="RETURN",
                qty=qty,
                sign=1,
                movement_ts=return_ts,
                reference_id=return_id,
                reason_code=reason,
                extract_ts=extract_ts,
            )
        )
        refund_tax = _tax(refund, store["state"])
        nonlocal_pay = payment_n_next()
        bucket["payments"].append(
            {
                "payment_id": f"P-RF-{nonlocal_pay:05d}",
                "order_id": order_id,
                "tender_type": "CARD",
                "amount": _money(refund + refund_tax),
                "status": "REFUNDED",
                "paid_ts": return_ts,
                "sequence_number": str(seq.next()),
                "extract_ts": extract_ts,
            }
        )

    def cdc_customer(
        cid: str, changes: dict, op: str, change_ts: str, extract_ts: str, apply: bool
    ) -> dict:
        image = deepcopy(customers[cid])
        image.update(changes)
        if apply:
            customers[cid].update(changes)
        row = {
            key: image[key]
            for key in (
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
            )
        }
        row.update(
            {
                "sequence_number": str(seq.next()),
                "op": op,
                "change_ts": change_ts,
                "extract_ts": extract_ts,
            }
        )
        return row

    def cdc_product(
        sku: str, changes: dict, op: str, change_ts: str, extract_ts: str, apply: bool
    ) -> dict:
        image = deepcopy(all_products[sku])
        image.update(changes)
        if apply:
            all_products[sku].update(changes)
            if sku in products:
                products[sku].update(changes)
            if "unit_cost" in changes:
                ledger.cost[sku] = changes["unit_cost"]
        row = {
            "sequence_number": str(seq.next()),
            "op": op,
            "change_ts": change_ts,
            "sku": sku,
            "product_name": image["product_name"],
            "category": image["category"],
            "subcategory": image["subcategory"],
            "brand": image["brand"],
            "unit_cost": _money(image["unit_cost"]),
            "list_price": _money(image["list_price"]),
            "status": image["status"],
            "extract_ts": extract_ts,
        }
        return row

    def cdc_from_order(
        order_id: str, status: str, change_ts: str, extract_ts: str, sequence: int | None = None
    ) -> dict:
        image = deepcopy(remembered[order_id])
        image["status"] = status
        image["sequence_number"] = str(sequence if sequence is not None else seq.next())
        image["op"] = "U"
        image["change_ts"] = change_ts
        image["extract_ts"] = extract_ts
        if sequence is None:
            remembered[order_id]["status"] = status
            remembered[order_id]["sequence_number"] = image["sequence_number"]
        return {
            "sequence_number": image["sequence_number"],
            "op": "U",
            "change_ts": change_ts,
            "order_id": image["order_id"],
            "order_ts": image["order_ts"],
            "customer_id": image["customer_id"],
            "store_id": image["store_id"],
            "channel_id": image["channel_id"],
            "promo_id": image["promo_id"],
            "status": status,
            "extract_ts": extract_ts,
        }

    days = _calendar(scale)
    batches: dict[str, dict[str, list]] = {}
    for index, day in enumerate(days):
        am = blank()
        pm = blank()
        extract_am = f"{day.isoformat()}T06:00:00Z"
        extract_pm = f"{day.isoformat()}T18:00:00Z"
        if index == 0:
            opening = (day - timedelta(days=1)).isoformat()
            for cid in customer_ids:
                row = dict(customers[cid])
                row["extract_ts"] = extract_am
                am["customers"].append(row)
            for sku in sorted(products):
                item = products[sku]
                am["products"].append(
                    {
                        "sku": item["sku"],
                        "product_name": item["product_name"],
                        "category": item["category"],
                        "subcategory": item["subcategory"],
                        "brand": item["brand"],
                        "unit_cost": _money(item["unit_cost"]),
                        "list_price": _money(item["list_price"]),
                        "status": item["status"],
                        "extract_ts": extract_am,
                    }
                )
            for store in stores:
                row = dict(store)
                row["extract_ts"] = extract_am
                am["stores"].append(row)
            for channel_id, channel_name in C.CHANNELS:
                am["channels"].append(
                    {
                        "channel_id": channel_id,
                        "channel_name": channel_name,
                        "extract_ts": extract_am,
                    }
                )
            am["promotions"].append(
                {
                    "promo_id": C.PROMO_ID,
                    "promo_name": "Trail Week 15",
                    "applies_to_category": "Shelter",
                    "discount_pct": "0.15",
                    "start_date": "2024-11-09",
                    "end_date": "2024-11-10",
                    "extract_ts": extract_am,
                }
            )
            opening_rows = ledger.snapshot_rows(opening, extract_am)
            am["inventory_snapshots"].extend(opening_rows)
            written_snapshots.extend(opening_rows)
        else:
            prev = (day - timedelta(days=1)).isoformat()
            rows = ledger.snapshot_rows(prev, extract_am)
            if prev == C.VARIANCE_DATE:
                for row in rows:
                    if row["sku"] == C.VARIANCE_SKU and row["store_id"] == C.VARIANCE_STORE:
                        qty = int(row["on_hand_qty"])
                        if qty < 1:
                            raise RuntimeError("variance plant has no units on hand")
                        row["on_hand_qty"] = str(qty - 1)
                        row["on_hand_cost"] = _money(Decimal(qty - 1) * ledger.cost[row["sku"]])
            am["inventory_snapshots"].extend(rows)
            written_snapshots.extend(rows)

        if day == date(2024, 11, 9):
            am["cdc_customers"].append(
                cdc_customer(
                    C.TIER_CUSTOMER,
                    {"loyalty_tier": "GOLD"},
                    "U",
                    "2024-11-09T06:00:00Z",
                    extract_am,
                    True,
                )
            )
            if "C000004" in customers:
                am["cdc_customers"].append(
                    cdc_customer(
                        "C000004",
                        {"loyalty_tier": "SILVER"},
                        "U",
                        "2024-11-09T06:05:00Z",
                        extract_am,
                        True,
                    )
                )
            am["cdc_products"].append(
                cdc_product(
                    C.PRICE_SKU,
                    {"list_price": Decimal("249.00"), "unit_cost": Decimal("104.00")},
                    "U",
                    "2024-11-09T06:00:00Z",
                    extract_am,
                    True,
                )
            )
            am["cdc_products"].append(
                cdc_product(
                    C.CATEGORY_SKU,
                    {"category": "Outlet", "subcategory": "Clearance"},
                    "U",
                    "2024-11-09T06:00:00Z",
                    extract_am,
                    True,
                )
            )

        if day == date(2024, 11, 10):
            am["cdc_orders"].append(
                cdc_from_order(C.CANCELLED_ORDER, "CANCELLED", "2024-11-10T05:15:00Z", extract_am)
            )
            for (order_id, number), line in list(lines.items()):
                if order_id != C.CANCELLED_ORDER:
                    continue
                am["cdc_order_lines"].append(
                    {
                        "sequence_number": str(seq.next()),
                        "op": "U",
                        "change_ts": "2024-11-10T05:15:00Z",
                        "order_id": order_id,
                        "line_number": line["line_number"],
                        "sku": line["sku"],
                        "qty": line["qty"],
                        "unit_price": line["unit_price"],
                        "discount_amt": line["discount_amt"],
                        "tax_amt": line["tax_amt"],
                        "line_status": "CANCELLED",
                        "extract_ts": extract_am,
                    }
                )
            pay = payments[remembered[C.CANCELLED_ORDER]["_payment_id"]]
            am["cdc_payments"].append(
                {
                    "sequence_number": str(seq.next()),
                    "op": "U",
                    "change_ts": "2024-11-10T05:16:00Z",
                    "payment_id": pay["payment_id"],
                    "order_id": pay["order_id"],
                    "tender_type": pay["tender_type"],
                    "amount": pay["amount"],
                    "status": "VOIDED",
                    "paid_ts": pay["paid_ts"],
                    "extract_ts": extract_am,
                }
            )
            if C.EMAIL_CUSTOMER in customers:
                am["cdc_customers"].append(
                    cdc_customer(
                        C.EMAIL_CUSTOMER,
                        {"email": "not-an-email"},
                        "U",
                        "2024-11-10T05:10:00Z",
                        extract_am,
                        False,
                    )
                )
            am["inventory_movements"].append(
                ledger.move(
                    movement_id=next_movement_id(),
                    sku=C.LATE_SKU,
                    store_id=C.DC_STORE,
                    movement_type="RECEIPT",
                    qty=5,
                    sign=1,
                    movement_ts="2024-11-10T05:00:00Z",
                    reference_id="PO-LATE-MUG",
                    reason_code="LATE_RECEIPT",
                    extract_ts=extract_am,
                )
            )
            add_order(
                am,
                order_id=C.LATE_ORDER,
                order_ts="2024-11-08T21:15:00Z",
                customer_id=C.LATE_CUSTOMER,
                store_id=C.DC_STORE,
                channel_id="WEB",
                line_specs=[(C.LATE_SKU, 1)],
                status="SHIPPED",
                extract_ts=extract_am,
                fulfill=True,
                movement_ts="2024-11-10T05:20:00Z",
                explicit=True,
            )
            add_order(
                am,
                order_id=C.GHOST_ORDER,
                order_ts="2024-11-08T21:45:00Z",
                customer_id=C.GHOST_CUSTOMER,
                store_id="STR01",
                channel_id="RETAIL",
                line_specs=[(C.KETTLE_SKU, 1)],
                status="SHIPPED",
                extract_ts=extract_am,
                fulfill=True,
                movement_ts="2024-11-10T05:30:00Z",
                explicit=True,
            )
            am["orders"].append(
                {
                    "order_id": "BAD",
                    "order_ts": "not-a-timestamp",
                    "customer_id": "CX",
                    "store_id": "NO",
                    "channel_id": "FAX",
                    "promo_id": "NOPE",
                    "status": "MAYBE",
                    "sequence_number": "xyz",
                    "extract_ts": extract_am,
                }
            )
            am["order_lines"].append(
                {
                    "order_id": "O-BAD-0001",
                    "line_number": "1",
                    "sku": "NOTASKU",
                    "qty": "abc",
                    "unit_price": "free",
                    "discount_amt": "-1",
                    "tax_amt": "0",
                    "line_status": "NOPE",
                    "sequence_number": "1",
                    "extract_ts": extract_am,
                }
            )
            am["payments"].append(
                {
                    "payment_id": "P-BAD-AMT01",
                    "order_id": C.PARTIAL_ORDER,
                    "tender_type": "CARD",
                    "amount": "-5.00",
                    "status": "CAPTURED",
                    "paid_ts": "2024-11-10T05:00:00Z",
                    "sequence_number": str(seq.next()),
                    "extract_ts": extract_am,
                }
            )
            am["payments"].append(
                {
                    "payment_id": "P-BAD-ORPHAN",
                    "order_id": "O-NONE-0001",
                    "tender_type": "CARD",
                    "amount": "10.00",
                    "status": "CAPTURED",
                    "paid_ts": "2024-11-10T05:01:00Z",
                    "sequence_number": str(seq.next()),
                    "extract_ts": extract_am,
                }
            )
            am["returns"].append(
                {
                    "return_id": "R-ZERO-0001",
                    "order_id": C.PARTIAL_ORDER,
                    "line_number": "1",
                    "sku": C.PRICE_SKU,
                    "qty_returned": "0",
                    "refund_amt": "0.00",
                    "reason_code": "SIZE",
                    "return_ts": "2024-11-10T05:02:00Z",
                    "sequence_number": str(seq.next()),
                    "extract_ts": extract_am,
                }
            )
            am["returns"].append(
                {
                    "return_id": "R-OVER-0001",
                    "order_id": C.PARTIAL_ORDER,
                    "line_number": "1",
                    "sku": C.PRICE_SKU,
                    "qty_returned": "99",
                    "refund_amt": "99.00",
                    "reason_code": "SIZE",
                    "return_ts": "2024-11-10T05:03:00Z",
                    "sequence_number": str(seq.next()),
                    "extract_ts": extract_am,
                }
            )
            am["inventory_movements"].append(
                {
                    "movement_id": C.NEGATIVE_MOVEMENT,
                    "sku": C.STOCKOUT_SKU,
                    "store_id": C.STOCKOUT_STORE,
                    "movement_type": "SHRINK",
                    "qty": "1",
                    "sign": "-1",
                    "unit_cost": _money(ledger.cost[C.STOCKOUT_SKU]),
                    "movement_ts": "2024-11-10T05:50:00Z",
                    "reference_id": "",
                    "reason_code": "BAD_COUNT",
                    "extract_ts": extract_am,
                }
            )
            am["inventory_movements"].append(
                {
                    "movement_id": C.BAD_TYPE_MOVEMENT,
                    "sku": C.KETTLE_SKU,
                    "store_id": "STR01",
                    "movement_type": "THEFT",
                    "qty": "1",
                    "sign": "-1",
                    "unit_cost": _money(ledger.cost[C.KETTLE_SKU]),
                    "movement_ts": "2024-11-10T05:51:00Z",
                    "reference_id": "",
                    "reason_code": "UNKNOWN",
                    "extract_ts": extract_am,
                }
            )

        if day == date(2024, 11, 8):
            pm["inventory_movements"].append(
                ledger.move(
                    movement_id=C.REPLAY_MOVEMENT,
                    sku=C.PRICE_SKU,
                    store_id=C.DC_STORE,
                    movement_type="RECEIPT",
                    qty=12,
                    sign=1,
                    movement_ts="2024-11-08T15:05:00Z",
                    reference_id="PO-RIDGE-DC",
                    reason_code="PURCHASE",
                    extract_ts=extract_pm,
                )
            )
            ensure(
                C.KETTLE_SKU,
                C.DC_STORE,
                6,
                "2024-11-08T15:10:00Z",
                extract_pm,
                pm["inventory_movements"],
            )
            pm["inventory_movements"].append(
                ledger.move(
                    movement_id=next_movement_id(),
                    sku=C.KETTLE_SKU,
                    store_id=C.DC_STORE,
                    movement_type="TRANSFER_OUT",
                    qty=6,
                    sign=-1,
                    movement_ts="2024-11-08T15:20:00Z",
                    reference_id="XFER-0001",
                    reason_code="TRANSFER",
                    extract_ts=extract_pm,
                )
            )
            pm["inventory_movements"].append(
                ledger.move(
                    movement_id=next_movement_id(),
                    sku=C.KETTLE_SKU,
                    store_id="STR01",
                    movement_type="TRANSFER_IN",
                    qty=6,
                    sign=1,
                    movement_ts="2024-11-08T15:20:00Z",
                    reference_id="XFER-0001",
                    reason_code="TRANSFER",
                    extract_ts=extract_pm,
                )
            )
            pm["inventory_movements"].append(
                ledger.move(
                    movement_id=next_movement_id(),
                    sku=C.KETTLE_SKU,
                    store_id="STR02",
                    movement_type="ADJUSTMENT",
                    qty=2,
                    sign=1,
                    movement_ts="2024-11-08T15:30:00Z",
                    reference_id="CNT-0001",
                    reason_code="CYCLE_COUNT",
                    extract_ts=extract_pm,
                )
            )
            add_order(
                pm,
                order_id=C.PARTIAL_ORDER,
                order_ts="2024-11-08T16:05:00Z",
                customer_id=C.TIER_CUSTOMER,
                store_id="STR01",
                channel_id="RETAIL",
                line_specs=[(C.PRICE_SKU, 4)],
                status="SHIPPED",
                extract_ts=extract_pm,
                fulfill=True,
                movement_ts=None,
                explicit=True,
            )
            add_order(
                pm,
                order_id=C.CAT_FRI_ORDER,
                order_ts="2024-11-08T17:15:00Z",
                customer_id=C.CANCEL_CUSTOMER,
                store_id="STR01",
                channel_id="RETAIL",
                line_specs=[(C.CATEGORY_SKU, 1)],
                status="SHIPPED",
                extract_ts=extract_pm,
                fulfill=True,
                movement_ts=None,
                explicit=True,
            )
            add_order(
                pm,
                order_id=C.EMP_ORDER,
                order_ts="2024-11-08T18:40:00Z",
                customer_id=C.DELETE_CUSTOMER,
                store_id="STR02",
                channel_id="RETAIL",
                line_specs=[(C.KETTLE_SKU, 1)],
                status="SHIPPED",
                extract_ts=extract_pm,
                fulfill=True,
                movement_ts=None,
                explicit=True,
            )
            add_order(
                pm,
                order_id=C.DUP_ORDER,
                order_ts="2024-11-08T19:05:00Z",
                customer_id=C.CANCEL_CUSTOMER,
                store_id="STR02",
                channel_id="RETAIL",
                line_specs=[(C.KETTLE_SKU, 1)],
                status="SHIPPED",
                extract_ts=extract_pm,
                fulfill=True,
                movement_ts=None,
                explicit=True,
            )
            pm["orders"].append(deepcopy(pm["orders"][-1]))
            pm["order_lines"].append(deepcopy(pm["order_lines"][-1]))

        if day == date(2024, 11, 9):
            add_order(
                pm,
                order_id=C.CANCELLED_ORDER,
                order_ts="2024-11-09T16:10:00Z",
                customer_id=C.CANCEL_CUSTOMER,
                store_id="STR01",
                channel_id="RETAIL",
                line_specs=[(C.KETTLE_SKU, 1)],
                status="OPEN",
                extract_ts=extract_pm,
                fulfill=False,
                movement_ts=None,
                explicit=True,
            )
            add_order(
                pm,
                order_id=C.TIER_ORDER,
                order_ts="2024-11-09T17:25:00Z",
                customer_id=C.TIER_CUSTOMER,
                store_id="STR01",
                channel_id="WEB",
                line_specs=[(C.KETTLE_SKU, 2)],
                status="SHIPPED",
                extract_ts=extract_pm,
                fulfill=True,
                movement_ts=None,
                explicit=True,
            )
            add_order(
                pm,
                order_id=C.PRICE_ORDER,
                order_ts="2024-11-09T18:05:00Z",
                customer_id=C.TIER_CUSTOMER,
                store_id="STR01",
                channel_id="RETAIL",
                line_specs=[(C.PRICE_SKU, 1)],
                status="SHIPPED",
                extract_ts=extract_pm,
                fulfill=True,
                movement_ts=None,
                explicit=True,
            )
            add_return(
                pm,
                return_id="R-PART-0001",
                order_id=C.PARTIAL_ORDER,
                line_number=1,
                qty=1,
                extract_ts=extract_pm,
                return_ts="2024-11-09T18:30:00Z",
                reason="CHANGED_MIND",
            )
            pm["cdc_orders"].append(
                cdc_from_order(
                    C.PARTIAL_ORDER, "PARTIALLY_RETURNED", "2024-11-09T18:31:00Z", extract_pm
                )
            )
            ensure(
                C.DISCONTINUE_SKU,
                "STR01",
                2,
                "2024-11-09T18:40:00Z",
                extract_pm,
                pm["inventory_movements"],
            )
            pm["inventory_movements"].append(
                ledger.move(
                    movement_id=next_movement_id(),
                    sku=C.DISCONTINUE_SKU,
                    store_id="STR01",
                    movement_type="SHRINK",
                    qty=1,
                    sign=-1,
                    movement_ts="2024-11-09T18:45:00Z",
                    reference_id="SHRINK-0001",
                    reason_code="SHRINK",
                    extract_ts=extract_pm,
                )
            )

        if day == date(2024, 11, 10):
            add_order(
                pm,
                order_id=C.CAT_SUN_ORDER,
                order_ts="2024-11-10T16:20:00Z",
                customer_id=C.CANCEL_CUSTOMER,
                store_id="STR01",
                channel_id="WEB",
                line_specs=[(C.CATEGORY_SKU, 1)],
                status="SHIPPED",
                extract_ts=extract_pm,
                fulfill=True,
                movement_ts=None,
                explicit=True,
            )

        # Random demand for this calendar day, evening extract.
        target = max(0, int(rng.poisson(cfg["orders_per_day"] * _dow_mult(day))))
        master_skus = [sku for sku, item in products.items() if item["status"] == "ACTIVE"]
        snapshot_customers = [cid for cid in customer_ids if customers[cid]["status"] == "ACTIVE"]
        for _ in range(target):
            roll = float(rng.random())
            has_mkt = "MKT01" in store_by_id
            if roll < 0.62:
                channel_id = "RETAIL"
                store_id = retail_ids[int(rng.integers(0, len(retail_ids)))]
            elif roll < 0.90 or not has_mkt:
                channel_id = "WEB"
                store_id = C.DC_STORE
            else:
                channel_id = "MARKETPLACE"
                store_id = "MKT01"
            customer_id = snapshot_customers[int(rng.integers(0, len(snapshot_customers)))]
            line_count = 1 if float(rng.random()) < 0.72 else 2
            specs: list[tuple[str, int]] = []
            choices = list(master_skus)
            rng.shuffle(choices)
            for sku in choices:
                if len(specs) >= line_count:
                    break
                if sku == C.STOCKOUT_SKU and store_id == C.STOCKOUT_STORE:
                    continue
                want = int(rng.integers(1, 4))
                floor = 8 if (sku, store_id) == (C.VARIANCE_SKU, C.VARIANCE_STORE) else 0
                room = ledger.on_hand.get((sku, store_id), 0) - floor
                if room <= 0:
                    continue
                specs.append((sku, min(want, room)))
            if not specs:
                continue
            order_n += 1
            hour = int(rng.integers(15, 18))
            minute = int(rng.integers(0, 60))
            order_ts = f"{day.isoformat()}T{hour:02d}:{minute:02d}:00Z"
            add_order(
                pm,
                order_id=f"O{day.strftime('%Y%m%d')}{order_n:05d}",
                order_ts=order_ts,
                customer_id=customer_id,
                store_id=store_id,
                channel_id=channel_id,
                line_specs=specs,
                status="SHIPPED",
                extract_ts=extract_pm,
                fulfill=True,
                movement_ts=None,
                explicit=False,
            )

        # A few partial returns of the prior day's non-explicit lines.
        prior = [
            row
            for row in returnable
            if row["order_ts"][:10] < day.isoformat() and row["qty"] - row["returned"] >= 2
        ]
        take = 1 if scale == "tiny" else 4 if scale == "small" else 12
        if prior:
            pick = rng.choice(len(prior), size=min(take, len(prior)), replace=False)
            chosen = [prior[int(i)] for i in (pick.tolist() if hasattr(pick, "tolist") else [pick])]
            for slot, line_ref in enumerate(chosen):
                return_n += 1
                minute = min(59, 10 + slot)
                return_ts = f"{day.isoformat()}T18:{minute:02d}:00Z"
                add_return(
                    pm,
                    return_id=f"R{return_n:08d}",
                    order_id=line_ref["order_id"],
                    line_number=line_ref["line_number"],
                    qty=1,
                    extract_ts=extract_pm,
                    return_ts=return_ts,
                    reason="SIZE",
                )
                line_ref["returned"] += 1

        if day == days[-1]:
            closing = ledger.snapshot_rows(day.isoformat(), extract_pm)
            pm["inventory_snapshots"].extend(closing)
            written_snapshots.extend(closing)
            insert = dict(late_customer)
            pm["cdc_customers"].append(
                {
                    "sequence_number": str(seq.next()),
                    "op": "I",
                    "change_ts": "2024-11-10T18:00:00Z",
                    "extract_ts": extract_pm,
                    **insert,
                }
            )
            pm["cdc_products"].append(
                cdc_product(C.LATE_SKU, {}, "I", "2024-11-10T18:00:00Z", extract_pm, True)
            )
            pm["cdc_customers"].append(
                cdc_customer(
                    C.DELETE_CUSTOMER,
                    {"status": "DELETED"},
                    "D",
                    "2024-11-10T18:05:00Z",
                    extract_pm,
                    True,
                )
            )
            pm["cdc_products"].append(
                cdc_product(
                    C.DISCONTINUE_SKU,
                    {"status": "DISCONTINUED"},
                    "U",
                    "2024-11-10T18:10:00Z",
                    extract_pm,
                    True,
                )
            )
            pm["cdc_products"].append(
                cdc_product(C.DELETE_SKU, {}, "D", "2024-11-10T18:12:00Z", extract_pm, True)
            )
            stale = cdc_from_order(
                C.CANCELLED_ORDER,
                "OPEN",
                "2024-11-09T16:10:00Z",
                extract_pm,
                sequence=int(insert_images[C.CANCELLED_ORDER]["sequence_number"]),
            )
            pm["cdc_orders"].append(stale)
            replay = next(row for row in ledger.clean if row["movement_id"] == C.REPLAY_MOVEMENT)
            replay_copy = dict(replay)
            replay_copy["extract_ts"] = extract_pm
            pm["inventory_movements"].append(replay_copy)

        batches[f"{day.isoformat()}T0600"] = am
        batches[f"{day.isoformat()}T1800"] = pm

    variances = compute_variances(written_snapshots, ledger.clean)
    expected = [
        {
            "sku": C.VARIANCE_SKU,
            "store_id": C.VARIANCE_STORE,
            "snapshot_date": C.VARIANCE_DATE,
            "variance_qty": -1,
        },
        {
            "sku": C.VARIANCE_SKU,
            "store_id": C.VARIANCE_STORE,
            "snapshot_date": C.REVERSAL_DATE,
            "variance_qty": 1,
        },
    ]
    if variances != expected:
        raise RuntimeError(f"variance self-check failed: {variances}")

    file_counts: dict[str, int] = {}
    corrupt_targets: list[Path] = []
    for batch_ts, tables in batches.items():
        folder = out / f"batch_ts={batch_ts}"
        folder.mkdir()
        for entity, rows in tables.items():
            if not rows:
                continue
            path = folder / f"{entity}.csv"
            columns = COLUMNS[entity]
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=columns, lineterminator="\n")
                writer.writeheader()
                for row in rows:
                    writer.writerow(
                        {col: "" if row.get(col) is None else row[col] for col in columns}
                    )
            file_counts[f"{batch_ts}/{entity}.csv"] = len(rows)
            if batch_ts.endswith("2024-11-10T0600") and entity == "order_lines":
                corrupt_targets.append(path)

    for path in corrupt_targets:
        with path.open("a", encoding="utf-8") as handle:
            handle.write("only,three,fields\n")
        file_counts["2024-11-10T0600/order_lines.csv"] += 1

    manifest = {
        "seed": seed,
        "scale": scale,
        "calendar_start": days[0].isoformat(),
        "calendar_end": days[-1].isoformat(),
        "batches": [f"{day.isoformat()}T0600" for day in days]
        + [f"{day.isoformat()}T1800" for day in days],
        "file_counts": file_counts,
        "variances": variances,
        "planted": {
            "cancelled_order_id": C.CANCELLED_ORDER,
            "partial_return_order_id": C.PARTIAL_ORDER,
            "partial_return_id": "R-PART-0001",
            "partial_refund_amt": "229.00",
            "late_customer_resolved": C.LATE_CUSTOMER,
            "late_customer_unresolved": C.GHOST_CUSTOMER,
            "late_sku_resolved": C.LATE_SKU,
            "tier_customer": C.TIER_CUSTOMER,
            "deleted_customer": C.DELETE_CUSTOMER,
            "price_sku": C.PRICE_SKU,
            "category_sku": C.CATEGORY_SKU,
            "discontinued_sku": C.DISCONTINUE_SKU,
            "deleted_sku": C.DELETE_SKU,
            "stockout_sku": C.STOCKOUT_SKU,
            "stockout_store": C.STOCKOUT_STORE,
            "negative_movement_id": C.NEGATIVE_MOVEMENT,
            "replay_movement_id": C.REPLAY_MOVEMENT,
            "duplicate_order_id": C.DUP_ORDER,
            "bad_email_customer": C.EMAIL_CUSTOMER,
        },
    }
    # Stable batch order in the manifest (each day AM then PM).
    manifest["batches"] = []
    for day in days:
        manifest["batches"].append(f"{day.isoformat()}T0600")
        manifest["batches"].append(f"{day.isoformat()}T1800")
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest
