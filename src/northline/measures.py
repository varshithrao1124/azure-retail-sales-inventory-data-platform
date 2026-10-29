"""Quantity sign rules shared by the generator and the Spark writers.

Synapse SQL repeats this CASE for analysts who only have the raw movement.
The lake stores ``qty_signed`` so gold does not re-derive it.
"""

MOVEMENT_SIGNS = {
    "RECEIPT": 1,
    "SALE": -1,
    "TRANSFER_IN": 1,
    "TRANSFER_OUT": -1,
    "SHRINK": -1,
    "RETURN": 1,
}

SIGNED_QTY_SQL = """
CASE movement_type
  WHEN 'ADJUSTMENT' THEN CAST(qty AS INT) * CAST(sign AS INT)
  WHEN 'RECEIPT' THEN CAST(qty AS INT)
  WHEN 'TRANSFER_IN' THEN CAST(qty AS INT)
  WHEN 'RETURN' THEN CAST(qty AS INT)
  WHEN 'SALE' THEN CAST(qty AS INT) * -1
  WHEN 'TRANSFER_OUT' THEN CAST(qty AS INT) * -1
  WHEN 'SHRINK' THEN CAST(qty AS INT) * -1
  ELSE CAST(NULL AS INT)
END
""".strip()


def signed_qty(movement_type: str, qty: int, sign: int | None = None) -> int:
    if movement_type == "ADJUSTMENT":
        if sign not in (-1, 1):
            raise ValueError("ADJUSTMENT requires sign +1 or -1")
        return int(qty) * int(sign)
    try:
        multiplier = MOVEMENT_SIGNS[movement_type]
    except KeyError as exc:
        raise ValueError(f"Unknown movement type {movement_type}") from exc
    return int(qty) * multiplier


def compute_variances(snapshot_rows: list[dict], movement_rows: list[dict]) -> list[dict]:
    """Day-over-day qty variance. The first snapshot per sku/store is an opening balance.

    A movement counts toward a snapshot when its date is after the previous
    snapshot date and on or before the snapshot date.
    """

    snaps: dict[tuple[str, str], dict[str, int]] = {}
    for row in snapshot_rows:
        key = (str(row["sku"]), str(row["store_id"]))
        snaps.setdefault(key, {})[str(row["snapshot_date"])] = int(row["on_hand_qty"])

    moves: dict[tuple[str, str], dict[str, int]] = {}
    for row in movement_rows:
        key = (str(row["sku"]), str(row["store_id"]))
        day = str(row.get("movement_date") or str(row["movement_ts"])[:10])
        moves.setdefault(key, {})
        moves[key][day] = moves[key].get(day, 0) + signed_qty(
            str(row["movement_type"]),
            int(row["qty"]),
            int(row["sign"]) if str(row.get("sign", "")) not in {"", "None"} else None,
        )

    found: list[dict] = []
    for key, by_date in snaps.items():
        dates = sorted(by_date)
        for prev, current in zip(dates, dates[1:], strict=False):
            total = sum(qty for day, qty in moves.get(key, {}).items() if prev < day <= current)
            variance = by_date[current] - (by_date[prev] + total)
            if variance != 0:
                found.append(
                    {
                        "sku": key[0],
                        "store_id": key[1],
                        "snapshot_date": current,
                        "variance_qty": variance,
                    }
                )
    found.sort(key=lambda item: (item["snapshot_date"], item["sku"], item["store_id"]))
    return found
