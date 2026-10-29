# Data dictionary

Synthetic data only. Amounts are decimal dollars rounded half-up to cents in the generator and stored as doubles in the local Delta tables. The dedicated-pool DDL uses `decimal(18,2)`.

## Orders and lines

| Column | Meaning |
| --- | --- |
| order_id | `O-…` planted id or `O` + `yyyyMMdd` + 5 digits |
| order_ts | Event time, UTC, `yyyy-MM-ddTHH:mm:ssZ` |
| channel_id | `RETAIL`, `WEB`, `MARKETPLACE` |
| store_id | Fulfillment location. Web is usually `DC01`. A planted web order can ship from a store. |
| status | `OPEN`, `SHIPPED`, `CANCELLED`, `PARTIALLY_RETURNED`, `RETURNED` |
| sequence_number | Monotonic per entity. Higher wins. |
| line_number | 1-based |
| qty | Positive integer on accepted lines |
| unit_price | Transaction price, not necessarily the as-of list price after a later CDC |
| discount_amt | Merchandise discount. Trail Week is 15% of Shelter list from 2024-11-09 |
| tax_amt | State rate on merchandise after discount. Not part of net sales |
| line_status | `OPEN`, `SHIPPED`, `CANCELLED` |

## Payments and returns

| Column | Meaning |
| --- | --- |
| tender_type | `CARD`, `GIFT_CARD`, `PAYPAL`, `STORE_CREDIT` |
| payment status | `CAPTURED` adds, `REFUNDED` subtracts, `VOIDED` signs to 0 on `fact_payments.signed_amount` |
| qty_returned | Units sent back. Must be ≥ 1 and ≤ ordered qty or the row is quarantined |
| refund_amt | Merchandise only. Tax on the refund is on the refund tender, not in net sales |
| reason_code | `CHANGED_MIND`, `SIZE`, others |

## Inventory

| Column | Meaning |
| --- | --- |
| movement_type | `RECEIPT`, `SALE`, `TRANSFER_IN`, `TRANSFER_OUT`, `ADJUSTMENT`, `SHRINK`, `RETURN` |
| qty | Always positive |
| sign | `1` or `-1`. Adjustments use it. Other types also store their natural sign |
| qty_signed | Posted units. Source of truth for rollforward |
| reference_id | `order_id-line` for sales, return id, transfer id, or purchase order |
| snapshot_date | End-of-day on-hand date |
| on_hand_qty | Book quantity from the snapshot file, not recomputed |
| on_hand_cost | Quantity times the standard cost in effect when the snapshot was written |

## Customer and product history

| Column | Meaning |
| --- | --- |
| effective_start / effective_end | Inclusive start, exclusive end. Null end means open |
| is_current | One open row per natural key, including a delete tombstone |
| is_inferred | Attributes were not on a source image yet |
| is_deleted | CDC `op=D` |
| applied_sequence | Last CDC sequence applied to this version. Inferred rows use `-1`. Snapshots use `0` |
| loyalty_tier | `NONE`, `SILVER`, `GOLD`, `TRAIL`, or `UNKNOWN` |
| list_price / unit_cost | Strings in silver SCD so the hash is stable (`229.00`). Doubles on the fact as `list_price_asof` and `unit_cost_asof` |

## Fact sales additions

| Column | Meaning |
| --- | --- |
| sales_sk | `xxhash64(order_id, line_number)` |
| customer_sk / product_sk | Hash of natural key plus `effective_start` of the matched version |
| is_cancelled | Order status or line status is `CANCELLED` |
| gross_sales_amt | 0 when cancelled, else qty × price − discount |
| net_sales_amt | gross − refund, 0 when cancelled |
| cogs_amt | `qty_net * unit_cost_asof`, null when the matched product cost is null (inferred) |
| qty_net | 0 when cancelled, else ordered − returned, floored at 0 |

## Audit

| Table | Meaning |
| --- | --- |
| pipeline_run | One row per run id, updated to SUCCESS or FAILED |
| batch_commit | Folder watermark |
| dq_metric | Seen and failed counts per entity per batch |
| reconciliation | One row per check |
| alert | DQ rate, variance info, pipeline failure, webhook failure |
