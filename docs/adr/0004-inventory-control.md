# Movements, snapshots, and a negative guard

The warehouse sends both a movement stream and a nightly snapshot. The snapshot is not recomputed from movements, because a bad snapshot is the thing the variance report exists to find. The first snapshot per sku and store is an opening balance and is not variance-checked. Later days must equal the previous snapshot plus signed movements in between.

Signed quantity is computed once in `northline.measures.signed_qty` and stored as `qty_signed`. Adjustments carry an explicit sign. Sales, shrink, and transfer-out are negative.

Before a movement is appended, the job walks on-hand from the opening snapshot plus movements already accepted. A post that would go negative is quarantined (`NEGATIVE_ON_HAND`) and does not enter silver. The planted shrink of the stocked-out lantern is that case. The planted snapshot short on 2024-11-09 is a valid non-negative count that simply does not match the book; the next truthful snapshot shows a reversing +1. Both rows are in `manifest.json`, and reconciliation fails if the pipeline's variance set differs.
