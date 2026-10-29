# Inferred members instead of dropping the fact

Rejecting an order because the customer file has not arrived loses the sale. Silver inserts an inferred SCD row from the epoch timestamp with unknown attributes and `applied_sequence = -1`. Any real CDC sequence replaces it: the inferred row is closed at `change_ts` and the mastered row opens.

Facts keep the surrogate of the version that was true at the event time. `O-LATE-0001` therefore still points at the inferred customer and product after Sunday's CDC, while `dim_*_current` shows Rowan Ellis and Trail Mug. `C000998` never gets a CDC insert, so the inferred row stays current and the fact is still conformed.

The epoch start is deliberate. A late file can describe an event from an earlier day. Starting the inferred row at ingest time would leave that event without a dimension.
