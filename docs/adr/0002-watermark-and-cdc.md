# Two watermarks

ADF's `ctl.ingest_watermark` records which files were copied into the lake. That table lives in the dedicated SQL pool and was not executed here.

The job that was executed stores `audit.batch_commit` (one row per folder whose silver step committed) and `applied_sequence` on each silver entity. A file replay with `--force` is safe because:

- bronze `ingest_id` is a hash of batch, entity, and payload, so an identical physical row is not appended twice
- in-file exact duplicates collapse to one bronze row and increment `exact_duplicates_removed`
- MERGE updates a current row only when the incoming sequence is greater
- movement ids already on the ledger are not posted again

The cancelled order's stale OPEN image is copied into the last batch at the original sequence. The silver status stays `CANCELLED`.
