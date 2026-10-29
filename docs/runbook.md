# Runbook

## Normal local run

```bash
make setup
make pipeline    # uses data/sample, writes data/lake
make dashboard   # http://localhost:8501
```

`make generate` rewrites `data/sample` at small scale, seed 42. You do not need to regenerate to run the committed sample.

## What success looks like

`audit.pipeline_run.status` is `SUCCESS`. `audit.reconciliation` for that `run_id` is all `PASS`. `audit.alert` may contain `DQ_FAIL_RATE` (the dirty batch) and `INVENTORY_VARIANCE` at INFO for the two planted breaks. Those alerts are expected. `RECONCILIATION` or `PIPELINE_FAILED` is not.

## Rerun

A second `make pipeline` skips every committed batch and rebuilds gold. Row counts on silver stay put. Use `--force` only when you intend to replay files; MERGE and the movement id guard keep the grain unique.

```bash
.venv/bin/python -m northline.cli run --env dev --force
```

## Stop after silver

```bash
.venv/bin/python -m northline.cli run --env dev --fail-after silver
```

The process exits non-zero, batches are committed, gold is absent. The next run without `--fail-after` builds gold and does not reload the files.

## Reconciliation failure

The check rows are already in `audit.reconciliation`. Fix the silver bug or the manifest, delete `data/lake/gold` if you want a clean rebuild, and rerun. Do not delete `audit.batch_commit` unless you also pass `--force`.

## Quarantine review

```text
silver/quarantine  rule_id, natural_key, raw_json, batch_ts
```

`NEGATIVE_ON_HAND` means the movement was not posted. `BAD_EMAIL` means that CDC image was not applied. Replaying the same file does not insert a second quarantine row (`quarantine_id`).

## Inventory break

`gold/inventory_variance` where `variance_qty <> 0`. The sample expects exactly:

- `SKU-SHL-0001`, `STR01`, `2024-11-09`, `-1`
- `SKU-SHL-0001`, `STR01`, `2024-11-10`, `1`

Any other non-zero row fails the run.

## Webhook

Set `NL_ALERT_WEBHOOK_URL` to a POST endpoint. The body is `run_id`, `severity`, `code`, `message`. A connection error is logged and stored as `WEBHOOK_FAILED`. The pipeline status does not change.

## Pause a dedicated pool

If you deployed `infra/main.bicep`, pause `nlretail` when you are not serving reports. The bicep template does not pause it for you.

## Logs

Structured lines are in `data/logs/pipeline.jsonl`. The human summary is the CLI stdout. `data/logs/latest_summary.json` is the last successful summary.
