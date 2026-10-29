# Quarantine the row, fail the run on reconciliation

Format, enum, referential, and negative-on-hand failures are written to `silver.quarantine` and counted in `audit.dq_metric`. A batch whose failure rate is at least `dq_fail_rate_alert_threshold` (0.15 in dev and test, 0.02 in prod) emits a `DQ_FAIL_RATE` alert and continues. The dirty Sunday-morning extract is supposed to alert.

The run fails after gold if a structural check fails or if inventory variances do not match `manifest.json`. Writing the reconciliation table happens before the exception, so a failed run still leaves the check rows.

Webhook delivery is best-effort. `NL_ALERT_WEBHOOK_URL` empty means the alert row is still written and no HTTP call is made. A failed POST records `WEBHOOK_FAILED` and does not change the pipeline status.
