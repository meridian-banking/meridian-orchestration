# Incident 2024-03-16-01: EOD pipeline blocked by data quality gate

**Status:** Resolved
**Severity:** SEV-3 (batch delayed, no incorrect data published)
**Duration:** 47 minutes from failure to resolution
**Author:** data-platform

> This is a *blameless* postmortem. The question is never "who did this" but
> "what about the system made this possible, and what makes it less likely
> next time". Teams that hunt for culprits get incidents hidden rather than
> reported, which is strictly worse.

---

## Summary

The nightly `eod_pipeline` run for logical date 2024-03-16 failed at the
`quality_gate` task. Four downstream tasks were blocked and the warehouse was
not loaded. Morning risk reports were delayed by approximately 50 minutes.

**No incorrect data was published.** The control worked exactly as designed —
this incident is a record of the system doing its job, not failing at it.

---

## Impact

- `eod_pipeline` run `2024-03-16` failed at `quality_gate`
- 4 downstream tasks set to `upstream_failed`: warehouse load, view refresh,
  reconcile, end
- Morning credit-risk dashboards stale by ~50 minutes
- No wrong numbers reached any consumer

---

## Timeline (UTC)

| Time | Event |
|---|---|
| 02:00 | `eod_pipeline` scheduled run begins |
| 02:04 | `generate_source_data` succeeds |
| 02:06 | `ingest.land_raw` and `ingest.promote_staged` succeed |
| 02:07 | **`quality_gate` fails with exit code 1** |
| 02:07 | `on_failure_callback` fires; alert emitted with task, date, log URL |
| 02:07 | Downstream tasks marked `upstream_failed`; warehouse untouched |
| 02:31 | On-call acknowledges, opens the task log from the alert URL |
| 02:38 | Root cause identified from `dq.quarantine` |
| 02:49 | Upstream source file corrected and re-landed |
| 02:54 | `quality_gate` cleared and re-run from the Airflow UI |
| 02:57 | Pipeline completes; reconciliation balanced |

---

## Root cause

The `quality_gate` task reported two ERROR-severity failures on the
`transactions` dataset:

```
FAIL[ERROR]  transactions.account_id   referential_account_id   1 row references a missing account
FAIL[ERROR]  transactions.amount       validity_amount          1 row below min 0
```

Investigation via the quarantine table:

```sql
SELECT business_key, reason_codes,
       row_data->>'account_id' AS account,
       row_data->>'amount'     AS amount
FROM dq.quarantine
WHERE dataset = 'transactions' AND NOT resolved
ORDER BY quarantined_at DESC;
```

```
 business_key  |              reason_codes              |    account    | amount
---------------+----------------------------------------+---------------+--------
 TXN_00000001  | referential_account_id,validity_amount | ACCT_99999999 | 89.44
 TXN_00000002  | referential_account_id,validity_amount | ACCT_00000493 | -50.0
```

Two distinct defects in the upstream extract:

1. `TXN_00000001` referenced `ACCT_99999999`, an account that does not exist
   in the account master. Had this loaded, it would have been silently dropped
   by the inner join in the fact load, understating the day's transaction
   count with no error anywhere.
2. `TXN_00000002` carried a negative amount, which is not valid money movement
   for this feed and would have understated total volume.

---

## What went well

- **The gate held.** `retries=0` meant no time wasted retrying a permanent
  failure, and the alert reached a human within seconds rather than after
  fifteen minutes of pointless retries.
- **The alert was actionable.** It named the DAG, task, logical date, and
  linked directly to the log. The on-call did not have to go hunting.
- **Quarantine made diagnosis fast.** The full original rows were preserved as
  JSONB with reason codes, so root cause took seven minutes rather than an
  archaeology session through raw files.
- **Idempotency made recovery trivial.** Clearing and re-running the task was
  safe with no cleanup step, because re-running the load does not duplicate
  rows. Recovery was one click.

## What went badly

- **24 minutes to acknowledge.** The alert was logged but not routed anywhere
  a human sees at 02:07. A log line is not an alert.
- **The quarantine `reason_codes` were imprecise.** Both rows were tagged with
  *both* check names even though each failed only one. Still useful for
  debugging — the row data makes the real defect obvious — but it sends the
  reader on a brief detour.
- **No upstream notification.** Nothing told the source system team their
  extract had a defect; it was found downstream by us.

---

## Action items

| # | Action | Owner | Priority |
|---|---|---|---|
| 1 | Route `on_failure_callback` to a real paging channel, not just logs | data-platform | High |
| 2 | Tag quarantined rows with only the checks they individually failed | data-platform | Medium |
| 3 | Add an alert when `dq.quarantine` gains rows, so source teams are notified | data-platform | Medium |
| 4 | Add a runbook entry for gate failures (done — see `runbooks/`) | data-platform | Done |

---

## Lessons

**A control that fires is a control that works.** The instinct after an
incident like this is to loosen the rule so the pipeline stops failing. That is
exactly backwards: the alternative outcome was two defective transactions
silently entering a warehouse that feeds regulatory credit reporting, with
nothing anywhere indicating a problem. A delayed report is recoverable; a
quietly wrong one is not.

**The 24-minute acknowledgement gap is the real finding.** The technical
controls worked perfectly. The human notification path did not. That is the
more common shape of an incident than people expect — the code was fine, the
alerting was not.
