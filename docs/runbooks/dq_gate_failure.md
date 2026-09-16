# Runbook: The quality gate failed

**Alert:** `eod_pipeline.quality_gate` failed
**Severity:** SEV-3 by default — batch delayed, nothing incorrect published

---

## What this means

The gate found ERROR-severity data-quality failures and stopped the pipeline.
Downstream tasks are `upstream_failed`; **the warehouse was not written to**.

This is the system working. Do not "fix" it by loosening the rule — the
alternative to a delayed report is a quietly wrong one, which is much worse.

## 1. Find out what failed (2 minutes)

Open the task log from the alert URL. Look for `FAIL[ERROR]` lines. Or query:

```sql
SELECT dataset, check_name, check_type, rows_failed, message, executed_at
FROM dq.check_results
WHERE NOT passed AND severity = 'ERROR'
  AND executed_at > now() - interval '3 hours'
ORDER BY executed_at DESC;
```

## 2. Look at the actual bad rows (2 minutes)

```sql
SELECT dataset, business_key, reason_codes, row_data
FROM dq.quarantine
WHERE NOT resolved
ORDER BY quarantined_at DESC
LIMIT 20;
```

The full original row is preserved as JSONB. Read the values — the defect is
usually obvious immediately.

> Known imprecision: a quarantined row is currently tagged with every
> ERROR-severity check name for that dataset, not only the ones it failed. The
> row data tells you the real defect.

## 3. Decide which case you are in

**Case A — the source data is genuinely wrong.**
Contact the source system owner. Get a corrected extract. Re-land it, then
clear and re-run `quality_gate` from the Airflow UI. Do not bypass the gate.

**Case B — the rule is wrong.**
Sometimes a legitimate business change (a new product type, a new segment)
violates a rule that was correct last month. Update `config/dq_rules.yaml`,
open a PR, get it reviewed. **Still do not bypass the gate** — fix the rule and
re-run, so the change is reviewed and recorded rather than invisible.

**Case C — a handful of bad rows out of millions, and the batch is urgent.**
The rows are already quarantined and the clean rows are unaffected. If the
business genuinely needs the batch tonight, this is a judgement call requiring
a named approver, and it goes in the incident record. It is not a decision to
make alone at 3am.

## 4. Recover

```
Airflow UI -> eod_pipeline -> the failed run -> quality_gate -> Clear
```

Clearing re-runs the task and everything downstream. **This is safe without any
cleanup step** because the loads are idempotent — re-running a date does not
duplicate rows.

## 5. Afterwards

- Mark the quarantined rows resolved:
  ```sql
  UPDATE dq.quarantine
     SET resolved = TRUE, resolved_at = now(), resolution_note = 'source corrected'
   WHERE dataset = 'transactions' AND NOT resolved;
  ```
- If this is the second occurrence of the same defect, write a postmortem. One
  failure is an event; two is a pattern with a missing control behind it.

## What NOT to do

- **Do not add retries to the gate.** Bad data is bad in five minutes too.
- **Do not change severity to WARN to make it pass.** That silently removes the
  control while leaving the reassuring appearance of one.
- **Do not manually load the data around the pipeline.** The warehouse then
  contains rows no reconciliation can explain.
