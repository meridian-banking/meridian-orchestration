# Runbook: Historical backfill

**When to use:** reprocessing past dates after a pipeline bug fix, a schema
change, or to populate history for a newly added table.

**Risk level:** HIGH. A backfill writes to production tables across many dates.
Read the prerequisites before starting.

---

## Prerequisites — verify ALL before starting

- [ ] **The bug is actually fixed and deployed.** Backfilling with the broken
      code reprocesses the same wrong numbers over a wider date range.
- [ ] **The loads are idempotent for the affected tables.** If re-running a
      date would duplicate rows, STOP. Backfill is unsafe without this.
- [ ] **The `warehouse_pool` exists** with a slot count you have deliberately
      chosen (see "Sizing the pool" below).
- [ ] **You know the date range** and have written it down. "Backfill
      everything" is how people accidentally reprocess three years.
- [ ] **Someone else knows you are doing this.** A backfill saturating the
      warehouse looks identical to an incident from the outside.

## Sizing the pool

```bash
airflow pools set warehouse_pool 2 "Limits concurrent warehouse writes"
```

Two slots is the default here, not an arbitrary number: the warehouse is a
single Postgres instance, and concurrent SCD2 merges on the same dimension
contend for the same rows. More slots buys wall-clock speed and costs
contention. Start at 2, watch, increase only if the warehouse is idle.

## Running it

```bash
# Dry run FIRST — lists what would run without running it.
airflow dags backfill historical_backfill \
  --start-date 2024-03-01 --end-date 2024-03-07 --dry-run

# Then for real, one week at a time.
airflow dags backfill historical_backfill \
  --start-date 2024-03-01 --end-date 2024-03-07
```

**Why one week at a time rather than the whole range?** Because a backfill that
goes wrong at date 400 of 700 leaves you reasoning about a partially-completed
operation. Small batches mean small blast radius and a clean stopping point.

## Monitoring while it runs

```sql
-- Are loads actually landing?
SELECT target_table, status, count(*), max(finished_at)
FROM audit.load_log
WHERE started_at > now() - interval '2 hours'
GROUP BY 1, 2 ORDER BY 3 DESC;

-- Is the quality gate rejecting historical data?
SELECT dataset, check_name, rows_failed, executed_at
FROM dq.check_results
WHERE NOT passed AND executed_at > now() - interval '2 hours'
ORDER BY executed_at DESC;
```

Watch the Airflow UI's pool view. If `warehouse_pool` is permanently full and
queued tasks are stacking up, the warehouse is the bottleneck — let it drain
rather than raising the slot count mid-flight.

## Stopping a backfill in progress

```bash
airflow dags pause historical_backfill
```

Pausing stops NEW runs; in-flight tasks finish. That is usually what you want —
killing a task mid-transaction is how you get partially-applied work. If you
genuinely must stop immediately, mark the running tasks failed in the UI, then
verify the affected dates with the reconciliation query below.

## After it completes — verify, do not assume

```bash
cd meridian-pipelines
python -m meridian_pipelines reconcile \
  --source-dir ../meridian-data-generator/output/parquet --date 2024-03-07
```

```sql
-- SCD2 sanity: no customer may have two current versions.
SELECT customer_id, count(*) FROM curated.dim_customer
WHERE is_current GROUP BY 1 HAVING count(*) > 1;   -- expect zero rows

-- SCD2 sanity: no overlapping validity windows.
SELECT a.customer_id FROM curated.dim_customer a
JOIN curated.dim_customer b
  ON a.customer_id = b.customer_id AND a.customer_key <> b.customer_key
 AND a.valid_from < b.valid_to AND b.valid_from < a.valid_to;  -- expect zero rows
```

The second query matters specifically after a backfill: `depends_on_past=True`
exists to keep dates in chronological order, and this is how you confirm it
worked.

## Common problems

| Symptom | Cause | Fix |
|---|---|---|
| Every date skipped, nothing loads | Watermark is past these dates | `--no-watermark` (already set in this DAG) |
| Runs queue but never start | Pool has no free slots, or DAG is paused | Check the pool view; unpause |
| Date N+1 stuck in `upstream_failed` | `depends_on_past` and date N failed | Fix and clear date N first — this is intended |
| Warehouse slow, everything backs up | Too many pool slots | Reduce slots; let the queue drain |
