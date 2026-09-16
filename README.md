# meridian-orchestration

Airflow DAGs that run the Meridian platform on a schedule. This is where the data-quality gate from `meridian-pipelines` stops being a command you type and becomes an automated control that physically prevents bad data from reaching the warehouse.

## The DAGs

| DAG | Schedule | Purpose |
|---|---|---|
| `eod_pipeline` | `0 2 * * *` (nightly) | generate → ingest → **quality gate** → warehouse load → reconcile |
| `analytics_refresh` | dataset-triggered | Runs when the curated warehouse updates — no cron at all |
| `historical_backfill` | manual | Reprocess past dates with concurrency and ordering controls |

## The gate is the point

```
ingest → [ QUALITY GATE ] → warehouse load → refresh → reconcile
              │
         exit code 1
              │
              ▼
    4 downstream tasks → upstream_failed
    warehouse untouched
```

Sprint 5 built a command whose exit code means something. Here that exit code is load-bearing: `retries=0` on the gate, `trigger_rule=all_success` downstream, and a failure means the warehouse load **never executes**. Not "runs and logs a warning."

Both of those settings have tests, because a one-word change (`all_success` → `all_done`) would silently remove the entire control while leaving the reassuring appearance of one.

## Key design decisions

**Retries encode transient vs permanent.** Airflow can't tell the difference, so we do. Infrastructure tasks retry 3 times with exponential backoff — a blipped connection usually succeeds on attempt two, and backoff avoids hammering a struggling service. The quality gate retries **zero** times: data that violates a rule will still violate it in five minutes, and retrying only delays the alert a human needs.

**`logical_date`, never `now()`.** Every task takes `{{ ds }}`. This is the single most important idea in Airflow and the most common beginner mistake: using `now()` means a backfill of March 2022 would process *today's* data 700 times, and a run that retries across midnight would process a different day than it started with.

**Datasets for cross-DAG dependencies.** `eod_pipeline` declares it *produces* `postgres://meridian/curated`; `analytics_refresh` declares it *consumes* it. Neither DAG names the other. This beats `ExternalTaskSensor` (pull-based, brittle to schedule changes, holds a worker slot) and `TriggerDagRunOperator` (inverts ownership — the upstream DAG must know every consumer). See [ADR 0008](docs/adr/0008-datasets-over-sensors.md).

**`catchup=False` on scheduled DAGs, `True` only on backfill.** Unpausing a DAG with a six-month-old `start_date` and `catchup=True` immediately queues 180 runs. Backfill is the one place that behaviour is the point, so it lives in its own DAG where it's explicit.

**`depends_on_past=True` on the backfill load.** SCD2 history is built sequentially — processing March before February produces validity windows in the wrong order and silently corrupts customer history. The cost is that one stuck day blocks every day after it; that's the right trade for ordered history and the wrong one for independent daily aggregates.

## Quick start

```bash
pip install -e ".[dev]"
make test          # 21 DAG integrity tests
make deploy        # copy DAGs into the running Airflow container
make pool          # create warehouse_pool (limits concurrent writes)
make list-dags
```

Then open Airflow at http://localhost:8080 and unpause `eod_pipeline`.

## Operations

- **[Backfill runbook](docs/runbooks/backfill.md)** — prerequisites, pool sizing, monitoring, verification, and what to do when it goes wrong
- **[Quality gate failure runbook](docs/runbooks/dq_gate_failure.md)** — triage in under 5 minutes, the three cases, and what *not* to do
- **[Incident postmortem](docs/incidents/2024-03-16-dq-gate-failure.md)** — a real gate failure, blamelessly analysed

The postmortem's finding is worth reading: every technical control worked perfectly, and the actual problem was a 24-minute gap before a human saw the alert. That's a more common shape of incident than people expect.

## Testing

DAG integrity tests don't *run* the DAGs — they assert structural properties. The pattern is **encode your conventions as tests**: a convention in a README is a suggestion, a convention with a test is a rule. These catch the overwhelming majority of "the scheduler is showing an import error" incidents before merge.

Enforced: no import errors, no cycles, every DAG has a real owner and tags and docs, concurrency is bounded, failure callbacks exist, the gate doesn't retry, infrastructure tasks do, the warehouse load is downstream of the gate with `all_success`, backfill is serialised with `depends_on_past` and a pool, and long-running tasks have timeouts.

Part of the 8-repository Meridian platform.
