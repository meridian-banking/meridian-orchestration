# ADR 0008: Datasets for cross-DAG dependencies, not sensors or triggers

## Status
Accepted — 2026-07

## Context
`analytics_refresh` must not run until the warehouse is loaded by
`eod_pipeline`. Putting both in one DAG would couple two different teams'
release cycles and produce a DAG too large to read or own. Three mechanisms
exist for expressing a dependency across DAG boundaries.

## Options considered

**1. ExternalTaskSensor.** DAG B polls until a named task in DAG A succeeds.
Works on any Airflow version, but it is PULL-based and brittle: the sensor must
know the other DAG's id, task id, AND schedule alignment. Change the upstream
schedule and the sensor waits forever with no error. It also occupies a worker
slot while polling unless configured in reschedule or deferrable mode.

**2. TriggerDagRunOperator.** DAG A explicitly triggers DAG B. Simple and
immediate, but it inverts ownership: the upstream DAG must now know about every
consumer. Adding a fifth downstream team means editing the platform team's DAG
for the fifth time.

**3. Datasets (Airflow 2.4+).** DAG A declares an `outlet`; DAG B sets
`schedule=[dataset]`. Airflow schedules B when A updates it.

## Decision
Use Datasets. `eod_pipeline.warehouse.refresh_materialized_views` produces
`postgres://meridian/curated`; `analytics_refresh` consumes it.

## Rationale
Neither DAG names the other. The dependency is on the DATA, which is what the
dependency actually is — analytics does not depend on "the EOD pipeline
running", it depends on "the curated warehouse being fresh". If that freshness
came from a backfill instead, analytics should still run, and with datasets it
does.

It also removes an entire category of bug: there is no schedule alignment to
get wrong, and no polling task consuming a worker slot.

## Consequences
+ Adding a consumer requires no change to the producer.
+ `analytics_refresh` has no cron at all — it cannot drift out of sync with
  upstream timing because it has no timing of its own.
+ The Airflow UI renders the dataset graph, so the dependency is discoverable.
- Requires Airflow 2.4+. Not available on older deployments.
- Dataset updates fire on task SUCCESS only. A task that succeeds without
  actually updating the data would still trigger consumers — the dataset is a
  declaration of intent, not a verified fact about the data.
- Datasets are identified by an opaque URI string. A typo produces a silently
  disconnected graph, which is why there is a test asserting the producer and
  consumer URIs match.
