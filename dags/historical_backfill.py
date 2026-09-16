"""Historical backfill: reprocess past dates safely.

WHAT BACKFILL IS
You have three years of history and a pipeline that was written last month.
Backfill runs that pipeline over past dates so the warehouse reflects all of
it. It is also what you do after fixing a bug: reprocess the affected window
rather than living with wrong numbers.

WHY BACKFILL IS DANGEROUS, AND WHAT MAKES IT SAFE

1. WITHOUT IDEMPOTENCY IT IS UNUSABLE.
   Backfilling 700 days means running the same load logic 700 times. If a
   re-run duplicated rows, the backfill would corrupt the warehouse far worse
   than the gap it was fixing. Every idempotency decision from Sprints 2 and 3
   — deterministic object keys, ON CONFLICT DO NOTHING, unique constraints on
   fact grain — exists so that this operation is survivable.

2. WITHOUT CONCURRENCY LIMITS IT IS A SELF-INFLICTED OUTAGE.
   Airflow will happily launch every date at once. Seven hundred simultaneous
   warehouse loads will saturate the connection pool and take production down
   while you were trying to fix data. POOLS cap how many run concurrently.

3. WITHOUT LOGICAL DATES IT DOES NOTHING USEFUL.
   If tasks used now() instead of the logical date, backfilling March 2022
   would process today's data 700 times. Every task takes {{ ds }}.

CATCHUP=TRUE HERE, AND WHY IT IS FALSE ON THE EOD DAG
catchup tells Airflow to schedule every interval between start_date and now.
On the EOD DAG that is a footgun: unpause a DAG with a start_date six months
back and it immediately queues 180 runs. Backfill is the one place where that
behaviour is the entire point, so it lives in its own DAG where it is explicit
and expected rather than a surprise.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from meridian.conventions import (
    INGESTION_DIR,
    PIPELINE_ENV,
    PIPELINES_DIR,
    WAREHOUSE_DIR,
    default_args,
    no_retry_args,
)

with DAG(
    dag_id="historical_backfill",
    description="Reprocess historical dates with concurrency controls",
    schedule=None,  # triggered manually or via the CLI, never on a timer
    start_date=datetime(2024, 1, 1),
    catchup=True,
    # ONE AT A TIME. This is the single most important setting in this file.
    max_active_runs=1,
    default_args=default_args(retries=2),
    tags=["meridian", "backfill", "manual"],
    doc_md=__doc__,
) as dag:
    start = EmptyOperator(task_id="start")

    # ---------------------------------------------------------------------
    # depends_on_past=True on the load task.
    #
    # This says: do not run this task for 2024-03-15 unless the SAME task
    # succeeded for 2024-03-14. It enforces chronological order, which matters
    # here because SCD Type 2 history is built sequentially — processing March
    # before February would produce validity windows in the wrong order and
    # silently corrupt the customer history.
    #
    # The cost: one stuck day blocks every day after it. That is the correct
    # trade for anything building ordered history, and the wrong trade for
    # independent daily aggregates. Know which one you have.
    # ---------------------------------------------------------------------

    ingest = BashOperator(
        task_id="ingest_historical",
        bash_command=(
            f"cd {INGESTION_DIR} && "
            "python -m meridian_ingestion ingest "
            "--source ../meridian-data-generator/output/parquet "
            "--date {{ ds }} --no-watermark"
        ),
        env=PIPELINE_ENV,
        append_env=True,
        # Pools cap concurrency across ALL DAGs competing for a resource. Even
        # with max_active_runs=1 here, a pool protects the warehouse from this
        # backfill running alongside the nightly EOD job.
        pool="warehouse_pool",
        doc_md=(
            "--no-watermark because a backfill deliberately reprocesses dates "
            "already past the watermark. Without it the loader would skip them."
        ),
    )

    quality_gate = BashOperator(
        task_id="quality_gate",
        bash_command=(
            f"cd {PIPELINES_DIR} && "
            "python -m meridian_pipelines check "
            "--source-dir ../meridian-data-generator/output/parquet "
            "--run-id backfill_{{ ds_nodash }}"
        ),
        env=PIPELINE_ENV,
        append_env=True,
        **no_retry_args(),
        doc_md="The same gate applies to historical data. Backfills load bad data too.",
    )

    load = BashOperator(
        task_id="load_warehouse",
        bash_command=(
            f"cd {WAREHOUSE_DIR} && "
            "python -m meridian_warehouse load "
            "--source-dir ../meridian-data-generator/output/parquet "
            "--effective-date {{ ds }}"
        ),
        env=PIPELINE_ENV,
        append_env=True,
        pool="warehouse_pool",
        depends_on_past=True,  # chronological order for SCD2 correctness
        execution_timeout=timedelta(minutes=45),
    )

    end = EmptyOperator(task_id="end")

    start >> ingest >> quality_gate >> load >> end
