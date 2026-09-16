"""End-of-day pipeline: the DAG that runs the whole platform.

    generate -> ingest -> stage -> [DQ GATE] -> load warehouse -> refresh -> reconcile

THE GATE IS THE POINT OF THIS DAG.
Sprint 5 built a data-quality command whose exit code means something: 0 to
proceed, 1 to stop. Here that exit code becomes load-bearing. If the gate fails,
Airflow marks the task failed and — because downstream tasks default to
`all_success` — the warehouse load simply never runs. Not "runs and logs a
warning". Does not run.

That is the difference between a quality report and a quality CONTROL.

WHY "END OF DAY"?
Banks run on batch windows. Card networks settle overnight, ledgers close, and
downstream reporting expects yesterday's complete picture by morning. That is
why bank data is typically "T+1" — you are always reporting on yesterday, and
the overnight window is when the work happens. Our 02:00 UTC schedule is a
miniature of exactly that.

WHY logical_date AND NOT datetime.now()?
This is the single most important idea in Airflow, and the most common
beginner mistake. Every DAG run has a LOGICAL DATE — the date the run is FOR,
not the wall-clock time it happens to execute at. Using now() means:
  - a backfill of March 2022 would process TODAY's data, 700 times
  - a run that retries at 00:03 after failing at 23:58 would process a
    different day than it started with
  - the run is not reproducible: running it twice gives different results
Every task below takes {{ ds }} — the logical date — precisely so a run for
2024-03-15 processes 2024-03-15 regardless of when it actually executes.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.datasets import Dataset
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from meridian.conventions import (
    GENERATOR_DIR,
    INGESTION_DIR,
    PIPELINE_ENV,
    PIPELINES_DIR,
    WAREHOUSE_DIR,
    alert_on_sla_miss,
    default_args,
    no_retry_args,
)

# Declared here and consumed by analytics_refresh. Neither DAG names the
# other — the dependency is on the DATA, which is what the dependency actually
# is. See analytics_refresh.py for why datasets beat sensors and triggers.
CURATED_WAREHOUSE = Dataset("postgres://meridian/curated")

with DAG(
    dag_id="eod_pipeline",
    description="End-of-day: generate, ingest, quality-gate, load, reconcile",
    schedule="0 2 * * *",  # 02:00 UTC daily — the overnight batch window
    start_date=datetime(2024, 3, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args(),
    tags=["meridian", "eod", "production"],
    doc_md=__doc__,
) as dag:
    # ---------------------------------------------------------------------
    # max_active_runs=1 deserves a comment of its own.
    #
    # Without it, a backfill would launch every historical date SIMULTANEOUSLY.
    # Two runs writing to the same warehouse tables at once is a race condition
    # that produces different results depending on which finishes first — the
    # worst class of bug, because it is intermittent and unreproducible.
    # Serialising runs costs wall-clock time and buys correctness.
    # ---------------------------------------------------------------------

    start = EmptyOperator(task_id="start")

    # =====================================================================
    # 1. SOURCE — produce the day's data
    # =====================================================================
    # In a real bank this task would not exist; source systems produce data
    # whether or not you ask. It stands in for "the upstream extract landed".
    generate = BashOperator(
        task_id="generate_source_data",
        bash_command=(
            f"cd {GENERATOR_DIR} && "
            "python -m meridian_datagen "
            "--config configs/small.yaml --format parquet --out ./output"
        ),
        env=PIPELINE_ENV,
        append_env=True,
        execution_timeout=timedelta(minutes=30),
        doc_md="Generates the day's synthetic source extracts.",
    )

    # =====================================================================
    # 2. INGEST — land it in the lake
    # =====================================================================
    with TaskGroup(group_id="ingest") as ingest_group:
        # TaskGroups are purely visual: they collapse related tasks into one
        # box in the UI. On a DAG with 40 tasks that is the difference between
        # a readable graph and spaghetti. They do NOT change execution.

        land_raw = BashOperator(
            task_id="land_raw",
            bash_command=(
                f"cd {INGESTION_DIR} && "
                "python -m meridian_ingestion ingest "
                f"--source ../meridian-data-generator/output/parquet "
                "--date {{ ds }}"
            ),
            env=PIPELINE_ENV,
            append_env=True,
            doc_md="Validates against contracts and lands files in the raw zone.",
        )

        promote_staged = BashOperator(
            task_id="promote_staged",
            bash_command=(
                f"cd {INGESTION_DIR} && " "python -m meridian_ingestion stage --date {{ ds }}"
            ),
            env=PIPELINE_ENV,
            append_env=True,
            doc_md="Converts raw files to typed Parquet in the staged zone.",
        )

        land_raw >> promote_staged

    # =====================================================================
    # 3. THE QUALITY GATE — the reason this DAG exists
    # =====================================================================
    # retries=0 is deliberate and is the whole philosophy in one argument.
    # If the data violates a quality rule, it will violate that rule again in
    # five minutes. Retrying a PERMANENT failure wastes time and, worse, delays
    # the alert a human needs to act on.
    quality_gate = BashOperator(
        task_id="quality_gate",
        bash_command=(
            f"cd {PIPELINES_DIR} && "
            "python -m meridian_pipelines check "
            "--source-dir ../meridian-data-generator/output/parquet "
            "--run-id eod_{{ ds_nodash }}_{{ ti.try_number }}"
        ),
        env=PIPELINE_ENV,
        append_env=True,
        **no_retry_args(execution_timeout=timedelta(minutes=20)),
        doc_md=(
            "**Blocking data-quality gate.** Exit code 1 fails this task, and "
            "because downstream tasks require all_success, the warehouse load "
            "will not run. Bad data does not reach the warehouse."
        ),
    )

    # =====================================================================
    # 4. WAREHOUSE LOAD — only reachable if the gate passed
    # =====================================================================
    with TaskGroup(group_id="warehouse") as warehouse_group:
        load_warehouse = BashOperator(
            task_id="load_dimensional_model",
            bash_command=(
                f"cd {WAREHOUSE_DIR} && "
                "python -m meridian_warehouse load "
                "--source-dir ../meridian-data-generator/output/parquet "
                "--effective-date {{ ds }}"
            ),
            env=PIPELINE_ENV,
            append_env=True,
            execution_timeout=timedelta(minutes=45),
            doc_md=(
                "SCD2 dimension merge and fact load. Idempotent: re-running "
                "the same logical date does not duplicate rows, which is what "
                "makes retries and backfills safe."
            ),
        )

        refresh_views = BashOperator(
            task_id="refresh_materialized_views",
            bash_command=(f"cd {WAREHOUSE_DIR} && python -m meridian_warehouse refresh-views"),
            env=PIPELINE_ENV,
            append_env=True,
            # PRODUCING the dataset. When this task succeeds, Airflow marks the
            # dataset updated, which schedules every DAG that consumes it.
            outlets=[CURATED_WAREHOUSE],
            doc_md="REFRESH MATERIALIZED VIEW CONCURRENTLY so readers are not blocked.",
        )

        load_warehouse >> refresh_views

    # =====================================================================
    # 5. RECONCILE — prove the warehouse agrees with the source
    # =====================================================================
    # SLA is set here rather than on earlier tasks because this is the task
    # whose lateness actually matters to a human: it is the last one, so if it
    # is late, the whole overnight batch is late and the morning reports will
    # not be ready. Putting an SLA on every task generates noise; putting one
    # on the task that represents "the batch is done" generates signal.
    reconcile = BashOperator(
        task_id="reconcile",
        bash_command=(
            f"cd {PIPELINES_DIR} && "
            "python -m meridian_pipelines reconcile "
            "--source-dir ../meridian-data-generator/output/parquet "
            "--date {{ ds }}"
        ),
        env=PIPELINE_ENV,
        append_env=True,
        sla=timedelta(hours=3),
        doc_md=(
            "Compares source and warehouse totals. An unexplained break means "
            "rows were lost or duplicated somewhere in the pipeline."
        ),
    )

    end = EmptyOperator(task_id="end")

    # ---------------------------------------------------------------------
    # THE DEPENDENCY CHAIN.
    # Read the >> operator as "must finish successfully before".
    # The default trigger rule is all_success, which is what makes the gate
    # actually gate: quality_gate failing means warehouse_group never starts.
    # ---------------------------------------------------------------------
    start >> generate >> ingest_group >> quality_gate >> warehouse_group >> reconcile >> end

    dag.sla_miss_callback = alert_on_sla_miss
