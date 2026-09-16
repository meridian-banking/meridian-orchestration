"""Analytics refresh: runs after the warehouse is loaded.

THE PROBLEM THIS SOLVES
Analytics must not run until the warehouse is loaded, but analytics and the EOD
pipeline are owned by different teams with different schedules. Putting them in
one DAG couples them: the analytics team cannot deploy without touching the
platform team's DAG, and one enormous DAG becomes unreadable and un-ownable.

THREE WAYS TO EXPRESS A CROSS-DAG DEPENDENCY, and when each fits:

1. ExternalTaskSensor — DAG B polls until a named task in DAG A succeeds.
   Works anywhere, but it is PULL-based and brittle: the sensor must know the
   other DAG's id, task id, AND schedule alignment. Change the upstream
   schedule and the sensor silently waits forever. Also occupies a worker slot
   while polling unless you use deferrable/reschedule mode.

2. TriggerDagRunOperator — DAG A explicitly triggers DAG B.
   Simple and immediate, but it inverts ownership: the upstream DAG now has to
   know about every consumer. Add a fifth downstream team and you edit the
   platform team's DAG again.

3. DATASETS (Airflow 2.4+) — DAG A declares it PRODUCES a dataset; DAG B
   declares it CONSUMES one. Airflow schedules B when A updates it.
   Neither DAG names the other. The dependency is on the DATA, which is what
   the dependency actually is. This is the modern answer and what we use.

Datasets also express intent more honestly: analytics does not depend on "the
EOD pipeline running", it depends on "the curated warehouse being fresh". If
that freshness came from a backfill instead, analytics should still run.
"""

from __future__ import annotations

from datetime import datetime

from airflow.datasets import Dataset
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from meridian.conventions import PIPELINE_ENV, WAREHOUSE_DIR, default_args

# The dataset is just a URI naming a logical resource. It does not have to be a
# real file path — it is an identifier both DAGs agree on.
CURATED_WAREHOUSE = Dataset("postgres://meridian/curated")

with DAG(
    dag_id="analytics_refresh",
    description="Refresh analytics aggregates after the warehouse updates",
    # SCHEDULED BY DATA, NOT BY TIME. There is no cron here. This DAG runs when
    # the curated warehouse dataset is updated, whichever DAG updated it.
    schedule=[CURATED_WAREHOUSE],
    start_date=datetime(2024, 3, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args(retries=2),
    tags=["meridian", "analytics", "dataset-triggered"],
    doc_md=__doc__,
) as dag:
    start = EmptyOperator(task_id="start")

    refresh_aggregates = BashOperator(
        task_id="refresh_aggregates",
        bash_command=(f"cd {WAREHOUSE_DIR} && python -m meridian_warehouse refresh-views"),
        env=PIPELINE_ENV,
        append_env=True,
        doc_md="Rebuilds monthly summary materialized views.",
    )

    dq_trend_report = BashOperator(
        task_id="dq_trend_report",
        bash_command=(
            'python -c "'
            "import os, psycopg2; "
            "c=psycopg2.connect(host=os.environ['WAREHOUSE_HOST'],"
            "port=os.environ['WAREHOUSE_PORT'],dbname=os.environ['WAREHOUSE_DB'],"
            "user=os.environ['WAREHOUSE_USER'],password=os.environ['WAREHOUSE_PASSWORD']); "
            "cur=c.cursor(); "
            'cur.execute(\\"SELECT dataset, check_name, rows_failed, executed_at \\"\\\n'
            '            \\"FROM dq.v_latest_check_status WHERE NOT passed\\"); '
            "rows=cur.fetchall(); "
            "print('failing checks:', len(rows)); "
            "[print(' ', r) for r in rows]\""
        ),
        env=PIPELINE_ENV,
        append_env=True,
        doc_md=(
            "Surfaces currently-failing quality checks. The value of storing "
            "check results is the TREND — a null rate creeping from 0.1% to "
            "1.2% over weeks never trips a daily threshold but is the story."
        ),
    )

    end = EmptyOperator(task_id="end")

    start >> [refresh_aggregates, dq_trend_report] >> end
