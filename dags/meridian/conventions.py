"""Shared conventions for every Meridian DAG.

WHY A CONVENTIONS MODULE RATHER THAN COPY-PASTING default_args?
Because the decisions encoded here — how many retries, who owns this, what
counts as an SLA breach — are POLICY, and policy that lives in twelve places
drifts into twelve different policies. One module means changing the retry
strategy is a single edit, and it means the DAG integrity tests from Sprint 0
have something concrete to assert against.

THE RETRY PHILOSOPHY, which is the real content here:

Not all failures are equal, and treating them identically is how teams end up
either retrying things that can never succeed or giving up on things that
would have worked on the second attempt.

  TRANSIENT failures — a database connection blipped, S3 returned a 503, a
  worker was evicted. Nothing is wrong with the data or the code. RETRY, with
  exponential backoff so a struggling downstream service gets breathing room
  rather than a thundering herd.

  PERMANENT failures — the data violates a quality rule, a required file is
  absent, a SQL statement is invalid. Retrying changes nothing except the
  timestamp on the failure. FAIL FAST and alert a human.

Airflow cannot tell these apart on its own, so we encode the distinction:
infrastructure-touching tasks get generous retries; the data-quality gate gets
ZERO, because bad data will still be bad in five minutes.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

logger = logging.getLogger(__name__)

# Every DAG is owned by a team that can be paged. "airflow" as an owner means
# nobody owns it, which is how alerts go unanswered for months.
OWNER = "data-platform"

# Repository paths inside the Airflow container. Mounted read-only by compose.
REPO_ROOT = "/opt/meridian"
GENERATOR_DIR = f"{REPO_ROOT}/meridian-data-generator"
INGESTION_DIR = f"{REPO_ROOT}/meridian-ingestion"
PIPELINES_DIR = f"{REPO_ROOT}/meridian-pipelines"
WAREHOUSE_DIR = f"{REPO_ROOT}/meridian-warehouse"


def alert_on_failure(context: dict[str, Any]) -> None:
    """Called by Airflow when a task fails.

    In production this posts to Slack or PagerDuty. Locally it logs a
    structured message — the SHAPE is what matters, and the shape is: say
    which task, in which DAG, for which logical date, and link to the log.
    An alert that does not include those four things sends the responder
    hunting before they can even start.
    """
    ti = context.get("task_instance")
    logger.error(
        "ALERT task_failed dag=%s task=%s logical_date=%s try=%s log_url=%s",
        context.get("dag").dag_id if context.get("dag") else "?",
        ti.task_id if ti else "?",
        context.get("logical_date"),
        ti.try_number if ti else "?",
        ti.log_url if ti else "?",
    )


def alert_on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """Called when a task misses its SLA.

    AN SLA MISS IS NOT A FAILURE. The task may still be running and may still
    succeed. It means "this is late enough that someone should know", which is
    a genuinely different signal from "this broke". Conflating the two trains
    people to ignore both.
    """
    logger.warning("ALERT sla_missed dag=%s tasks=%s", dag.dag_id if dag else "?", task_list)


def default_args(
    retries: int = 3,
    retry_delay_minutes: int = 5,
    execution_timeout_minutes: int | None = 60,
) -> dict[str, Any]:
    """Standard default_args for a Meridian DAG.

    EXPONENTIAL BACKOFF (retry_exponential_backoff=True) matters more than it
    looks: if a downstream service is struggling, retrying every 5 minutes on
    the dot from every task is precisely the traffic pattern that keeps it
    down. Backoff spaces attempts out — 5, 10, 20 minutes — giving it room to
    recover.

    EXECUTION TIMEOUT matters because a task that hangs forever is worse than
    a task that fails: it holds a worker slot, blocks downstream work, and
    never triggers a failure alert. A hung task is an invisible outage.
    """
    args: dict[str, Any] = {
        "owner": OWNER,
        "depends_on_past": False,
        "retries": retries,
        "retry_delay": timedelta(minutes=retry_delay_minutes),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
        "on_failure_callback": alert_on_failure,
    }
    if execution_timeout_minutes is not None:
        args["execution_timeout"] = timedelta(minutes=execution_timeout_minutes)
    return args


def no_retry_args(**overrides: Any) -> dict[str, Any]:
    """default_args for tasks where retrying is pointless.

    Used by the data-quality gate. If the data violates a rule, it will still
    violate that rule on attempt four. Retrying a permanent failure wastes
    fifteen minutes and, worse, delays the alert that a human needs to see.
    """
    args = default_args(retries=0)
    args.update(overrides)
    return args


# Environment passed to every shell task. Airflow itself holds these as
# Connections/Variables in production; here they come from the container
# environment set up back in Sprint 0's docker-compose.
PIPELINE_ENV = {
    "WAREHOUSE_HOST": "{{ var.value.get('warehouse_host', 'warehouse-db') }}",
    "WAREHOUSE_PORT": "{{ var.value.get('warehouse_port', '5432') }}",
    "WAREHOUSE_DB": "{{ var.value.get('warehouse_db', 'meridian') }}",
    "WAREHOUSE_USER": "{{ var.value.get('warehouse_user', 'meridian') }}",
    "WAREHOUSE_PASSWORD": "{{ var.value.get('warehouse_password', '') }}",
    "MINIO_ENDPOINT": "{{ var.value.get('minio_endpoint', 'http://minio:9000') }}",
    "MINIO_ACCESS_KEY": "{{ var.value.get('minio_access_key', 'meridian') }}",
    "MINIO_SECRET_KEY": "{{ var.value.get('minio_secret_key', '') }}",
}
