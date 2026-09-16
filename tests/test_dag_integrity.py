"""DAG integrity tests.

These do not RUN the DAGs; they assert structural properties. In real teams
this class of test catches the overwhelming majority of "the scheduler is
showing an import error" incidents before merge, because the failure modes are
almost always structural: a typo, a missing owner, a retry policy nobody
noticed, a cycle introduced while refactoring.

The pattern is: encode your CONVENTIONS as tests. A convention that is only
written in a README is a suggestion; a convention with a test is a rule.
"""

from __future__ import annotations

import pytest

airflow = pytest.importorskip("airflow", reason="airflow not installed")

from airflow.models import DagBag  # noqa: E402

EXPECTED_DAGS = {"eod_pipeline", "historical_backfill", "analytics_refresh"}
CURATED_DATASET = "postgres://meridian/curated"


@pytest.fixture(scope="session")
def dag_bag() -> DagBag:
    return DagBag(dag_folder="dags", include_examples=False)


# --- the basics -------------------------------------------------------------


def test_no_import_errors(dag_bag):
    """The single highest-value test in any Airflow repo."""
    assert dag_bag.import_errors == {}, f"DAG import errors: {dag_bag.import_errors}"


def test_expected_dags_are_present(dag_bag):
    assert EXPECTED_DAGS.issubset(set(dag_bag.dag_ids))


# --- conventions, enforced --------------------------------------------------


def test_every_dag_has_a_real_owner(dag_bag):
    """'airflow' as an owner means nobody owns it, which is how alerts go
    unanswered for months."""
    for dag_id, dag in dag_bag.dags.items():
        owner = dag.default_args.get("owner")
        assert owner and owner != "airflow", f"{dag_id} has no real owner"


def test_every_dag_has_tags(dag_bag):
    """Tags are how anyone finds a DAG in a UI with hundreds of them."""
    for dag_id, dag in dag_bag.dags.items():
        assert dag.tags, f"{dag_id} has no tags"


def test_every_dag_has_documentation(dag_bag):
    """A DAG whose purpose is not written down becomes un-ownable the moment
    its author leaves."""
    for dag_id, dag in dag_bag.dags.items():
        assert dag.doc_md, f"{dag_id} has no doc_md"


def test_every_dag_limits_concurrent_runs(dag_bag):
    """Unbounded concurrent runs of a warehouse-writing DAG is a race
    condition waiting to produce non-reproducible results."""
    for dag_id, dag in dag_bag.dags.items():
        assert dag.max_active_runs > 0, f"{dag_id} does not limit concurrent runs"


def test_every_dag_has_a_failure_callback(dag_bag):
    """A failure nobody is told about is an outage that lasts until someone
    happens to look."""
    for dag_id, dag in dag_bag.dags.items():
        assert dag.default_args.get("on_failure_callback"), f"{dag_id} has no on_failure_callback"


def test_no_cycles(dag_bag):
    """The 'A' in DAG. A cycle means the graph can never complete.

    topological_sort() is the check: it is only possible on an acyclic graph,
    so it raises if a cycle exists. It also returns every task, which lets us
    assert nothing was silently dropped.
    """
    for dag_id, dag in dag_bag.dags.items():
        ordered = list(dag.topological_sort())
        assert len(ordered) == len(dag.tasks), f"{dag_id} lost tasks in sorting"


# --- retry policy: the transient vs permanent distinction -------------------


def test_quality_gate_does_not_retry(dag_bag):
    """THE key policy test.

    If data violates a quality rule, it will violate that rule again in five
    minutes. Retrying a permanent failure wastes time and delays the alert a
    human needs. Any future edit that adds retries here breaks this test, and
    that is the point.
    """
    for dag_id in ("eod_pipeline", "historical_backfill"):
        gate = dag_bag.dags[dag_id].get_task("quality_gate")
        assert (
            gate.retries == 0
        ), f"{dag_id}.quality_gate must not retry — DQ failures are permanent"


def test_infrastructure_tasks_do_retry(dag_bag):
    """The other half of the policy: transient failures SHOULD retry."""
    dag = dag_bag.dags["eod_pipeline"]
    for task_id in ("generate_source_data", "warehouse.load_dimensional_model"):
        assert (
            dag.get_task(task_id).retries >= 1
        ), f"{task_id} should retry — its failures are typically transient"


def test_retries_use_exponential_backoff(dag_bag):
    """Retrying every 5 minutes on the dot from every task is the traffic
    pattern that keeps a struggling service down."""
    for dag_id, dag in dag_bag.dags.items():
        if dag.default_args.get("retries", 0) > 0:
            assert dag.default_args.get(
                "retry_exponential_backoff"
            ), f"{dag_id} retries without backoff"


# --- the gate actually gates ------------------------------------------------


def test_warehouse_load_is_downstream_of_the_quality_gate(dag_bag):
    """The structural guarantee that bad data cannot reach the warehouse.

    If someone refactors the DAG and accidentally puts the load in parallel
    with the gate instead of after it, the gate silently stops gating and
    nothing else would notice.
    """
    dag = dag_bag.dags["eod_pipeline"]
    gate = dag.get_task("quality_gate")
    assert "warehouse.load_dimensional_model" in gate.downstream_task_ids


def test_gate_uses_all_success_trigger_rule(dag_bag):
    """all_success is what makes an upstream failure block downstream tasks.
    A trigger rule of all_done would run the load even after the gate failed —
    a one-word change that silently removes the entire control."""
    dag = dag_bag.dags["eod_pipeline"]
    load = dag.get_task("warehouse.load_dimensional_model")
    assert load.trigger_rule == "all_success"


# --- backfill safety --------------------------------------------------------


def test_backfill_is_serialised(dag_bag):
    """Backfilling 700 dates in parallel is a self-inflicted outage."""
    assert dag_bag.dags["historical_backfill"].max_active_runs == 1


def test_backfill_load_depends_on_past(dag_bag):
    """SCD2 history is built sequentially; processing March before February
    would produce validity windows in the wrong order."""
    load = dag_bag.dags["historical_backfill"].get_task("load_warehouse")
    assert load.depends_on_past is True


def test_backfill_uses_a_pool(dag_bag):
    """Pools cap concurrency across ALL DAGs competing for the warehouse, not
    just within this one."""
    load = dag_bag.dags["historical_backfill"].get_task("load_warehouse")
    assert load.pool == "warehouse_pool"


def test_eod_does_not_catch_up(dag_bag):
    """catchup=True on a scheduled DAG with an old start_date queues months of
    runs the instant it is unpaused. Backfill has its own DAG for that."""
    assert dag_bag.dags["eod_pipeline"].catchup is False


# --- cross-DAG dependency ---------------------------------------------------


def test_eod_produces_the_curated_dataset(dag_bag):
    task = dag_bag.dags["eod_pipeline"].get_task("warehouse.refresh_materialized_views")
    assert CURATED_DATASET in [d.uri for d in task.outlets]


def test_analytics_consumes_the_curated_dataset(dag_bag):
    """Dataset scheduling means neither DAG names the other — the dependency
    is on the data, which is what the dependency actually is."""
    dag = dag_bag.dags["analytics_refresh"]
    trigger = dag.dataset_triggers
    uris = [o.uri for o in trigger.objects]
    assert CURATED_DATASET in uris


def test_analytics_has_no_cron_schedule(dag_bag):
    """It runs when the data is fresh, not at a guessed time that hopefully
    falls after the upstream job finishes."""
    dag = dag_bag.dags["analytics_refresh"]
    assert dag.timetable.summary == "Dataset"


# --- timeouts ---------------------------------------------------------------


def test_long_running_tasks_have_timeouts(dag_bag):
    """A hung task is worse than a failed one: it holds a worker slot, blocks
    downstream work, and never fires a failure alert. An invisible outage."""
    dag = dag_bag.dags["eod_pipeline"]
    for task_id in (
        "generate_source_data",
        "quality_gate",
        "warehouse.load_dimensional_model",
    ):
        assert dag.get_task(task_id).execution_timeout is not None, f"{task_id} can hang forever"
