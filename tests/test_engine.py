from datetime import datetime

import pytest

from orchestrator import (
    DagDefinition,
    RetryPolicy,
    RunStatus,
    TaskDefinition,
    TriggerType,
    UnknownDagError,
)
from orchestrator.dags import demo_dag, retry_demo_dag

from conftest import failing_task, ok_task


def test_demo_dag_runs_end_to_end(orch):
    orch.register(demo_dag())
    run = orch.trigger("daily_orders", TriggerType.MANUAL)
    assert run.status == RunStatus.SUCCEEDED
    assert all(t.status == RunStatus.SUCCEEDED for t in run.tasks.values())
    assert run.context["load_summary"]["rows_loaded"] == 8
    assert run.context["load_summary"]["total_amount"] == 360.0


def test_retry_then_success(orch):
    orch.register(retry_demo_dag())
    run = orch.trigger("retry_demo")
    assert run.status == RunStatus.SUCCEEDED
    task_run = run.tasks["flaky_extract"]
    assert task_run.attempts == 3
    assert task_run.retry_delays == [1.0, 2.0]
    assert orch.recorded_sleeps == [1.0, 2.0]


def test_failure_after_retries_exhausted(orch):
    orch.register(DagDefinition(
        dag_id="always_fails",
        tasks=(TaskDefinition("bad", failing_task(), retry=RetryPolicy(max_retries=2)),),
    ))
    run = orch.trigger("always_fails")
    assert run.status == RunStatus.FAILED
    task_run = run.tasks["bad"]
    assert task_run.attempts == 3
    assert "boom" in (task_run.error or "")


def test_failed_task_skips_downstream(orch):
    orch.register(DagDefinition(
        dag_id="skip_demo",
        tasks=(
            TaskDefinition("bad", failing_task(), retry=RetryPolicy(max_retries=0)),
            TaskDefinition("down", ok_task("down"), depends_on=("bad",)),
            TaskDefinition("free", ok_task("free")),
        ),
    ))
    run = orch.trigger("skip_demo")
    assert run.tasks["bad"].status == RunStatus.FAILED
    assert run.tasks["down"].status == RunStatus.SKIPPED
    assert run.tasks["free"].status == RunStatus.SUCCEEDED
    assert "down" not in run.context
    assert run.status == RunStatus.FAILED


def test_quality_gate_failure_blocks_downstream(orch):
    from functools import partial

    from orchestrator import check_row_count, make_quality_gate

    gate = make_quality_gate("data", [partial(check_row_count, minimum=100)])

    def seed(context):
        context["data"] = [{"a": 1}]

    orch.register(DagDefinition(
        dag_id="gated",
        tasks=(
            TaskDefinition("seed", seed, produces=("data",)),
            TaskDefinition("gate", gate, depends_on=("seed",), consumes=("data",),
                           retry=RetryPolicy(max_retries=0)),
            TaskDefinition("use", ok_task("use"), depends_on=("gate",)),
        ),
    ))
    run = orch.trigger("gated")
    assert run.tasks["gate"].status == RunStatus.FAILED
    assert run.tasks["use"].status == RunStatus.SKIPPED
    assert run.status == RunStatus.FAILED


def test_unknown_dag_trigger_rejected(orch):
    with pytest.raises(UnknownDagError):
        orch.trigger("nope")


def test_backfill_creates_one_run_per_window(orch):
    orch.register(demo_dag())
    runs = orch.backfill("daily_orders", datetime(2026, 10, 24), datetime(2026, 10, 26, 23, 59))
    assert len(runs) == 3
    assert all(r.trigger == TriggerType.BACKFILL for r in runs)
    assert all(r.status == RunStatus.SUCCEEDED for r in runs)
    assert [r.scheduled_for.day for r in runs] == [24, 25, 26]


def test_backfill_without_schedule_rejected(orch):
    orch.register(DagDefinition(dag_id="manual_only", tasks=(TaskDefinition("a", ok_task("a")),)))
    with pytest.raises(ValueError):
        orch.backfill("manual_only", datetime(2026, 10, 24), datetime(2026, 10, 25))


def test_run_history_filtering(orch):
    orch.register(demo_dag())
    orch.trigger("daily_orders")
    orch.register(DagDefinition(
        dag_id="bad", tasks=(TaskDefinition("b", failing_task(), retry=RetryPolicy(max_retries=0)),)
    ))
    orch.trigger("bad")
    assert len(orch.store.list_runs()) == 2
    assert len(orch.store.list_runs(status=RunStatus.FAILED)) == 1
    assert len(orch.store.list_runs(dag_id="daily_orders")) == 1
