from datetime import datetime, timedelta

from orchestrator import (
    DagDefinition,
    InMemoryAlertSink,
    Orchestrator,
    RetryPolicy,
    RunStatus,
    TaskDefinition,
)
from orchestrator.dags import demo_dag

from conftest import FakeClock, failing_task, ok_task


def test_sla_breach_alert_fires_once():
    clock = FakeClock(datetime(2026, 10, 27, 6, 0))
    sink = InMemoryAlertSink()
    orch = Orchestrator(now_fn=clock, sleeper=lambda _d: None, alert_sink=sink)
    dag = demo_dag()  # SLA 3600s from scheduled_for
    orch.register(dag)
    run = orch.trigger("daily_orders", scheduled_for=datetime(2026, 10, 27, 6, 0))
    # Run "finishes" at 06:00 in fake time, so no breach yet; move the clock
    # past the deadline and re-check: the stored finished_at is 06:00, so
    # use a run whose finish is late instead — simulate by triggering with
    # a clock already past the deadline.
    assert sink.of_kind("sla_breach") == []

    clock2 = FakeClock(datetime(2026, 10, 27, 8, 30))
    sink2 = InMemoryAlertSink()
    orch2 = Orchestrator(now_fn=clock2, sleeper=lambda _d: None, alert_sink=sink2)
    orch2.register(dag)
    run2 = orch2.trigger("daily_orders", scheduled_for=datetime(2026, 10, 27, 6, 0))
    breaches = sink2.of_kind("sla_breach")
    assert len(breaches) == 1
    assert "SLA" in breaches[0].message
    # Re-checking does not duplicate.
    assert orch2.check_sla(run2) is None
    assert len(sink2.of_kind("sla_breach")) == 1


def test_failure_alert_emitted():
    sink = InMemoryAlertSink()
    orch = Orchestrator(sleeper=lambda _d: None, alert_sink=sink)
    orch.register(DagDefinition(
        dag_id="bad",
        tasks=(TaskDefinition("b", failing_task("disk full"), retry=RetryPolicy(max_retries=0)),),
    ))
    run = orch.trigger("bad")
    failures = sink.of_kind("task_failed")
    assert len(failures) == 1
    assert failures[0].task_id == "b"
    assert "disk full" in failures[0].message


def test_lineage_recorded_for_successful_run(orch):
    orch.register(demo_dag())
    run = orch.trigger("daily_orders")
    edges = {(e.producer_task, e.consumer_task, e.dataset) for e in run.lineage}
    assert ("extract", "validate", "orders") in edges
    assert ("transform", "load", "clean_orders") in edges


def test_lineage_excludes_failed_producer(orch):
    orch.register(DagDefinition(
        dag_id="partial",
        tasks=(
            TaskDefinition("bad", failing_task(), retry=RetryPolicy(max_retries=0),
                           produces=("x",)),
            TaskDefinition("use", ok_task("use"), depends_on=("bad",), consumes=("x",)),
        ),
    ))
    run = orch.trigger("partial")
    assert run.lineage == []
