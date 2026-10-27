"""The orchestration engine: registration, triggering, execution, SLA, lineage.

State machine (per task and per run): queued -> running -> succeeded, or
running -> failed after retries are exhausted. When a task fails, every
downstream task that (transitively) depends on it is marked skipped — a
run never executes a task whose inputs did not materialize.

Determinism levers, all injectable for tests:
- ``now_fn`` supplies the clock (SLA checks, timestamps).
- ``sleeper`` is called with each retry backoff delay instead of sleeping
  (tests pass a recorder; production passes ``time.sleep``).
- the executor is pluggable (see :mod:`orchestrator.executor`).
"""

from __future__ import annotations

import itertools
import time
from datetime import datetime
from typing import Any, Callable, Iterable

from .alerting import AlertSink, InMemoryAlertSink
from .dag import topological_order, validate_dag
from .errors import UnknownDagError
from .executor import Executor, InProcessExecutor
from .models import (
    Alert,
    DagDefinition,
    DagRun,
    LineageEdge,
    RunStatus,
    TaskRun,
    TriggerType,
)
from .scheduling import CronSchedule, backfill_windows
from .store import RunStore


class Orchestrator:
    def __init__(
        self,
        executor: Executor | None = None,
        store: RunStore | None = None,
        alert_sink: AlertSink | None = None,
        now_fn: Callable[[], datetime] | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        self.executor = executor or InProcessExecutor()
        self.store = store or RunStore()
        self.alert_sink = alert_sink or InMemoryAlertSink()
        self.now_fn = now_fn or datetime.now
        self.sleeper = sleeper or time.sleep
        self._dags: dict[str, DagDefinition] = {}
        self._counter = itertools.count(1)

    # -- registration ----------------------------------------------------
    def register(self, dag: DagDefinition) -> DagDefinition:
        validate_dag(dag)
        if dag.schedule is not None:
            CronSchedule(dag.schedule)  # fail fast on a bad expression
        self._dags[dag.dag_id] = dag
        return dag

    def get_dag(self, dag_id: str) -> DagDefinition:
        try:
            return self._dags[dag_id]
        except KeyError as exc:
            raise UnknownDagError(f"no DAG registered as {dag_id!r}") from exc

    def list_dags(self) -> list[DagDefinition]:
        return list(self._dags.values())

    # -- triggering ------------------------------------------------------
    def trigger(
        self,
        dag_id: str,
        trigger: TriggerType = TriggerType.MANUAL,
        scheduled_for: datetime | None = None,
        initial_context: dict[str, Any] | None = None,
    ) -> DagRun:
        dag = self.get_dag(dag_id)
        now = self.now_fn()
        run = DagRun(
            run_id=f"{dag_id}-{next(self._counter)}",
            dag_id=dag_id,
            trigger=trigger,
            scheduled_for=scheduled_for or now,
            context=dict(initial_context or {}),
        )
        run.tasks = {t.task_id: TaskRun(task_id=t.task_id) for t in dag.tasks}
        run.status = RunStatus.RUNNING
        run.started_at = now
        self.store.save(run)
        self._execute(dag, run)
        return run

    def backfill(
        self, dag_id: str, start: datetime, end: datetime
    ) -> list[DagRun]:
        """Trigger one run per scheduled time in [start, end]."""
        dag = self.get_dag(dag_id)
        if dag.schedule is None:
            raise ValueError(f"DAG {dag_id!r} has no schedule to backfill")
        windows = backfill_windows(CronSchedule(dag.schedule), start, end)
        return [
            self.trigger(dag_id, TriggerType.BACKFILL, scheduled_for=w)
            for w in windows
        ]

    # -- SLA --------------------------------------------------------------
    def check_sla(self, run: DagRun) -> Alert | None:
        """Emit an SLA-breach alert if the run missed its deadline.

        Returns the alert, or None when on time / already alerted / no SLA.
        Safe to call repeatedly: a run gets at most one SLA alert.
        """
        dag = self.get_dag(run.dag_id)
        deadline = run.sla_deadline(dag)
        if deadline is None:
            return None
        if any(a.kind == "sla_breach" for a in run.alerts):
            return None
        now = self.now_fn()
        finished = run.finished_at or now
        if finished > deadline:
            alert = Alert(
                kind="sla_breach",
                dag_id=run.dag_id,
                run_id=run.run_id,
                task_id=None,
                message=(
                    f"run {run.run_id} missed its SLA deadline "
                    f"{deadline.isoformat()} (finished {finished.isoformat()})"
                ),
                created_at=now,
            )
            run.alerts.append(alert)
            self.alert_sink.send(alert)
            self.store.save(run)
            return alert
        return None

    # -- execution --------------------------------------------------------
    def _execute(self, dag: DagDefinition, run: DagRun) -> None:
        by_id = {t.task_id: t for t in dag.tasks}
        failed: set[str] = set()
        for task_id in topological_order(dag):
            task = by_id[task_id]
            task_run = run.tasks[task_id]
            if any(dep in failed for dep in task.depends_on):
                task_run.status = RunStatus.SKIPPED
                continue
            self._run_task(dag, run, task, task_run)
            if task_run.status == RunStatus.FAILED:
                failed.add(task_id)
        run.status = RunStatus.FAILED if failed else RunStatus.SUCCEEDED
        run.finished_at = self.now_fn()
        self._record_lineage(dag, run)
        self.check_sla(run)
        self.store.save(run)

    def _run_task(self, dag: DagDefinition, run: DagRun, task, task_run: TaskRun) -> None:
        task_run.status = RunStatus.RUNNING
        task_run.started_at = self.now_fn()
        max_attempts = task.retry.max_retries + 1
        while task_run.attempts < max_attempts:
            task_run.attempts += 1
            try:
                result = self.executor.execute(task.task_id, task.func, run.context)
            except Exception as exc:  # noqa: BLE001 - retries cover all failures
                if task_run.attempts < max_attempts:
                    delay = task.retry.delay_for_attempt(task_run.attempts)
                    task_run.retry_delays.append(delay)
                    self.sleeper(delay)
                    continue
                task_run.status = RunStatus.FAILED
                task_run.error = f"{type(exc).__name__}: {exc}"
                task_run.finished_at = self.now_fn()
                self._alert_failure(dag, run, task.task_id, task_run.error)
                return
            task_run.status = RunStatus.SUCCEEDED
            task_run.result = result
            task_run.finished_at = self.now_fn()
            return

    def _alert_failure(
        self, dag: DagDefinition, run: DagRun, task_id: str, error: str
    ) -> None:
        alert = Alert(
            kind="task_failed",
            dag_id=dag.dag_id,
            run_id=run.run_id,
            task_id=task_id,
            message=f"task {task_id!r} in run {run.run_id} failed: {error}",
            created_at=self.now_fn(),
        )
        run.alerts.append(alert)
        self.alert_sink.send(alert)

    def _record_lineage(self, dag: DagDefinition, run: DagRun) -> None:
        producer_of: dict[str, str] = {}
        for task in dag.tasks:
            for dataset in task.produces:
                producer_of[dataset] = task.task_id
        edges: list[LineageEdge] = []
        for task in dag.tasks:
            if run.tasks[task.task_id].status != RunStatus.SUCCEEDED:
                continue
            for dataset in task.consumes:
                producer = producer_of.get(dataset)
                if producer and producer != task.task_id:
                    edges.append(LineageEdge(producer, task.task_id, dataset))
        run.lineage = edges
