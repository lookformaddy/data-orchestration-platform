"""Core data model: DAGs, tasks, runs, retries, alerts, lineage.

Everything here is plain typed dataclasses — no framework imports — so the
core can be unit-tested, embedded, or driven by any service layer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Mapping, Sequence


class RunStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"


class TriggerType(str, Enum):
    MANUAL = "manual"
    SCHEDULED = "scheduled"
    BACKFILL = "backfill"


@dataclass(frozen=True)
class RetryPolicy:
    """How a failed task is retried.

    ``backoff_seconds`` is the base delay; each retry multiplies it by
    ``backoff_factor`` up to ``max_backoff_seconds``. Jitter is off by
    default so runs are deterministic and tests are repeatable.
    """

    max_retries: int = 3
    backoff_seconds: float = 1.0
    backoff_factor: float = 2.0
    max_backoff_seconds: float = 300.0

    def delay_for_attempt(self, attempt: int) -> float:
        """Delay before retry number ``attempt`` (1-based)."""
        if attempt < 1:
            raise ValueError("attempt must be >= 1")
        delay = self.backoff_seconds * (self.backoff_factor ** (attempt - 1))
        return min(delay, self.max_backoff_seconds)


@dataclass(frozen=True)
class TaskDefinition:
    """One node of a DAG.

    ``func`` is a callable taking a mutable context dict and returning any
    JSON-able result (or None). ``produces`` / ``consumes`` are logical
    dataset names used only for lineage — the engine never moves data.
    """

    task_id: str
    func: Callable[[dict[str, Any]], Any]
    depends_on: tuple[str, ...] = ()
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    produces: tuple[str, ...] = ()
    consumes: tuple[str, ...] = ()
    description: str = ""


@dataclass(frozen=True)
class DagDefinition:
    """A named, schedulable pipeline."""

    dag_id: str
    tasks: tuple[TaskDefinition, ...]
    schedule: str | None = None  # cron-like, e.g. "0 6 * * *" or None (manual only)
    sla_seconds: float | None = None  # deadline after scheduled/manual trigger time
    description: str = ""
    tags: tuple[str, ...] = ()


@dataclass
class TaskRun:
    task_id: str
    status: RunStatus = RunStatus.QUEUED
    attempts: int = 0
    retry_delays: list[float] = field(default_factory=list)
    result: Any = None
    error: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None


@dataclass
class LineageEdge:
    producer_task: str
    consumer_task: str
    dataset: str


@dataclass
class Alert:
    kind: str  # "task_failed" | "sla_breach"
    dag_id: str
    run_id: str
    task_id: str | None
    message: str
    created_at: datetime


@dataclass
class DagRun:
    run_id: str
    dag_id: str
    trigger: TriggerType
    scheduled_for: datetime
    status: RunStatus = RunStatus.QUEUED
    tasks: dict[str, TaskRun] = field(default_factory=dict)
    lineage: list[LineageEdge] = field(default_factory=list)
    alerts: list[Alert] = field(default_factory=list)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    context: dict[str, Any] = field(default_factory=dict)

    def sla_deadline(self, dag: DagDefinition) -> datetime | None:
        if dag.sla_seconds is None:
            return None
        from datetime import timedelta

        return self.scheduled_for + timedelta(seconds=dag.sla_seconds)
