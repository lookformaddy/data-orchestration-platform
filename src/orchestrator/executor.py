"""Pluggable task execution.

The engine never calls task functions directly: it goes through an
:class:`Executor`. The in-process executor runs callables synchronously
and deterministically — ideal for tests, local runs, and demos. A
production deployment would implement the same protocol over a worker
pool, a container runner, or a remote task queue, without changing the
engine, the state machine, or the alerting logic.
"""

from __future__ import annotations

from typing import Any, Callable, Protocol


class Executor(Protocol):
    """Runs one task function with the run context; returns its result."""

    def execute(
        self, task_id: str, func: Callable[[dict[str, Any]], Any], context: dict[str, Any]
    ) -> Any: ...


class InProcessExecutor:
    """Synchronous, deterministic executor: calls the function in-process."""

    def execute(
        self, task_id: str, func: Callable[[dict[str, Any]], Any], context: dict[str, Any]
    ) -> Any:
        return func(context)


class RecordingExecutor(InProcessExecutor):
    """In-process executor that records execution order (for tests/demos)."""

    def __init__(self) -> None:
        self.executed: list[str] = []

    def execute(
        self, task_id: str, func: Callable[[dict[str, Any]], Any], context: dict[str, Any]
    ) -> Any:
        self.executed.append(task_id)
        return super().execute(task_id, func, context)
