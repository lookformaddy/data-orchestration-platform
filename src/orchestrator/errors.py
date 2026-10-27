"""Error types for the orchestration platform.

All failures are expressed through these types so callers (CLI, service,
tests) can distinguish a bad DAG definition from a runtime task failure.
"""

from __future__ import annotations


class OrchestrationError(Exception):
    """Base class for all orchestration errors."""


class DagDefinitionError(OrchestrationError):
    """A DAG definition is invalid (bad structure, bad schedule, ...)."""


class CycleError(DagDefinitionError):
    """Task dependencies contain a cycle."""


class UnknownTaskError(DagDefinitionError):
    """A dependency refers to a task that does not exist in the DAG."""


class UnknownDagError(OrchestrationError):
    """No DAG with the requested name is registered."""


class DuplicateTaskError(DagDefinitionError):
    """Two tasks in one DAG share a task id."""


class InvalidCronError(DagDefinitionError):
    """A cron-like schedule expression cannot be parsed."""


class TaskExecutionError(OrchestrationError):
    """A task function raised during execution.

    Carries the task id and the attempt number so the engine can decide
    whether to retry or fail the run.
    """

    def __init__(self, task_id: str, attempt: int, cause: BaseException) -> None:
        super().__init__(f"task {task_id!r} failed on attempt {attempt}: {cause}")
        self.task_id = task_id
        self.attempt = attempt
        self.cause = cause
