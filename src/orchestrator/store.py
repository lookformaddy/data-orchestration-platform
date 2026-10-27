"""Run history: an in-memory store with a replaceable interface.

The store is deliberately tiny — dict-backed, insertion-ordered — because
run history in this project is a record-keeping concern, not a storage
benchmark. Swap it for a database-backed implementation behind the same
methods when durability matters.
"""

from __future__ import annotations

from .models import DagRun, RunStatus


class RunStore:
    def __init__(self) -> None:
        self._runs: dict[str, DagRun] = {}

    def save(self, run: DagRun) -> None:
        self._runs[run.run_id] = run

    def get(self, run_id: str) -> DagRun | None:
        return self._runs.get(run_id)

    def list_runs(
        self, dag_id: str | None = None, status: RunStatus | None = None
    ) -> list[DagRun]:
        runs = list(self._runs.values())
        if dag_id is not None:
            runs = [r for r in runs if r.dag_id == dag_id]
        if status is not None:
            runs = [r for r in runs if r.status == status]
        return runs
