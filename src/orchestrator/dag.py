"""DAG validation and topological ordering.

Validation runs at registration time, not at run time: a DAG with a cycle
or a dangling dependency can never be scheduled, and the operator learns
that immediately instead of at 3 a.m. when the run fires.
"""

from __future__ import annotations

from collections import deque

from .errors import CycleError, DuplicateTaskError, UnknownTaskError
from .models import DagDefinition


def validate_dag(dag: DagDefinition) -> None:
    """Raise on duplicate ids, unknown dependencies, or cycles."""
    seen: set[str] = set()
    for task in dag.tasks:
        if task.task_id in seen:
            raise DuplicateTaskError(f"duplicate task id {task.task_id!r} in DAG {dag.dag_id!r}")
        seen.add(task.task_id)
    for task in dag.tasks:
        for dep in task.depends_on:
            if dep not in seen:
                raise UnknownTaskError(
                    f"task {task.task_id!r} depends on unknown task {dep!r}"
                )
    # Cycle detection via Kahn's algorithm: if we cannot order every task,
    # a cycle exists.
    topological_order(dag)


def topological_order(dag: DagDefinition) -> list[str]:
    """Return task ids in an order where every dependency precedes its consumer.

    Raises CycleError when the graph is cyclic. Ties are broken by the
    order tasks were declared, so ordering is deterministic.
    """
    by_id = {t.task_id: t for t in dag.tasks}
    in_degree = {t.task_id: 0 for t in dag.tasks}
    dependents: dict[str, list[str]] = {t.task_id: [] for t in dag.tasks}
    for task in dag.tasks:
        for dep in task.depends_on:
            in_degree[task.task_id] += 1
            dependents[dep].append(task.task_id)
    ready = deque(t.task_id for t in dag.tasks if in_degree[t.task_id] == 0)
    order: list[str] = []
    while ready:
        current = ready.popleft()
        order.append(current)
        for nxt in dependents[current]:
            in_degree[nxt] -= 1
            if in_degree[nxt] == 0:
                ready.append(nxt)
    if len(order) != len(dag.tasks):
        raise CycleError(f"DAG {dag.dag_id!r} contains a dependency cycle")
    return order


def execution_levels(dag: DagDefinition) -> list[list[str]]:
    """Group tasks into levels that may run in parallel.

    Level 0 holds tasks with no dependencies; each later level holds tasks
    whose dependencies are all in earlier levels.
    """
    order = topological_order(dag)
    by_id = {t.task_id: t for t in dag.tasks}
    level_of: dict[str, int] = {}
    for task_id in order:
        deps = by_id[task_id].depends_on
        level_of[task_id] = 0 if not deps else 1 + max(level_of[d] for d in deps)
    levels: dict[int, list[str]] = {}
    for task_id in order:
        levels.setdefault(level_of[task_id], []).append(task_id)
    return [levels[i] for i in sorted(levels)]
