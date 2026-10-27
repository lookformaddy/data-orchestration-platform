from datetime import datetime

import pytest

from orchestrator import (
    CycleError,
    DagDefinition,
    DuplicateTaskError,
    TaskDefinition,
    UnknownTaskError,
    execution_levels,
    topological_order,
    validate_dag,
)
from orchestrator.dags import demo_dag

from conftest import ok_task


def _dag(tasks):
    return DagDefinition(dag_id="d", tasks=tuple(tasks))


def test_topological_order_respects_dependencies():
    dag = _dag([
        TaskDefinition("c", ok_task("c"), depends_on=("a", "b")),
        TaskDefinition("a", ok_task("a")),
        TaskDefinition("b", ok_task("b"), depends_on=("a",)),
    ])
    order = topological_order(dag)
    assert order.index("a") < order.index("b") < order.index("c")


def test_cycle_is_rejected():
    dag = _dag([
        TaskDefinition("a", ok_task("a"), depends_on=("b",)),
        TaskDefinition("b", ok_task("b"), depends_on=("a",)),
    ])
    with pytest.raises(CycleError):
        validate_dag(dag)


def test_self_dependency_is_a_cycle():
    dag = _dag([TaskDefinition("a", ok_task("a"), depends_on=("a",))])
    with pytest.raises(CycleError):
        validate_dag(dag)


def test_unknown_dependency_is_rejected():
    dag = _dag([TaskDefinition("a", ok_task("a"), depends_on=("ghost",))])
    with pytest.raises(UnknownTaskError):
        validate_dag(dag)


def test_duplicate_task_id_is_rejected():
    dag = _dag([TaskDefinition("a", ok_task("a")), TaskDefinition("a", ok_task("a"))])
    with pytest.raises(DuplicateTaskError):
        validate_dag(dag)


def test_execution_levels_group_independents():
    dag = _dag([
        TaskDefinition("a", ok_task("a")),
        TaskDefinition("b", ok_task("b")),
        TaskDefinition("c", ok_task("c"), depends_on=("a", "b")),
    ])
    assert execution_levels(dag) == [["a", "b"], ["c"]]


def test_demo_dag_validates_and_orders():
    dag = demo_dag()
    validate_dag(dag)
    order = topological_order(dag)
    assert order == ["extract", "validate", "transform", "load"]
