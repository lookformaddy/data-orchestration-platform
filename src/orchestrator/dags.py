"""Built-in demo DAGs over synthetic data.

These exist so the CLI, the service, and the demo script all share one
definition. The data is invented (orders for a fictional online shop);
no real company data, schema, or pipeline is involved.
"""

from __future__ import annotations

from functools import partial
from typing import Any

from .models import DagDefinition, RetryPolicy, TaskDefinition
from .quality import check_null_rate, check_row_count, check_unique, make_quality_gate

ORDERS = "orders"
CLEAN_ORDERS = "clean_orders"


def extract_orders(context: dict[str, Any]) -> list[dict[str, Any]]:
    rows = [
        {"order_id": i, "customer_id": 100 + (i % 5), "amount": 10.0 * i, "region": r}
        for i, r in enumerate(
            ["north", "south", "east", "west", "north", "south", "east", "west"], start=1
        )
    ]
    context[ORDERS] = rows
    return rows


def transform_orders(context: dict[str, Any]) -> list[dict[str, Any]]:
    rows = [
        {**row, "amount_usd": round(row["amount"] * 1.0, 2)}
        for row in context[ORDERS]
    ]
    context[CLEAN_ORDERS] = rows
    return rows


def load_orders(context: dict[str, Any]) -> dict[str, Any]:
    rows = context[CLEAN_ORDERS]
    summary = {"rows_loaded": len(rows), "total_amount": sum(r["amount"] for r in rows)}
    context["load_summary"] = summary
    return summary


def flaky_extract_factory(fail_times: int = 2) -> Any:
    """A task that fails ``fail_times`` times, then succeeds — retry demo."""
    state = {"calls": 0}

    def flaky(context: dict[str, Any]) -> str:
        state["calls"] += 1
        if state["calls"] <= fail_times:
            raise RuntimeError(f"transient source outage (call {state['calls']})")
        context["flaky_ok"] = True
        return "recovered"

    return flaky


def demo_dag() -> DagDefinition:
    gate = make_quality_gate(
        ORDERS,
        [
            partial(check_row_count, minimum=5),
            partial(check_null_rate, column="customer_id", max_rate=0.0),
            partial(check_unique, column="order_id"),
        ],
    )
    return DagDefinition(
        dag_id="daily_orders",
        description="Synthetic daily orders pipeline: extract, quality gate, transform, load.",
        schedule="0 6 * * *",
        sla_seconds=3600,
        tags=("demo", "orders"),
        tasks=(
            TaskDefinition("extract", extract_orders, produces=(ORDERS,)),
            TaskDefinition("validate", gate, depends_on=("extract",), consumes=(ORDERS,)),
            TaskDefinition(
                "transform", transform_orders, depends_on=("validate",),
                consumes=(ORDERS,), produces=(CLEAN_ORDERS,),
            ),
            TaskDefinition(
                "load", load_orders, depends_on=("transform",),
                consumes=(CLEAN_ORDERS,),
            ),
        ),
    )


def retry_demo_dag() -> DagDefinition:
    return DagDefinition(
        dag_id="retry_demo",
        description="One flaky task that recovers on its third attempt.",
        tasks=(
            TaskDefinition(
                "flaky_extract", flaky_extract_factory(2),
                retry=RetryPolicy(max_retries=3, backoff_seconds=1.0),
            ),
        ),
    )


def all_demo_dags() -> list[DagDefinition]:
    return [demo_dag(), retry_demo_dag()]
