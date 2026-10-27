"""Command-line interface.

Usage:
    python -m orchestrator list
    python -m orchestrator trigger daily_orders
    python -m orchestrator run daily_orders --show
    python -m orchestrator backfill daily_orders 2026-10-24 2026-10-26
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime

from .dags import all_demo_dags
from .engine import Orchestrator
from .models import TriggerType


def build_orchestrator() -> Orchestrator:
    orch = Orchestrator(sleeper=lambda _delay: None)  # demo runs never sleep
    for dag in all_demo_dags():
        orch.register(dag)
    return orch


def _print_run(run) -> None:
    print(f"run {run.run_id} [{run.trigger.value}] -> {run.status.value}")
    for task_id, task_run in run.tasks.items():
        extra = f" attempts={task_run.attempts}"
        if task_run.retry_delays:
            extra += f" backoffs={task_run.retry_delays}"
        if task_run.error:
            extra += f" error={task_run.error}"
        print(f"  {task_id}: {task_run.status.value}{extra}")
    for edge in run.lineage:
        print(f"  lineage: {edge.producer_task} --{edge.dataset}--> {edge.consumer_task}")
    for alert in run.alerts:
        print(f"  alert [{alert.kind}]: {alert.message}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="orchestrator")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="list registered DAGs")
    trigger = sub.add_parser("trigger", help="trigger a DAG run")
    trigger.add_argument("dag_id")
    trigger.add_argument("--show", action="store_true")
    backfill = sub.add_parser("backfill", help="backfill a scheduled DAG")
    backfill.add_argument("dag_id")
    backfill.add_argument("start", help="YYYY-MM-DD")
    backfill.add_argument("end", help="YYYY-MM-DD")

    args = parser.parse_args(argv)
    orch = build_orchestrator()

    if args.command == "list":
        for dag in orch.list_dags():
            print(f"{dag.dag_id}  schedule={dag.schedule}  sla={dag.sla_seconds}s  {dag.description}")
        return 0
    if args.command == "trigger":
        run = orch.trigger(args.dag_id, TriggerType.MANUAL)
        _print_run(run)
        return 0 if run.status.value == "succeeded" else 1
    if args.command == "backfill":
        start = datetime.strptime(args.start, "%Y-%m-%d")
        end = datetime.strptime(args.end, "%Y-%m-%d")
        runs = orch.backfill(args.dag_id, start, end)
        for run in runs:
            _print_run(run)
        print(f"{len(runs)} backfill run(s)")
        return 0
    parser.print_help(sys.stderr)
    return 2
