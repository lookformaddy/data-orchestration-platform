"""Demo: run both built-in DAGs and print the outcome.

Run from the project root:
    PYTHONPATH=src python examples/run_demo.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from orchestrator.cli import build_orchestrator  # noqa: E402
from orchestrator.models import TriggerType  # noqa: E402


def main() -> int:
    orch = build_orchestrator()
    for dag_id in ("daily_orders", "retry_demo"):
        run = orch.trigger(dag_id, TriggerType.MANUAL)
        print(f"{dag_id}: run {run.run_id} -> {run.status.value}")
        for task_id, task_run in run.tasks.items():
            print(f"  {task_id}: {task_run.status.value} (attempts={task_run.attempts})")
        for edge in run.lineage:
            print(f"  lineage: {edge.producer_task} --{edge.dataset}--> {edge.consumer_task}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
