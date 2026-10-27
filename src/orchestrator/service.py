"""Optional FastAPI service over the engine.

FastAPI is an optional dependency: if it is not installed,
:func:`create_app` raises a clear ImportError and the core (CLI, engine,
tests) still works. The test suite skips the service tests in that case.
"""

from __future__ import annotations

from typing import Any

try:
    from fastapi import FastAPI, HTTPException
    from pydantic import BaseModel
except ImportError as exc:  # pragma: no cover - exercised via skip
    raise ImportError(
        "FastAPI is required for the service layer; "
        "install it with: pip install -r requirements.txt"
    ) from exc

from .cli import build_orchestrator
from .models import RunStatus, TriggerType


class TriggerRequest(BaseModel):
    scheduled_for: str | None = None


def _run_view(run) -> dict[str, Any]:
    return {
        "run_id": run.run_id,
        "dag_id": run.dag_id,
        "trigger": run.trigger.value,
        "status": run.status.value,
        "tasks": {
            tid: {
                "status": tr.status.value,
                "attempts": tr.attempts,
                "error": tr.error,
            }
            for tid, tr in run.tasks.items()
        },
        "lineage": [
            {"producer": e.producer_task, "consumer": e.consumer_task, "dataset": e.dataset}
            for e in run.lineage
        ],
        "alerts": [{"kind": a.kind, "message": a.message} for a in run.alerts],
    }


def create_app() -> FastAPI:
    orch = build_orchestrator()
    app = FastAPI(title="data-orchestration-platform")

    @app.get("/dags")
    def list_dags() -> list[dict[str, Any]]:
        return [
            {
                "dag_id": d.dag_id,
                "schedule": d.schedule,
                "sla_seconds": d.sla_seconds,
                "description": d.description,
            }
            for d in orch.list_dags()
        ]

    @app.post("/dags/{dag_id}/trigger")
    def trigger_dag(dag_id: str, body: TriggerRequest | None = None) -> dict[str, Any]:
        try:
            from datetime import datetime

            scheduled = (
                datetime.fromisoformat(body.scheduled_for)
                if body and body.scheduled_for
                else None
            )
            run = orch.trigger(dag_id, TriggerType.MANUAL, scheduled_for=scheduled)
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return _run_view(run)

    @app.get("/runs/{run_id}")
    def get_run(run_id: str) -> dict[str, Any]:
        run = orch.store.get(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail=f"unknown run {run_id!r}")
        return _run_view(run)

    @app.get("/runs")
    def list_runs(dag_id: str | None = None, status: str | None = None) -> list[dict[str, Any]]:
        parsed = RunStatus(status) if status else None
        return [_run_view(r) for r in orch.store.list_runs(dag_id, parsed)]

    return app
