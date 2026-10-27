# Data Orchestration Platform

Batch data pipeline orchestration: define pipelines as DAGs, schedule them,
retry failed tasks with backoff, enforce data-quality gates, watch SLAs,
backfill missed windows, trace lineage, and alert on failure — with a
deterministic, dependency-free core.

## The enterprise problem

Data teams run dozens of batch pipelines that must finish before the
business day starts, in the right order, on good data — and someone must
know the moment one does not. Ad-hoc cron scripts give you none of this:
no dependency ordering, no retry policy, no SLA, no record of what ran,
and no gate stopping bad data from flowing downstream. This project is a
small, readable implementation of the orchestration layer that fixes all
of that, designed the way a production scheduler is designed, minus the
distributed-systems weight.

## Architecture overview

```
DAG definition (tasks + dependencies + schedule + SLA)
        │  validated at registration (cycles, dangling deps, bad cron)
        ▼
Orchestrator ──► topological execution (deps first, level by level)
        │            │  retry with exponential backoff (injectable sleeper)
        │            │  quality gates are ordinary tasks: fail ⇒ downstream skipped
        │            ▼
        ├──► RunStore (run history, filterable by DAG / status)
        ├──► lineage edges (producer task → consumer task per dataset)
        └──► AlertSink (task_failed / sla_breach; in-memory by default)
```

Modules under `src/orchestrator/`:

| Module | Responsibility |
|---|---|
| `models.py` | Typed dataclasses: DAGs, tasks, runs, retry policy, alerts, lineage |
| `dag.py` | Validation (cycles, duplicates, unknown deps) + topological order + levels |
| `scheduling.py` | Cron-like parser (5 fields), next-run, backfill windows |
| `engine.py` | Registration, triggering, execution state machine, SLA checks, lineage |
| `executor.py` | Pluggable execution; in-process executor for tests/demos |
| `quality.py` | Data-quality checks and gate task factory |
| `store.py` / `alerting.py` | In-memory run history and alert sinks |
| `dags.py` | Built-in synthetic demo DAGs |
| `cli.py` / `service.py` | CLI and optional FastAPI service |

See `docs/architecture.md` for the full design, state machine, and
extension points.

## How to run

```bash
python -m venv /tmp/orch-venv && source /tmp/orch-venv/bin/activate
pip install -r requirements.txt        # FastAPI optional; core needs nothing
PYTHONPATH=src python examples/run_demo.py
PYTHONPATH=src python -m orchestrator list
PYTHONPATH=src python -m orchestrator trigger daily_orders
PYTHONPATH=src python -m orchestrator backfill daily_orders 2026-10-24 2026-10-26
PYTHONPATH=src python -m pytest -q     # full test suite
# Service (requires FastAPI):
PYTHONPATH=src uvicorn orchestrator.service:create_app --factory --port 8000
```

Or with Docker: `docker build -t data-orchestration-platform .`

## Design decisions and trade-offs

- **Pure-stdlib core.** Scheduling, retries, state, and lineage use no
  third-party code, so the core is trivially embeddable and testable.
  FastAPI is an optional service layer, not a core dependency; if it is
  absent, the service tests skip and everything else still runs.
- **In-process executor by default.** Deterministic, synchronous
  execution is the right default for tests and demos. The `Executor`
  protocol is the seam where a worker pool, container runner, or remote
  queue plugs in — the engine, state machine, and alerting do not change.
- **Validation at registration, not at run time.** A cyclic DAG or bad
  cron expression fails loudly when registered, never at 3 a.m.
- **Injectable clock and sleeper.** Tests control time and observe
  backoff delays without sleeping; production passes `time.sleep` and
  the real clock.
- **Quality gates as tasks, not sidecars.** A gate is an ordinary DAG
  task, so the existing failure semantics apply for free: a failed gate
  fails the run and skips everything downstream. No special-casing.
- **At most one SLA alert per run.** Re-checking an SLA is idempotent,
  so monitoring loops cannot spam operators.
- **Cron dialect is intentionally small** (5 fields, no seconds, no
  month/day names). Enough for batch schedules; a production deployment
  would swap `scheduling.py` for a full scheduler without touching the
  engine.
- **In-memory run store.** History is a record-keeping concern here, not
  a durability benchmark; the store interface is the seam for a database
  implementation.

## What it demonstrates

DAG authoring with dependency validation, cron-like scheduling and
backfill, retry with exponential backoff, a queued/running/succeeded/
failed/skipped state machine, data-quality gates that block downstream
work, SLA/deadline monitoring with deduplicated alerts, run-output
lineage, failure alerting, and a pluggable executor — all deterministic
and fully tested.

## CI

The CI workflow lives at `docs/github-actions-ci.yml`, **not** under
`.github/workflows/`. That path is rejected by the GitHub integration
used to publish this project, so the workflow ships as documentation and
activates by moving it to `.github/workflows/ci.yml` after checkout.

## Data

All data is synthetic (invented orders for a fictional shop). No real
company data, schema, or pipeline is used anywhere in this project.
