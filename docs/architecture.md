# Architecture

## Overview

```
                          ┌──────────────┐
  DAG definition ────────►│ Orchestrator │
  (tasks, deps, schedule, │  register()  │──► validation (dag.py, scheduling.py)
   SLA, retry policies)   │  trigger()   │
                           │  backfill()  │
                           └──────┬───────┘
                                  │ topological order (deps first)
                    ┌─────────────┼──────────────────────────────┐
                    ▼             ▼                              ▼
             Executor       RunStore                     AlertSink
        (in-process by   (run history,               (task_failed,
         default;        filter by DAG/               sla_breach)
         pluggable)      status)
```

## State machine

Per task and per run: `queued → running → succeeded`, or
`running → failed` once retries are exhausted. A task whose dependency
failed is `skipped`, never executed. A run is `failed` if any task
failed, otherwise `succeeded`. Runs are never mutated after finishing
except for the (deduplicated) SLA alert record.

## Execution algorithm

1. Validate the DAG at registration (duplicate ids, unknown deps, cycles
   via Kahn's algorithm, cron parse).
2. On trigger, create a `DagRun` with one `TaskRun` per task and walk
   tasks in topological order.
3. For each task: if any dependency failed, mark it skipped. Otherwise
   run it through the executor, retrying up to `max_retries` times with
   exponential backoff (`base × factor^(attempt-1)`, capped). The
   backoff delay goes through the injectable sleeper, so tests observe
   delays without sleeping.
4. On terminal failure, emit a `task_failed` alert.
5. After all tasks, record lineage edges (producer → consumer per
   dataset, only for succeeded tasks), check the SLA, and persist the run.

Levels (`execution_levels`) group tasks that could run concurrently; the
current engine executes levels sequentially in topological order, which
keeps execution deterministic. Parallel execution is a future step that
only needs to parallelize within a level.

## Scheduling

The cron dialect is five fields — minute, hour, day-of-month, month,
day-of-week (0 and 7 = Sunday) — supporting `*`, `*/n`, `a-b`, `a-b/n`,
lists, and single values. `next_after` scans minute-by-minute with a
4-year safety bound. Backfill enumerates every scheduled time in
`[start, end]` and triggers one run per window with `trigger=backfill`.

## Data-quality gates

A gate is a task built by `make_quality_gate(dataset, checks)`. Checks
(`check_row_count`, `check_null_rate`, `check_unique`,
`check_value_range`) run over the in-memory rows the context holds under
the dataset's logical name. Any failing check raises
`QualityCheckError`, failing the task — which, by the normal rules,
skips all downstream tasks. Gates therefore need no special engine code.

## SLA monitoring

A DAG may declare `sla_seconds`: the run must finish within that long
after its scheduled time. `check_sla` compares the run's finish time
(or now, for a still-running run) against the deadline, emits at most
one `sla_breach` alert per run, and is safe to call repeatedly from a
monitoring loop.

## Lineage

Each task declares the datasets it `produces` and `consumes`. After a
run, the engine records one edge per (producer, consumer, dataset)
triple where both tasks succeeded. Lineage is derived from declarations
and outcomes, not from data movement — the engine never moves data.

## Alerting

Alert sinks implement a one-method protocol (`send`). The default
in-memory sink records alerts for tests and demos; a production sink
posts to email, Slack, or PagerDuty. Alerting is decoupled from
execution so a broken notifier cannot fail a run.

## Extension points

| Need | Seam |
|---|---|
| Remote/distributed execution | Implement `Executor` |
| Durable run history | Implement the `RunStore` interface |
| Real notifications | Implement `AlertSink` |
| Full cron / timezone-aware schedules | Replace `scheduling.py` |
| Parallel task execution | Parallelize within `execution_levels` |
| Persistent DAG registry | Serialize `DagDefinition` (dataclasses) |

## Diagram (text)

```
extract ──► validate (quality gate) ──► transform ──► load
   │              │                          │
   └─ produces ───┘                          └─ produces clean_orders
      orders        (gate blocks bad data)      consumed by load
```

## CI note

The GitHub Actions workflow is stored at `docs/github-actions-ci.yml`,
not `.github/workflows/ci.yml`, because the GitHub integration used to
publish this project rejects files under `.github/workflows/`. Move it
into place after checkout to activate CI.
