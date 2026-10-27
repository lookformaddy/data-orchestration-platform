"""Data-quality gates, implemented as plain functions over in-memory rows.

A gate is a task like any other: it reads datasets the context already
holds (under their logical names) and raises :class:`QualityCheckError`
when a check fails. Because a failed gate fails its task, downstream
tasks are skipped and the run is marked failed — data never flows past
a gate it did not pass. All inputs here are synthetic rows of dicts.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Sequence


class QualityCheckError(Exception):
    """A data-quality check failed; the gate task fails with this."""


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str


def check_row_count(rows: Sequence[dict[str, Any]], minimum: int) -> CheckResult:
    ok = len(rows) >= minimum
    return CheckResult(
        "row_count", ok, f"{len(rows)} rows, minimum {minimum}"
    )


def check_null_rate(
    rows: Sequence[dict[str, Any]], column: str, max_rate: float
) -> CheckResult:
    if not rows:
        return CheckResult(f"null_rate:{column}", False, "no rows to check")
    nulls = sum(1 for r in rows if r.get(column) is None)
    rate = nulls / len(rows)
    return CheckResult(
        f"null_rate:{column}", rate <= max_rate,
        f"null rate {rate:.3f} over {len(rows)} rows, max {max_rate}",
    )


def check_unique(
    rows: Sequence[dict[str, Any]], column: str
) -> CheckResult:
    values = [r.get(column) for r in rows]
    ok = len(set(values)) == len(values)
    return CheckResult(
        f"unique:{column}", ok,
        f"{len(values)} values, {len(set(values))} distinct",
    )


def check_value_range(
    rows: Sequence[dict[str, Any]], column: str, low: float, high: float
) -> CheckResult:
    bad = [r.get(column) for r in rows if r.get(column) is None or not (low <= r[column] <= high)]
    return CheckResult(
        f"range:{column}", not bad,
        f"{len(bad)} out-of-range values in {column}" if bad else "all values in range",
    )


def make_quality_gate(
    dataset_key: str, checks: Sequence[Callable[[Sequence[dict[str, Any]]], CheckResult]]
) -> Callable[[dict[str, Any]], list[dict[str, Any]]]:
    """Build a gate task function: runs every check, raises on any failure.

    Returns the list of check results (as dicts) so the run record shows
    exactly what was verified.
    """

    def gate(context: dict[str, Any]) -> list[dict[str, Any]]:
        rows = context.get(dataset_key, [])
        results = [c(rows) for c in checks]
        failed = [r for r in results if not r.passed]
        if failed:
            raise QualityCheckError(
                f"quality gate failed on {dataset_key}: "
                + "; ".join(f"{r.name} ({r.detail})" for r in failed)
            )
        return [{"name": r.name, "detail": r.detail} for r in results]

    return gate
