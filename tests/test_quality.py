from functools import partial

import pytest

from orchestrator import (
    QualityCheckError,
    check_null_rate,
    check_row_count,
    check_unique,
    check_value_range,
    make_quality_gate,
)

ROWS = [
    {"id": 1, "score": 10.0, "label": "a"},
    {"id": 2, "score": 20.0, "label": "b"},
    {"id": 3, "score": 30.0, "label": None},
]


def test_row_count_and_unique_pass():
    assert check_row_count(ROWS, 3).passed
    assert check_unique(ROWS, "id").passed
    assert check_unique(ROWS, "label").passed  # a, b, None are three distinct values
    assert check_unique([{"x": 1}, {"x": 1}], "x").passed is False


def test_null_rate():
    assert check_null_rate(ROWS, "label", 0.5).passed
    assert not check_null_rate(ROWS, "label", 0.1).passed
    assert not check_null_rate([], "label", 0.5).passed


def test_value_range():
    assert check_value_range(ROWS, "score", 0, 100).passed
    assert not check_value_range(ROWS, "score", 0, 25).passed


def test_gate_passes_and_returns_results():
    gate = make_quality_gate("rows", [partial(check_row_count, minimum=2)])
    results = gate({"rows": ROWS})
    assert len(results) == 1


def test_gate_raises_on_failure():
    gate = make_quality_gate("rows", [partial(check_row_count, minimum=100)])
    with pytest.raises(QualityCheckError):
        gate({"rows": ROWS})
