"""Shared fixtures: a deterministic clock and a no-sleep orchestrator."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from orchestrator import Orchestrator  # noqa: E402


class FakeClock:
    """A clock the test controls; ``advance`` moves time forward."""

    def __init__(self, start: datetime) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        from datetime import timedelta

        self.now = self.now + timedelta(seconds=seconds)


@pytest.fixture()
def clock():
    return FakeClock(datetime(2026, 10, 27, 6, 0, 0))


@pytest.fixture()
def orch(clock):
    sleeps: list[float] = []

    def sleeper(delay: float) -> None:
        sleeps.append(delay)

    engine = Orchestrator(now_fn=clock, sleeper=sleeper)
    engine.recorded_sleeps = sleeps  # type: ignore[attr-defined]
    return engine


def ok_task(key: str, value=None):
    def fn(context):
        context[key] = value if value is not None else key
        return value

    return fn


def failing_task(message: str = "boom"):
    def fn(context):
        raise RuntimeError(message)

    return fn
