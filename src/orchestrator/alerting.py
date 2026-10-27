"""Alerting: sinks that record failure and SLA-breach alerts.

An :class:`AlertSink` is any object with a ``send`` method. The default
in-memory sink is what tests and demos use; a real deployment wires a
sink that posts to email, Slack, or PagerDuty. Alerting is deliberately
decoupled from the engine so a broken notifier can never break a run.
"""

from __future__ import annotations

from typing import Protocol

from .models import Alert


class AlertSink(Protocol):
    def send(self, alert: Alert) -> None: ...


class InMemoryAlertSink:
    """Records every alert it receives, in order."""

    def __init__(self) -> None:
        self.alerts: list[Alert] = []

    def send(self, alert: Alert) -> None:
        self.alerts.append(alert)

    def of_kind(self, kind: str) -> list[Alert]:
        return [a for a in self.alerts if a.kind == kind]
