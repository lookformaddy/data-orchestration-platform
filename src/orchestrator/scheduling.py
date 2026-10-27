"""Cron-like scheduling and backfill windows.

A deliberately small cron dialect: five fields (minute hour day-of-month
month day-of-week) supporting ``*``, ``*/n``, ``a-b``, ``a-b/n``, lists,
and single values. Day-of-week follows cron convention (0 and 7 = Sunday).
This keeps the scheduler dependency-free and fully testable; a production
deployment would swap this module for a real scheduler without touching
the engine.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from .errors import InvalidCronError

_FIELD_RANGES = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 7))


def _parse_field(text: str, lo: int, hi: int) -> set[int]:
    values: set[int] = set()
    for part in text.split(","):
        part = part.strip()
        if not part:
            raise InvalidCronError(f"empty cron field part in {text!r}")
        step = 1
        if "/" in part:
            base, step_text = part.split("/", 1)
            try:
                step = int(step_text)
            except ValueError as exc:
                raise InvalidCronError(f"bad cron step {step_text!r}") from exc
            if step < 1:
                raise InvalidCronError(f"cron step must be >= 1, got {step}")
        else:
            base = part
        if base == "*":
            start, end = lo, hi
        elif "-" in base:
            try:
                start, end = (int(x) for x in base.split("-", 1))
            except ValueError as exc:
                raise InvalidCronError(f"bad cron range {base!r}") from exc
        else:
            try:
                start = end = int(base)
            except ValueError as exc:
                raise InvalidCronError(f"bad cron value {base!r}") from exc
        if start < lo or end > hi or start > end:
            raise InvalidCronError(f"cron value {base!r} out of range {lo}-{hi}")
        values.update(range(start, end + 1, step))
    return values


class CronSchedule:
    """A parsed five-field cron expression."""

    def __init__(self, expression: str) -> None:
        fields = expression.split()
        if len(fields) != 5:
            raise InvalidCronError(
                f"cron expression must have 5 fields, got {len(fields)}: {expression!r}"
            )
        parsed = [_parse_field(f, lo, hi) for f, (lo, hi) in zip(fields, _FIELD_RANGES)]
        self.minutes, self.hours, self.days, self.months, dow = parsed
        # Normalize Sunday: cron 0 and 7 both mean Sunday.
        self.weekdays = {0 if d == 7 else d for d in dow}
        self.expression = expression

    def matches(self, moment: datetime) -> bool:
        cron_dow = (moment.weekday() + 1) % 7  # Mon=0..Sun=6 -> Sun=0..Sat=6
        return (
            moment.minute in self.minutes
            and moment.hour in self.hours
            and moment.day in self.days
            and moment.month in self.months
            and cron_dow in self.weekdays
        )

    def next_after(self, moment: datetime) -> datetime:
        """Next matching minute strictly after ``moment`` (minute resolution)."""
        candidate = (moment.replace(second=0, microsecond=0) + timedelta(minutes=1))
        # A schedule that never matches would loop forever; a 4-year bound
        # is far beyond any sane schedule and turns that into a clear error.
        limit = candidate + timedelta(days=4 * 366)
        while candidate <= limit:
            if self.matches(candidate):
                return candidate
            candidate += timedelta(minutes=1)
        raise InvalidCronError(f"cron expression never matches: {self.expression!r}")


def backfill_windows(
    schedule: CronSchedule, start: datetime, end: datetime
) -> list[datetime]:
    """All scheduled times in [start, end], in ascending order."""
    if end < start:
        raise ValueError("backfill end must not precede start")
    windows: list[datetime] = []
    cursor = start - timedelta(minutes=1)
    while True:
        nxt = schedule.next_after(cursor)
        if nxt > end:
            break
        windows.append(nxt)
        cursor = nxt
    return windows
