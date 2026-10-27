from datetime import datetime

import pytest

from orchestrator import CronSchedule, InvalidCronError, backfill_windows


def test_daily_6am_next_run():
    sched = CronSchedule("0 6 * * *")
    assert sched.next_after(datetime(2026, 10, 27, 5, 59)) == datetime(2026, 10, 27, 6, 0)
    assert sched.next_after(datetime(2026, 10, 27, 6, 0)) == datetime(2026, 10, 28, 6, 0)


def test_step_and_list_expressions():
    sched = CronSchedule("*/30 9-17 * * 1-5")
    assert sched.matches(datetime(2026, 10, 26, 9, 30))  # Monday
    assert not sched.matches(datetime(2026, 10, 26, 9, 15))
    assert not sched.matches(datetime(2026, 10, 25, 9, 30))  # Sunday


def test_sunday_0_and_7_equivalent():
    for expr in ("0 6 * * 0", "0 6 * * 7"):
        assert CronSchedule(expr).matches(datetime(2026, 10, 25, 6, 0))


def test_bad_expressions_rejected():
    for expr in ("0 6 * *", "0 25 * * *", "x 6 * * *", "0 6 * * 8", "*/0 * * * *"):
        with pytest.raises(InvalidCronError):
            CronSchedule(expr)


def test_backfill_windows_daily():
    sched = CronSchedule("0 6 * * *")
    windows = backfill_windows(
        sched, datetime(2026, 10, 24, 0, 0), datetime(2026, 10, 26, 23, 59)
    )
    assert windows == [
        datetime(2026, 10, 24, 6, 0),
        datetime(2026, 10, 25, 6, 0),
        datetime(2026, 10, 26, 6, 0),
    ]


def test_backfill_rejects_inverted_range():
    with pytest.raises(ValueError):
        backfill_windows(CronSchedule("0 6 * * *"), datetime(2026, 10, 26), datetime(2026, 10, 24))
