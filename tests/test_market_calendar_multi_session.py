"""Multi-session coverage for MarketCalendar trading-time arithmetic.

`add_trading_hours` drives watch `window_end` for SWING (120h) and LEAP (720h)
horizons, which span weeks of trading. Expected results are derived from the
exchange-calendars trading-minute index rather than hand-computed constants.
"""

from __future__ import annotations

from datetime import UTC, datetime

import exchange_calendars as xcals
import pandas as pd
import pytest

from heber.calendar import MarketCalendar

# Mid-session start: 2026-01-05 15:00 UTC == 10:00 ET on a Monday.
START = datetime(2026, 1, 5, 15, 0, tzinfo=UTC)


def _expected(start: datetime, hours: float) -> datetime:
    """Trading minute `hours * 60` after `start`, from the exchange calendar itself."""
    minutes = xcals.get_calendar("XNYS").minutes
    index = int(minutes.searchsorted(pd.Timestamp(start)))
    return minutes[index + int(hours * 60)].to_pydatetime()


@pytest.mark.unit
@pytest.mark.parametrize("hours", [4, 120, 720])
def test_add_trading_hours_matches_calendar_minute_index(hours: int) -> None:
    calendar = MarketCalendar()

    assert calendar.add_trading_hours(START, hours) == _expected(START, hours)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("start", "hours"),
    [
        # 13:00 ET + 120h and 11:00 ET + 720h both land exactly on a Friday close.
        (datetime(2026, 1, 5, 18, 0, tzinfo=UTC), 120),
        (datetime(2026, 1, 5, 16, 0, tzinfo=UTC), 720),
        # 09:30 ET on the half-day after Thanksgiving 2026 + that session's 3.5 hours.
        (datetime(2026, 11, 27, 14, 30, tzinfo=UTC), 3.5),
        # DST-adjacent: 09:30 EST the Friday before the spring-forward weekend.
        (datetime(2026, 3, 6, 14, 30, tzinfo=UTC), 6.5),
    ],
)
def test_add_trading_hours_lands_on_next_open_at_exact_session_boundary(
    start: datetime,
    hours: float,
) -> None:
    """A span ending exactly at a close resolves to the next open, not the close."""
    calendar = MarketCalendar()

    result = calendar.add_trading_hours(start, hours)

    assert result == _expected(start, hours)
    assert calendar.is_market_open(result)
    assert calendar.trading_minutes_until(start, result) == int(hours * 60)


@pytest.mark.unit
def test_add_trading_hours_leap_window_spans_months() -> None:
    """A 720 trading-hour window is ~111 sessions, not the same calendar day."""
    calendar = MarketCalendar()

    result = calendar.add_trading_hours(START, 720)

    assert (result - START).days > 150
    assert calendar.is_market_open(result)


@pytest.mark.unit
def test_add_trading_hours_stays_utc_within_one_session() -> None:
    """Intraday spans must convert ET back to UTC, not relabel the tzinfo."""
    calendar = MarketCalendar()

    # 10:00 ET + 4 trading hours = 14:00 ET = 19:00 UTC.
    assert calendar.add_trading_hours(START, 4) == datetime(2026, 1, 5, 19, 0, tzinfo=UTC)


@pytest.mark.unit
def test_trading_minutes_until_counts_across_sessions() -> None:
    """Round-trip: the span produced by add_trading_hours measures back to itself."""
    calendar = MarketCalendar()

    end = calendar.add_trading_hours(START, 120)

    assert calendar.trading_minutes_until(START, end) == 120 * 60
