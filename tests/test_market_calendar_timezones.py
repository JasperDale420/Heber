from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from heber.calendar.market import MarketCalendar

REFERENCE_AWARE = datetime(2026, 1, 5, 15, 0, tzinfo=UTC)
REFERENCE_NAIVE = REFERENCE_AWARE.replace(tzinfo=None)


@pytest.fixture(scope="module")
def calendar() -> MarketCalendar:
    return MarketCalendar()


def test_naive_and_aware_inputs_match_for_core_methods(calendar: MarketCalendar) -> None:
    assert calendar.is_trading_day(REFERENCE_NAIVE) == calendar.is_trading_day(REFERENCE_AWARE)
    assert calendar.is_market_open(REFERENCE_NAIVE) == calendar.is_market_open(REFERENCE_AWARE)
    assert calendar.session_open(REFERENCE_NAIVE) == calendar.session_open(REFERENCE_AWARE)
    assert calendar.session_close(REFERENCE_NAIVE) == calendar.session_close(REFERENCE_AWARE)
    assert calendar.next_open(REFERENCE_NAIVE) == calendar.next_open(REFERENCE_AWARE)
    assert calendar.next_close(REFERENCE_NAIVE) == calendar.next_close(REFERENCE_AWARE)
    assert calendar.add_trading_hours(REFERENCE_NAIVE, 1.5) == calendar.add_trading_hours(REFERENCE_AWARE, 1.5)
    assert calendar.trading_minutes_until(
        REFERENCE_NAIVE,
        REFERENCE_NAIVE + timedelta(hours=5),
    ) == calendar.trading_minutes_until(
        REFERENCE_AWARE,
        REFERENCE_AWARE + timedelta(hours=5),
    )
    assert calendar.seconds_until_open(REFERENCE_NAIVE) == calendar.seconds_until_open(REFERENCE_AWARE)


def test_pandas_timestamp_inputs_are_supported(calendar: MarketCalendar) -> None:
    ts_naive = pd.Timestamp("2026-01-05 15:00:00")
    ts_aware = pd.Timestamp("2026-01-05 15:00:00", tz="UTC")

    assert calendar.session_open(ts_naive) == calendar.session_open(ts_aware)
    assert calendar.session_close(ts_naive) == calendar.session_close(ts_aware)
    assert calendar.seconds_until_open(ts_naive) == calendar.seconds_until_open(ts_aware)


def test_invalid_datetime_input_raises_clear_error(calendar: MarketCalendar) -> None:
    with pytest.raises(TypeError, match="datetime or pandas.Timestamp"):
        calendar.is_market_open("2026-01-05")  # type: ignore[arg-type]
