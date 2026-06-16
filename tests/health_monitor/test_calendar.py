"""Tests for health monitor market calendar."""

from __future__ import annotations

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import pytest

from heber.health_monitor.calendar import HealthCalendar

ET = ZoneInfo("America/New_York")


class TestHealthCalendar:
    @pytest.fixture()
    def cal(self):
        return HealthCalendar()

    def test_regular_trading_day_hours(self, cal):
        d = date(2026, 3, 24)
        assert cal.is_trading_day(d)
        hours = cal.expected_hours(d)
        assert hours == [9, 10, 11, 12, 13, 14, 15]

    def test_weekend_not_trading_day(self, cal):
        d = date(2026, 3, 28)
        assert not cal.is_trading_day(d)
        assert cal.expected_hours(d) == []

    def test_market_hours(self, cal):
        d = date(2026, 3, 24)
        open_time, close_time = cal.market_hours(d)
        assert open_time == time(9, 30)
        assert close_time == time(16, 0)

    def test_closed_day_market_hours(self, cal):
        d = date(2026, 3, 28)
        assert cal.market_hours(d) is None

    def test_is_market_open_during_hours(self, cal):
        dt = datetime(2026, 3, 24, 10, 30, 0, tzinfo=ET)
        assert cal.is_market_open(dt)

    def test_is_market_closed_before_open(self, cal):
        dt = datetime(2026, 3, 24, 9, 0, 0, tzinfo=ET)
        assert not cal.is_market_open(dt)

    def test_is_market_closed_after_close(self, cal):
        dt = datetime(2026, 3, 24, 16, 30, 0, tzinfo=ET)
        assert not cal.is_market_open(dt)

    def test_next_trading_day_from_friday(self, cal):
        friday = date(2026, 3, 27)
        assert cal.next_trading_day(friday) == date(2026, 3, 30)

    def test_elapsed_market_hours(self, cal):
        dt = datetime(2026, 3, 24, 12, 15, 0, tzinfo=ET)
        elapsed = cal.elapsed_hours(dt)
        assert elapsed == [9, 10, 11, 12]

    def test_elapsed_hours_before_market_open(self, cal):
        dt = datetime(2026, 3, 24, 8, 0, 0, tzinfo=ET)
        assert cal.elapsed_hours(dt) == []

    def test_suppress_severity_outside_market(self, cal):
        from heber.health_monitor.models import Severity

        weekend_dt = datetime(2026, 3, 28, 12, 0, 0, tzinfo=ET)
        result = cal.adjust_severity(Severity.P0_CRITICAL, weekend_dt)
        assert result == Severity.P2_INFO

    def test_no_suppression_during_market(self, cal):
        from heber.health_monitor.models import Severity

        market_dt = datetime(2026, 3, 24, 10, 0, 0, tzinfo=ET)
        result = cal.adjust_severity(Severity.P0_CRITICAL, market_dt)
        assert result == Severity.P0_CRITICAL
