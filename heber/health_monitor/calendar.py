"""Extended market calendar with hour-level granularity for health checks."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import structlog

from heber.calendar.market import get_calendar
from heber.health_monitor.models import Severity

logger = structlog.get_logger(__name__)

ET = ZoneInfo("America/New_York")

REGULAR_OPEN = time(9, 30)
REGULAR_CLOSE = time(16, 0)


class HealthCalendar:
    """Market calendar extended with hour-level granularity for health monitoring."""

    def __init__(self) -> None:
        self._base = get_calendar()

    def _date_to_datetime(self, d: date) -> datetime:
        """Convert a plain date to a noon-ET datetime for calendar lookups."""
        return datetime.combine(d, time(12, 0), tzinfo=ET)

    def is_trading_day(self, d: date) -> bool:
        return self._base.is_trading_day(self._date_to_datetime(d))

    def is_market_open(self, dt: datetime) -> bool:
        return self._base.is_market_open(dt)

    def market_hours(self, d: date) -> tuple[time, time] | None:
        if not self.is_trading_day(d):
            return None
        close_dt = self._base.session_close(self._date_to_datetime(d))
        close_et = close_dt.astimezone(ET)
        close_time = close_et.time()
        return REGULAR_OPEN, close_time

    def expected_hours(self, d: date) -> list[int]:
        hours = self.market_hours(d)
        if hours is None:
            return []
        _, close_time = hours
        last_hour = close_time.hour - 1 if close_time.minute == 0 else close_time.hour
        return list(range(9, last_hour + 1))

    def elapsed_hours(self, dt: datetime) -> list[int]:
        et_dt = dt.astimezone(ET) if dt.tzinfo else dt.replace(tzinfo=ET)
        d = et_dt.date()

        expected = self.expected_hours(d)
        if not expected:
            return []

        current_hour = et_dt.hour
        return [h for h in expected if h <= current_hour]

    def next_trading_day(self, d: date) -> date:
        candidate = d + timedelta(days=1)
        while not self.is_trading_day(candidate):
            candidate += timedelta(days=1)
        return candidate

    def adjust_severity(self, severity: Severity, dt: datetime) -> Severity:
        if self.is_market_open(dt):
            return severity
        return Severity.P2_INFO
