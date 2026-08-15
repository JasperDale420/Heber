"""Market Calendar - Trading hours and holiday awareness.

Uses exchange-calendars for accurate market session data.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from functools import lru_cache
from typing import cast
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import pandas as pd
import structlog

logger = structlog.get_logger(__name__)

# US Eastern timezone for NYSE
ET = ZoneInfo("America/New_York")

# Default exchange for US equity options
DEFAULT_EXCHANGE = "XNYS"  # NYSE


class MarketCalendar:
    """US equity options market calendar.

    Wraps exchange-calendars to provide:
    - Market open/close detection
    - Holiday and early close awareness
    - Trading time calculations (skip non-trading hours)

    Example:
        cal = MarketCalendar()
        if cal.is_market_open():
            # Safe to poll for quotes
            ...

        # Calculate window end in trading time
        window_end = cal.add_trading_hours(alert_time, 4.0)
    """

    def __init__(
        self,
        exchange: str = DEFAULT_EXCHANGE,
        include_extended: bool = False,
    ):
        """Initialize with exchange calendar.

        Args:
            exchange: Exchange code (ISO-10383), default XNYS (NYSE)
            include_extended: Extended-hours support switch. Only regular sessions
                             are currently supported. Setting this to True raises.
        """
        if include_extended:
            raise NotImplementedError(
                "MarketCalendar does not currently support include_extended=True. "
                "Use regular session hours or add explicit extended-session logic."
            )

        self.exchange = exchange
        self._cal = xcals.get_calendar(exchange)

    def is_market_open(self, dt: datetime | None = None) -> bool:
        """Check if market is currently open.

        Args:
            dt: Datetime to check (default: now). Naive values are treated as UTC.

        Returns:
            True if market is open for trading
        """
        ts = self._to_exchange_timestamp(dt)

        try:
            return cast(bool, self._cal.is_open_on_minute(ts))
        except ValueError:
            # Date out of calendar range
            return False

    def is_trading_day(self, dt: datetime | pd.Timestamp | None = None) -> bool:
        """Check if date is a trading day (not weekend/holiday).

        Args:
            dt: Datetime to check (default: now). Naive values are treated as UTC.

        Returns:
            True if the date has a trading session
        """
        ts = self._to_exchange_timestamp(dt)
        date = ts.date()

        try:
            return cast(bool, self._cal.is_session(pd.Timestamp(date)))
        except ValueError:
            return False

    def session_open(self, dt: datetime | pd.Timestamp | None = None) -> datetime:
        """Get market open time for the session containing dt.

        Args:
            dt: Datetime within the session (default: now). Naive values are treated as UTC.

        Returns:
            Market open time as UTC datetime
        """
        ts = self._to_exchange_timestamp(dt)
        session = self._get_session(ts)

        if session is None:
            # Not a trading day, get next session
            session = self._next_session(ts)

        open_time = self._cal.session_open(session)
        return cast(datetime, open_time.to_pydatetime().replace(tzinfo=UTC))

    def session_close(self, dt: datetime | pd.Timestamp | None = None) -> datetime:
        """Get market close time for the session containing dt.

        Handles early closes (e.g., day after Thanksgiving at 1pm).

        Args:
            dt: Datetime within the session (default: now). Naive values are treated as UTC.

        Returns:
            Market close time as UTC datetime
        """
        ts = self._to_exchange_timestamp(dt)
        session = self._get_session(ts)

        if session is None:
            # Not a trading day, get next session
            session = self._next_session(ts)

        close_time = self._cal.session_close(session)
        return cast(datetime, close_time.to_pydatetime().replace(tzinfo=UTC))

    def next_open(self, dt: datetime | pd.Timestamp | None = None) -> datetime:
        """Get next market open time.

        If market is currently open, returns the open time of the next session.

        Args:
            dt: Reference time (default: now). Naive values are treated as UTC.

        Returns:
            Next market open as UTC datetime
        """
        ts = self._to_exchange_timestamp(dt)

        if self.is_market_open(dt):
            session = self._get_session(ts)
            if session is not None:
                ts = self._cal.session_close(session) + pd.Timedelta(minutes=1)

        next_session = self._next_session(ts)
        open_time = self._cal.session_open(next_session)

        return cast(datetime, open_time.to_pydatetime().replace(tzinfo=UTC))

    def next_close(self, dt: datetime | pd.Timestamp | None = None) -> datetime:
        """Get next market close time.

        If market is currently open, returns close time of current session.
        If market is closed, returns close time of next session.

        Args:
            dt: Reference time (default: now). Naive values are treated as UTC.

        Returns:
            Next market close as UTC datetime
        """
        ts = self._to_exchange_timestamp(dt)

        if self.is_market_open(dt):
            # Return current session's close
            session = self._get_session(ts)
            if session is not None:
                close_time = self._cal.session_close(session)
                return cast(datetime, close_time.to_pydatetime().replace(tzinfo=UTC))

        # Return next session's close
        next_session = self._next_session(ts)
        close_time = self._cal.session_close(next_session)
        return cast(datetime, close_time.to_pydatetime().replace(tzinfo=UTC))

    def add_trading_hours(self, dt: datetime | pd.Timestamp, hours: float) -> datetime:
        """Add trading hours to a timestamp, skipping non-trading time.

        Essential for calculating watch window_end in *market time* rather
        than clock time. A "4-hour intraday window" should span 4 hours of
        actual trading, not calendar time.

        Args:
            dt: Starting datetime. Naive values are treated as UTC.
            hours: Number of trading hours to add

        Returns:
            Datetime after the specified trading hours have elapsed
        """
        minutes_remaining = int(hours * 60)
        current = self._to_exchange_timestamp(dt)

        while minutes_remaining > 0:
            # If not during trading hours, advance to next open
            if not self._cal.is_open_on_minute(current):
                next_session = self._next_session(current)
                current = self._cal.session_open(next_session)

            # Get close of current session
            session = self._get_session(current)
            if session is None:
                next_session = self._next_session(current)
                current = self._cal.session_open(next_session)
                continue

            session_close = self._cal.session_close(session)

            # Minutes until session close
            minutes_to_close = int((session_close - current).total_seconds() / 60)

            if minutes_remaining < minutes_to_close:
                # Finish within this session
                current = current + pd.Timedelta(minutes=minutes_remaining)
                minutes_remaining = 0
            else:
                # Use up this session and continue to next. Search from the close,
                # not from `current`: _next_session returns the current session for
                # any timestamp before its close. A span that ends exactly at the
                # close lands here too, so it resolves to the next session's open
                # rather than a minute the market is shut.
                minutes_remaining -= minutes_to_close
                next_session = self._next_session(session_close)
                current = self._cal.session_open(next_session)

        return cast(datetime, current.tz_convert(UTC).to_pydatetime())

    def trading_minutes_until(
        self,
        start: datetime | pd.Timestamp,
        end: datetime | pd.Timestamp,
    ) -> int:
        """Count trading minutes between two times.

        Only counts minutes when the market is open. Useful for calculating
        how long a trade actually took in *market time*.

        Args:
            start: Start datetime. Naive values are treated as UTC.
            end: End datetime. Naive values are treated as UTC.

        Returns:
            Number of trading minutes between start and end
        """
        start_ts = self._to_exchange_timestamp(start)
        end_ts = self._to_exchange_timestamp(end)

        if end_ts <= start_ts:
            return 0

        total_minutes = 0
        current = start_ts

        while current < end_ts:
            current, session_minutes = self._count_session_minutes(current, end_ts)
            if session_minutes < 0:
                break
            total_minutes += session_minutes

        return total_minutes

    def _count_session_minutes(
        self,
        current: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> tuple[pd.Timestamp, int]:
        """Count trading minutes in current session and advance to next.

        Returns:
            Tuple of (next_position, minutes_counted).
            Returns (current, -1) to signal end of iteration.
        """
        # Advance to next open if not during trading
        if not self._cal.is_open_on_minute(current):
            try:
                next_session = self._next_session(current)
                next_open = self._cal.session_open(next_session)
                if next_open >= end_ts:
                    return current, -1
                return next_open, 0
            except ValueError:
                return current, -1

        # Get session close
        session = self._get_session(current)
        if session is None:
            return current, -1

        session_close = self._cal.session_close(session)
        effective_end = min(session_close, end_ts)

        # Count minutes in this trading window
        minutes = max(0, int((effective_end - current).total_seconds() / 60))

        # Move to next session if needed
        if session_close < end_ts:
            try:
                next_session = self._next_session(session_close)
                return self._cal.session_open(next_session), minutes
            except ValueError:
                return current, -1

        return end_ts, minutes

    def seconds_until_open(self, dt: datetime | pd.Timestamp | None = None) -> float:
        """Get seconds until market opens.

        Returns 0 if market is currently open.

        Args:
            dt: Reference time (default: now). Naive values are treated as UTC.

        Returns:
            Seconds until next market open, or 0 if already open
        """
        dt_utc = self._to_utc_timestamp(dt).to_pydatetime()

        if self.is_market_open(dt_utc):
            return 0.0

        next_open = self.next_open(dt_utc)
        return cast(float, max(0.0, (next_open - dt_utc).total_seconds()))

    def _to_utc_timestamp(self, dt: datetime | pd.Timestamp | None) -> pd.Timestamp:
        """Normalize supported datetime inputs to timezone-aware UTC pandas timestamp.

        Naive datetime-like values are interpreted as UTC.
        """
        if dt is None:
            return pd.Timestamp(datetime.now(UTC))
        if not isinstance(dt, datetime | pd.Timestamp):
            raise TypeError(f"MarketCalendar expects datetime or pandas.Timestamp, got {type(dt).__name__}")

        ts = pd.Timestamp(dt)
        if ts.tz is None:
            return ts.tz_localize(UTC)
        return ts.tz_convert(UTC)

    def _to_exchange_timestamp(self, dt: datetime | pd.Timestamp | None) -> pd.Timestamp:
        """Convert supported inputs to exchange timezone timestamp for calendar checks."""
        return self._to_utc_timestamp(dt).tz_convert(ET)

    def _get_session(self, ts: pd.Timestamp) -> pd.Timestamp | None:
        """Get the trading session for a timestamp."""
        date = ts.date()
        session = pd.Timestamp(date)

        if self._cal.is_session(session):
            return session
        return None

    def _next_session(self, ts: pd.Timestamp) -> pd.Timestamp:
        """Get the next trading session after timestamp."""
        date = ts.date()

        # Start from today if before close, else tomorrow
        if self._cal.is_session(pd.Timestamp(date)):
            session_close = self._cal.session_close(pd.Timestamp(date))
            if ts < session_close:
                # Still in today's session window
                return pd.Timestamp(date)

        # Find next session
        start_date = pd.Timestamp(date + timedelta(days=1))

        # Search up to 10 days (covers long weekends)
        for i in range(10):
            check_date = start_date + pd.Timedelta(days=i)
            if self._cal.is_session(check_date):
                return check_date

        raise ValueError(f"No trading session found within 10 days of {date}")


@lru_cache(maxsize=4)
def get_calendar(exchange: str = DEFAULT_EXCHANGE) -> MarketCalendar:
    """Get cached MarketCalendar instance.

    Args:
        exchange: Exchange code (default: XNYS)

    Returns:
        Cached MarketCalendar instance
    """
    return MarketCalendar(exchange=exchange)
