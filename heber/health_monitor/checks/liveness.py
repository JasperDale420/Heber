"""Per-feed liveness check — the critical-feed alarm's detector.

Compares recent Silver activity to an absolute floor. A feed that produces no
partition (or errors on read) is treated as zero rows — the worst case — rather
than skipped.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import structlog

from heber.health_monitor.feed_registry import FeedRule, resolved_registry
from heber.health_monitor.models import CheckContext, CheckResult, Severity, Status

logger = structlog.get_logger(__name__)

ET = ZoneInfo("America/New_York")
CHECK_NAME = "feed_liveness"
_REGULAR_CLOSE_MINUTES = 16 * 60  # 16:00 ET


def _parse_hhmm(value: str) -> time:
    hh, mm = value.split(":")
    return time(int(hh), int(mm))


def _minutes(t: time) -> int:
    return t.hour * 60 + t.minute


def _early_close_adjusted_end(ctx: CheckContext, window_end: time, day_et: date | None) -> time:
    """Shift a continuous window's end earlier on early-close (half) days.

    On a 13:00 ET close, regular-hours feeds legitimately go quiet at 13:00,
    so a fixed 16:00/20:00 window fires false 'feed dark' criticals all
    afternoon. Shift the end earlier by the early-close delta — this preserves
    each feed's after-hours tail relative to the real close (e.g. darkpool's
    20:00 = +4h tail becomes 13:00+4h = 17:00).
    """
    if day_et is None:
        return window_end
    hours = ctx.calendar.market_hours(day_et)
    if hours is None:
        return window_end
    close_minutes = _minutes(hours[1])
    if close_minutes >= _REGULAR_CLOSE_MINUTES:
        return window_end
    shifted = max(0, _minutes(window_end) - (_REGULAR_CLOSE_MINUTES - close_minutes))
    return time(shifted // 60, shifted % 60)


def _window_row_count(ctx: CheckContext, feed: str, start_utc: datetime, end_utc: datetime) -> int:
    """Count Silver rows for `feed` with ts_event in [start_utc, end_utc].

    Missing partition / read error / empty frame -> 0 (the worst case).
    """
    try:
        df = ctx.reader.read_silver(
            feed,
            time_range=(start_utc.isoformat(), end_utc.isoformat()),
            columns=["ts_event"],
            prune_by_dt=True,
        )
    except Exception:
        logger.warning("liveness_read_error", feed=feed, exc_info=True)
        return 0
    if df is None or df.empty:
        return 0
    return int(len(df))


def _result(feed: str, status: Status, message: str, now: datetime, details: dict) -> CheckResult:
    severity = Severity.P2_INFO if status == Status.PASS else Severity.P0_CRITICAL
    return CheckResult(
        check_name=CHECK_NAME,
        feed=feed,
        severity=severity,
        status=status,
        message=message,
        details=details,
        ts_checked=now,
    )


def _check_continuous(ctx: CheckContext, rule: FeedRule, now_et: datetime) -> CheckResult | None:
    start = _parse_hhmm(rule.window_start_et)
    end = _parse_hhmm(rule.window_end_et)
    if not (start <= now_et.time() <= end):
        return None  # out of the regular window -> no result
    # Inside the regular window: on early-close (half) days the feed may already
    # be legitimately done, so shift the end earlier and re-check before alarming.
    end = _early_close_adjusted_end(ctx, end, now_et.date())
    if now_et.time() > end:
        return None  # past the early close -> feed legitimately quiet, no alarm

    now_utc = now_et.astimezone(UTC)
    cutoff_utc = now_utc - timedelta(minutes=rule.lookback_minutes)
    count = _window_row_count(ctx, rule.feed, cutoff_utc, now_utc)
    details = {
        "feed": rule.feed,
        "rows": count,
        "floor": rule.floor,
        "lookback_minutes": rule.lookback_minutes,
    }
    if count < rule.floor:
        msg = (
            f"{rule.feed}: {count} rows in last {rule.lookback_minutes}m "
            f"(floor {rule.floor}) — feed appears dark/degraded"
        )
        return _result(rule.feed, Status.FAIL, msg, now_et, details)
    msg = f"{rule.feed}: {count} rows in last {rule.lookback_minutes}m (floor {rule.floor})"
    return _result(rule.feed, Status.PASS, msg, now_et, details)


def _check_daily(ctx: CheckContext, rule: FeedRule, now_et: datetime) -> CheckResult | None:
    deadline = _parse_hhmm(rule.window_end_et)
    if now_et.time() < deadline:
        return None  # before deadline -> not yet in scope

    now_utc = now_et.astimezone(UTC)
    # EOD feeds stamp ts_event at UTC-midnight (00:00Z), so the day window must
    # start at UTC-midnight of today's date — not ET-midnight (04:00/05:00Z),
    # which would fall *after* those rows and exclude them (false CRITICAL).
    today = now_et.date()
    day_start_utc = datetime(today.year, today.month, today.day, tzinfo=UTC)
    count = _window_row_count(ctx, rule.feed, day_start_utc, now_utc)
    details = {"feed": rule.feed, "rows": count, "floor": rule.floor, "deadline_et": rule.window_end_et}
    if count < rule.floor:
        msg = f"{rule.feed}: {count} rows today by {rule.window_end_et} ET (floor {rule.floor}) — EOD feed missing"
        return _result(rule.feed, Status.FAIL, msg, now_et, details)
    msg = f"{rule.feed}: {count} rows today (floor {rule.floor})"
    return _result(rule.feed, Status.PASS, msg, now_et, details)


async def run_liveness_checks(ctx: CheckContext, now: datetime) -> list[CheckResult]:
    """Run per-feed liveness checks. `now` must be a tz-aware datetime."""
    now_et = now.astimezone(ET)
    today = now_et.date()
    if not ctx.calendar.is_trading_day(today):
        return []

    results: list[CheckResult] = []
    for rule in resolved_registry(ctx.settings.alert_floor_overrides):
        if rule.kind == "continuous":
            res = _check_continuous(ctx, rule, now_et)
        else:
            res = _check_daily(ctx, rule, now_et)
        if res is not None:
            results.append(res)
    return results
