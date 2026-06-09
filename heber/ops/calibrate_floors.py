"""Suggest per-feed liveness floors from a healthy historical window."""

from __future__ import annotations

import json
import statistics
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import structlog

from heber.health_monitor.feed_registry import DEFAULT_REGISTRY
from heber.reader import HeberReader

logger = structlog.get_logger(__name__)
ET = ZoneInfo("America/New_York")


def suggest_floor_from_counts(bucket_counts: list[int], ratio: float = 0.3) -> int:
    """Suggested floor = max(1, int(ratio * median(bucket_counts)))."""
    if not bucket_counts:
        return 1
    median = statistics.median(bucket_counts)
    return max(1, int(ratio * median))


def _bucket_counts(ts_series, ref_day: date, rule, lookback: int) -> list[int]:
    """Count ts_event rows in consecutive `lookback`-minute buckets over the window."""
    start_t = time(*[int(x) for x in rule.window_start_et.split(":")])
    end_t = time(*[int(x) for x in rule.window_end_et.split(":")])
    counts: list[int] = []
    cursor = datetime.combine(ref_day, start_t, tzinfo=ET)
    window_end = datetime.combine(ref_day, end_t, tzinfo=ET)
    ts_utc = ts_series.dt.tz_convert("UTC")
    while cursor < window_end:
        nxt = cursor + timedelta(minutes=lookback)
        lo, hi = cursor.astimezone(UTC), nxt.astimezone(UTC)
        counts.append(int(((ts_utc >= lo) & (ts_utc < hi)).sum()))
        cursor = nxt
    return counts


def calibrate(days_back: int = 50, ratio: float = 0.3, reader: HeberReader | None = None) -> dict[str, int]:
    """Return suggested floors for continuous feeds keyed by feed name."""
    reader = reader or HeberReader()
    ref_day = (datetime.now(ET) - timedelta(days=days_back)).date()
    suggestions: dict[str, int] = {}
    for rule in DEFAULT_REGISTRY:
        if rule.kind != "continuous":
            continue
        try:
            df = reader.read_silver(
                rule.feed,
                time_range=(f"{ref_day.isoformat()}T00:00:00+00:00", f"{ref_day.isoformat()}T23:59:59+00:00"),
                columns=["ts_event"],
            )
        except Exception:
            logger.warning("calibrate_read_error", feed=rule.feed, exc_info=True)
            df = None
        if df is None or df.empty:
            suggestions[rule.feed] = 1
            continue
        counts = _bucket_counts(df["ts_event"], ref_day, rule, rule.lookback_minutes)
        suggestions[rule.feed] = suggest_floor_from_counts(counts, ratio=ratio)
    return suggestions


def calibrate_json(days_back: int = 50, ratio: float = 0.3) -> str:
    return json.dumps(calibrate(days_back=days_back, ratio=ratio))
