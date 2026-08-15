"""Alert Watch Models - Pydantic models for tracking active alerts.

The watch list tracks flow alerts that need ongoing monitoring to determine
if they would have produced acceptable trade outcomes.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _normalize_datetime_utc(value: datetime | None) -> datetime | None:
    """Normalize datetimes to UTC and treat naive values as UTC."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


class WatchStatus(StrEnum):
    """Status of an alert watch."""

    WATCHING = "watching"  # Actively being tracked
    HIT_TP = "hit_tp"  # Take profit barrier hit
    HIT_SL = "hit_sl"  # Stop loss barrier hit
    EXPIRED = "expired"  # Window ended without barrier hit
    ERROR = "error"  # Failed to track (e.g., no quotes available)


class WatchHorizon(StrEnum):
    """Trading horizon classification."""

    INTRADAY = "intraday"  # 0-2 DTE, 5-min polling, 4h max
    SWING = "swing"  # 3-21 DTE, 15-min polling, 5d max
    LEAP = "leap"  # 22+ DTE, 1h polling, 30d max


class AlertWatch(BaseModel):
    """A watch entry for tracking an alert's outcome.

    This is stored in Redis and updated as new quotes arrive.
    """

    watch_id: str = Field(..., description="Unique watch identifier (UUID)")
    alert_id: str = Field(..., description="Original UW alert ID")

    # Contract identification
    occ_symbol: str = Field(..., description="OCC option symbol")
    underlying: str = Field(..., description="Underlying stock ticker")
    put_call: str = Field(..., description="C or P")
    expiry: str = Field(..., description="Option expiry date (YYYY-MM-DD)")
    strike: float = Field(..., description="Strike price")

    # Entry prices
    entry_price: float = Field(..., description="Option mid at alert time")
    spot_at_alert: float = Field(..., description="Underlying price at alert")
    atr_at_alert: float | None = Field(None, description="ATR for underlying at alert")

    # Timing
    alert_time: datetime = Field(..., description="When alert fired")
    window_end: datetime = Field(..., description="When to stop watching")
    horizon: WatchHorizon = Field(..., description="Trading horizon")

    # Barrier thresholds
    tp_threshold: float = Field(..., description="Take profit threshold (decimal)")
    sl_threshold: float = Field(..., description="Stop loss threshold (decimal)")

    # Tracking state
    status: WatchStatus = Field(default=WatchStatus.WATCHING)
    current_price: float | None = Field(None, description="Latest option price")
    current_return: float | None = Field(None, description="Current return")
    mfe: float | None = Field(None, description="Max favorable excursion so far")
    mae: float | None = Field(None, description="Max adverse excursion so far")
    snapshot_count: int = Field(default=0, description="Number of snapshots collected")

    # Outcome (populated when complete)
    outcome_time: datetime | None = Field(None, description="When outcome determined")
    outcome_return: float | None = Field(None, description="Final return at outcome")
    bars_to_hit: int | None = Field(None, description="Snapshots until barrier hit")

    # Metadata
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    model_config = ConfigDict(use_enum_values=True)

    @field_validator("alert_time", "window_end", "outcome_time", "created_at", "updated_at", mode="after")
    @classmethod
    def _normalize_datetimes(cls, value: datetime | None) -> datetime | None:
        return _normalize_datetime_utc(value)


class WatchSnapshot(BaseModel):
    """A single price snapshot for a watched contract."""

    watch_id: str
    occ_symbol: str
    timestamp: datetime

    # Prices
    bid_px: float | None = None
    ask_px: float | None = None
    mid_px: float | None = None
    last_px: float | None = None

    # Underlying context
    underlying_price: float | None = None

    # Computed
    return_pct: float | None = None  # (mid - entry) / entry

    @field_validator("timestamp", mode="after")
    @classmethod
    def _normalize_timestamp(cls, value: datetime) -> datetime:
        normalized = _normalize_datetime_utc(value)
        if normalized is None:
            raise ValueError("timestamp must not be null")
        return normalized


class WatchOutcome(BaseModel):
    """Final outcome when watch completes."""

    watch_id: str
    alert_id: str
    occ_symbol: str
    underlying: str
    put_call: str
    horizon: WatchHorizon

    # Outcome
    status: WatchStatus
    outcome_time: datetime
    outcome_return: float
    bars_to_hit: int | None

    # Path statistics
    mfe: float
    mae: float
    mfe_adj: float | None = None  # Adjusted for slippage
    mae_adj: float | None = None

    # Label (the key output)
    hit_tp_first: int  # 1 if HIT_TP, 0 otherwise

    # Context
    entry_price: float
    spot_at_alert: float
    alert_time: datetime
    window_duration_hours: float
    trading_minutes_to_hit: int | None = None  # Market time to barrier

    @field_validator("outcome_time", "alert_time", mode="after")
    @classmethod
    def _normalize_datetimes(cls, value: datetime) -> datetime:
        normalized = _normalize_datetime_utc(value)
        if normalized is None:
            raise ValueError("datetime value must not be null")
        return normalized


# Redis key patterns
class WatchKeys:
    """Redis key patterns for watch storage."""

    # Hash: watch:{watch_id} -> AlertWatch JSON
    WATCH = "heber:watch:{watch_id}"

    # Set: watches:active -> set of active watch_ids
    ACTIVE_WATCHES = "heber:watches:active"

    # Set: watches:by_symbol:{occ_symbol} -> set of watch_ids for this contract
    BY_SYMBOL = "heber:watches:by_symbol:{occ_symbol}"

    # Sorted set: watches:expiring -> (watch_id, window_end_timestamp)
    EXPIRING = "heber:watches:expiring"

    # List: snapshots:{watch_id} -> list of WatchSnapshot JSON
    SNAPSHOTS = "heber:snapshots:{watch_id}"

    @classmethod
    def watch_key(cls, watch_id: str) -> str:
        return cls.WATCH.format(watch_id=watch_id)

    @classmethod
    def by_symbol_key(cls, occ_symbol: str) -> str:
        return cls.BY_SYMBOL.format(occ_symbol=occ_symbol)

    @classmethod
    def snapshots_key(cls, watch_id: str) -> str:
        return cls.SNAPSHOTS.format(watch_id=watch_id)


# The row's observation window closed before its horizon's nominal span had
# elapsed, so its outcome describes a shorter horizon than the one it is filed
# under. `expired` in particular means "no barrier touched in the truncated
# window", not "no barrier touched in the nominal one"; a barrier that was
# touched is a real touch, but the touch population is selected on happening
# before the early close.
QUALITY_FLAG_LABEL_WINDOW_TRUNCATED = "label_window_truncated"

# One regular XNYS session. `max_duration_hours` below is spent by
# `MarketCalendar.add_trading_hours` at this rate.
TRADING_HOURS_PER_SESSION = 6.5

# Polling configuration per horizon.
#
# `max_duration_hours` is in *trading* hours, the unit add_trading_hours spends.
# These were previously 120 and 720 — 5 x 24 and 30 x 24, i.e. calendar hours —
# which the calendar then spent at 6.5 hours per session, making a SWING window
# 18.5 sessions (~26 calendar days) and a LEAP window 110.8 sessions (~5 months).
# Both routinely outlived the contract being watched, since SWING alerts are
# 3-21 DTE and LEAP 22+. The day counts are now expressed in sessions.
#
# Changing these changes the horizon every future label answers. Rows already
# written carry `label_window_truncated` in `quality_flags`, so lowering the
# nominal does not un-exclude them.
POLL_CONFIG = {
    WatchHorizon.INTRADAY: {
        "interval_seconds": 300,  # 5 minutes
        "max_duration_hours": 4,  # most of one session
    },
    WatchHorizon.SWING: {
        "interval_seconds": 900,  # 15 minutes
        "max_duration_hours": 5 * TRADING_HOURS_PER_SESSION,  # 5 trading days
    },
    WatchHorizon.LEAP: {
        "interval_seconds": 3600,  # 1 hour
        "max_duration_hours": 30 * TRADING_HOURS_PER_SESSION,  # 30 trading days
    },
}


def nominal_window_hours(horizon: WatchHorizon | str) -> float:
    """Trading hours a watch of this horizon is meant to observe.

    Raises ``KeyError`` for an unknown horizon: a window that cannot be measured
    against anything must not be silently treated as sound.
    """
    return float(POLL_CONFIG[WatchHorizon(horizon)]["max_duration_hours"])


def window_is_truncated(horizon: WatchHorizon | str, window_duration_hours: float | None) -> bool:
    """True if this window closed short of what its horizon promised.

    ``window_duration_hours`` is wall-clock ``window_end - alert_time``, and
    ``add_trading_hours`` only ever advances its cursor — it skips non-trading
    time rather than consuming it. A correct window therefore always spans at
    least its nominal count of hours in wall clock, so anything shorter cannot
    have come from an intact calendar. A window that cannot be checked is
    reported as truncated rather than assumed sound.
    """
    if window_duration_hours is None:
        return True
    try:
        return not window_duration_hours >= nominal_window_hours(horizon)
    except (KeyError, ValueError):
        return True
