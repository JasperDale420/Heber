"""Barrier Checker - Detects TP/SL hits and computes final labels."""

from __future__ import annotations

import math
from datetime import UTC, datetime
from typing import Any

import numpy as np
import structlog

from heber.calendar import MarketCalendar
from heber.features.templates.alert_labels import SlippageModel
from heber.watch.manager import WatchManager
from heber.watch.models import (
    QUALITY_FLAG_LABEL_WINDOW_TRUNCATED,
    AlertWatch,
    WatchOutcome,
    WatchStatus,
    window_is_truncated,
)

logger = structlog.get_logger(__name__)


class BarrierChecker:
    """Checks if watches have hit TP/SL barriers and computes labels.

    This is called after new snapshots are added or on a schedule
    to determine which watches have reached an outcome.
    """

    def __init__(
        self,
        watch_manager: WatchManager,
        slippage_model: SlippageModel | None = None,
        calendar: MarketCalendar | None = None,
    ):
        """Initialize the checker.

        Args:
            watch_manager: WatchManager instance
            slippage_model: Optional execution cost model
            calendar: MarketCalendar instance (created if not provided)
        """
        self.manager = watch_manager
        self.slippage = slippage_model or SlippageModel()
        self.calendar = calendar or MarketCalendar()

    def check_all(self) -> list[WatchOutcome]:
        """Check all active watches for barrier hits.

        Returns:
            List of completed WatchOutcomes
        """
        active = self.manager.get_active_watches()
        outcomes = []

        for watch in active:
            outcome = self.check_watch(watch)
            if outcome:
                outcomes.append(outcome)

        return outcomes

    def check_watch(self, watch: AlertWatch) -> WatchOutcome | None:
        """Check a single watch for barrier hit.

        Args:
            watch: The watch to check

        Returns:
            WatchOutcome if complete, None if still watching
        """
        snapshots = sorted(self.manager.get_snapshots(watch.watch_id), key=lambda snap: snap.timestamp)

        now = datetime.now(UTC)
        alert_time = watch.alert_time if watch.alert_time.tzinfo is not None else watch.alert_time.replace(tzinfo=UTC)
        window_end = watch.window_end if watch.window_end.tzinfo is not None else watch.window_end.replace(tzinfo=UTC)

        if not snapshots:
            return self._handle_no_snapshots(watch, now, alert_time, window_end)

        # Build return path
        returns, return_timestamps = self._build_return_path(snapshots, watch.entry_price)

        if not returns:
            return self._handle_no_returns(watch, snapshots, now, alert_time, window_end)

        returns_arr = np.array(returns)

        # Compute MFE/MAE
        mfe = float(np.nanmax(returns_arr))
        mae = float(np.nanmin(returns_arr))

        # Check barriers
        status, bars_to_hit = self._check_barriers(
            returns_arr,
            watch.tp_threshold,
            watch.sl_threshold,
        )

        # Check expiry
        if status == WatchStatus.WATCHING and now >= window_end:
            status = WatchStatus.EXPIRED
            bars_to_hit = len(returns)

        # Still watching
        if status == WatchStatus.WATCHING:
            return None

        # Determine final return and outcome time
        outcome_return, barrier_time = self._resolve_outcome_return(returns, return_timestamps, bars_to_hit)
        outcome_time = window_end if status == WatchStatus.EXPIRED else (barrier_time or now)

        # Complete the watch
        self.manager.complete_watch(
            watch.watch_id,
            status,
            outcome_return,
            bars_to_hit,
            outcome_time=outcome_time,
        )

        outcome = self._build_outcome(
            watch,
            status,
            outcome_time,
            outcome_return,
            bars_to_hit,
            mfe,
            mae,
            alert_time,
            window_end,
        )

        logger.info(
            "Watch outcome determined",
            watch_id=watch.watch_id,
            status=status.value,
            hit_tp_first=outcome.hit_tp_first,
            trading_minutes=outcome.trading_minutes_to_hit,
            mfe=mfe,
            mae=mae,
        )

        return outcome

    @staticmethod
    def _build_return_path(
        snapshots: list,
        entry_price: float,
    ) -> tuple[list[float], list[datetime]]:
        """Compute return series from snapshots."""
        returns: list[float] = []
        timestamps: list[datetime] = []
        for snap in snapshots:
            if snap.mid_px is not None and entry_price > 0:
                ret = (snap.mid_px - entry_price) / entry_price
            elif snap.return_pct is not None:
                ret = float(snap.return_pct)
            else:
                continue
            if math.isfinite(ret):
                returns.append(ret)
                timestamps.append(snap.timestamp)
        return returns, timestamps

    def _handle_no_returns(
        self,
        watch: AlertWatch,
        snapshots: list,
        now: datetime,
        alert_time: datetime,
        window_end: datetime,
    ) -> WatchOutcome | None:
        """Handle the case where no valid returns were computed."""
        if now < window_end:
            return None

        bars_to_hit = len(snapshots)
        self.manager.complete_watch(
            watch.watch_id,
            WatchStatus.EXPIRED,
            0.0,
            bars_to_hit,
            outcome_time=window_end,
        )
        return self._build_outcome(
            watch,
            WatchStatus.EXPIRED,
            window_end,
            0.0,
            bars_to_hit,
            0.0,
            0.0,
            alert_time,
            window_end,
        )

    def _handle_no_snapshots(
        self,
        watch: AlertWatch,
        now: datetime,
        alert_time: datetime,
        window_end: datetime,
    ) -> WatchOutcome | None:
        """Expire watches with no snapshots once their window has ended."""
        if now < window_end:
            return None

        self.manager.complete_watch(
            watch.watch_id,
            WatchStatus.EXPIRED,
            0.0,
            bars_to_hit=0,
            outcome_time=window_end,
        )
        return self._build_outcome(
            watch,
            WatchStatus.EXPIRED,
            window_end,
            0.0,
            0,
            0.0,
            0.0,
            alert_time,
            window_end,
        )

    @staticmethod
    def _resolve_outcome_return(
        returns: list[float],
        return_timestamps: list[datetime],
        bars_to_hit: int | None,
    ) -> tuple[float, datetime | None]:
        """Determine final return and barrier hit time."""
        if bars_to_hit and bars_to_hit <= len(returns):
            return returns[bars_to_hit - 1], return_timestamps[bars_to_hit - 1]
        barrier_time = return_timestamps[-1] if return_timestamps else None
        return returns[-1], barrier_time

    def _build_outcome(
        self,
        watch: AlertWatch,
        status: WatchStatus,
        outcome_time: datetime,
        outcome_return: float,
        bars_to_hit: int | None,
        mfe: float,
        mae: float,
        alert_time: datetime,
        window_end: datetime,
    ) -> WatchOutcome:
        """Construct a WatchOutcome with trading-time metrics."""
        window_hours = (window_end - alert_time).total_seconds() / 3600
        trading_mins = self.calendar.trading_minutes_until(alert_time, outcome_time)

        return WatchOutcome(
            watch_id=watch.watch_id,
            alert_id=watch.alert_id,
            occ_symbol=watch.occ_symbol,
            underlying=watch.underlying,
            put_call=watch.put_call,
            horizon=watch.horizon,
            status=status,
            outcome_time=outcome_time,
            outcome_return=outcome_return,
            bars_to_hit=bars_to_hit,
            mfe=mfe,
            mae=mae,
            mfe_adj=self.slippage.adjust_option_mfe(mfe),
            mae_adj=self.slippage.adjust_option_mae(mae),
            hit_tp_first=1 if status == WatchStatus.HIT_TP else 0,
            entry_price=watch.entry_price,
            spot_at_alert=watch.spot_at_alert,
            alert_time=alert_time,
            window_duration_hours=window_hours,
            trading_minutes_to_hit=trading_mins,
        )

    def _check_barriers(
        self,
        returns: np.ndarray,
        tp_threshold: float,
        sl_threshold: float,
    ) -> tuple[WatchStatus, int | None]:
        """Check if TP or SL barrier has been hit.

        Args:
            returns: Array of return values
            tp_threshold: Take profit threshold
            sl_threshold: Stop loss threshold

        Returns:
            (status, bars_to_hit)
        """
        # Adjust TP for execution costs
        effective_tp = tp_threshold + self.slippage.option_roundtrip_cost_pct

        tp_hits = np.nonzero(returns >= effective_tp)[0]
        sl_hits = np.nonzero(returns <= -sl_threshold)[0]

        tp_first = tp_hits[0] if len(tp_hits) > 0 else float("inf")
        sl_first = sl_hits[0] if len(sl_hits) > 0 else float("inf")

        if tp_first < sl_first:
            return WatchStatus.HIT_TP, int(tp_first) + 1
        elif sl_first < tp_first:
            return WatchStatus.HIT_SL, int(sl_first) + 1
        elif tp_first != float("inf"):
            return WatchStatus.HIT_TP, int(tp_first) + 1
        else:
            return WatchStatus.WATCHING, None


def outcome_to_label_row(outcome: WatchOutcome) -> dict[str, Any]:
    """Convert WatchOutcome to a label row for Gold storage.

    Args:
        outcome: Completed watch outcome

    Returns:
        Dict suitable for DataFrame row
    """
    outcome_value = outcome.status.value if hasattr(outcome.status, "value") else str(outcome.status)

    return {
        # Identifiers
        "alert_id": outcome.alert_id,
        "watch_id": outcome.watch_id,
        "instrument_key": f"option:OCC:{outcome.occ_symbol}" if outcome.occ_symbol else f"equity:{outcome.underlying}",
        # Contract info
        "occ_symbol": outcome.occ_symbol,
        "underlying": outcome.underlying,
        "put_call": outcome.put_call,
        # Timing
        "ts_event": outcome.alert_time,
        "ts_available": outcome.outcome_time,
        "horizon": outcome.horizon,
        # THE LABEL
        "outcome": outcome_value,
        "hit_tp_first": outcome.hit_tp_first,
        "contract_hit_tp_first": outcome.hit_tp_first,
        "outcome_reason": outcome_value,
        # Path stats
        "mfe": outcome.mfe,
        "mae": outcome.mae,
        "bars_to_hit": outcome.bars_to_hit,
        "contract_mfe": outcome.mfe,
        "contract_mae": outcome.mae,
        "contract_mfe_adj": outcome.mfe_adj,
        "contract_mae_adj": outcome.mae_adj,
        "contract_bars_to_hit": outcome.bars_to_hit,
        "trading_minutes_to_hit": outcome.trading_minutes_to_hit,
        "outcome_return": outcome.outcome_return,
        # Context
        "entry_price": outcome.entry_price,
        "spot_at_alert": outcome.spot_at_alert,
        "window_duration_hours": outcome.window_duration_hours,
        # Stamped here rather than left to the reader. The meta-label builder
        # recomputes this, but it is not the only consumer — the ticker base
        # rates pipeline aggregates these rows straight off the lakehouse — so
        # a label resolved against a short window has to say so itself.
        "quality_flags": (
            [QUALITY_FLAG_LABEL_WINDOW_TRUNCATED]
            if window_is_truncated(outcome.horizon, outcome.window_duration_hours)
            else []
        ),
    }
