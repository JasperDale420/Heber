"""Barrier Checker - Detects TP/SL hits and computes final labels."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import numpy as np
import structlog

from heber.features.templates.alert_labels import SlippageModel
from heber.watch.manager import WatchManager
from heber.watch.models import (
    AlertWatch,
    WatchOutcome,
    WatchStatus,
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
    ):
        """Initialize the checker.

        Args:
            watch_manager: WatchManager instance
            slippage_model: Optional execution cost model
        """
        self.manager = watch_manager
        self.slippage = slippage_model or SlippageModel()

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
        snapshots = self.manager.get_snapshots(watch.watch_id)

        if not snapshots:
            return None

        # Build return path
        returns = []
        for snap in snapshots:
            if snap.mid_px and watch.entry_price > 0:
                ret = (snap.mid_px - watch.entry_price) / watch.entry_price
                returns.append(ret)
            elif snap.return_pct is not None:
                returns.append(snap.return_pct)

        if not returns:
            return None

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
        now = datetime.now(UTC)
        if status == WatchStatus.WATCHING and now >= watch.window_end:
            status = WatchStatus.EXPIRED
            bars_to_hit = len(returns)

        # Still watching
        if status == WatchStatus.WATCHING:
            return None

        # Determine final return
        if bars_to_hit and bars_to_hit <= len(returns):
            outcome_return = returns[bars_to_hit - 1]
        else:
            outcome_return = returns[-1]

        # Complete the watch
        self.manager.complete_watch(
            watch.watch_id,
            status,
            outcome_return,
            bars_to_hit,
        )

        # Build outcome
        window_hours = (watch.window_end - watch.alert_time).total_seconds() / 3600

        outcome = WatchOutcome(
            watch_id=watch.watch_id,
            alert_id=watch.alert_id,
            occ_symbol=watch.occ_symbol,
            underlying=watch.underlying,
            put_call=watch.put_call,
            horizon=watch.horizon,
            status=status,
            outcome_time=now,
            outcome_return=outcome_return,
            bars_to_hit=bars_to_hit,
            mfe=mfe,
            mae=mae,
            mfe_adj=self.slippage.adjust_option_mfe(mfe),
            mae_adj=self.slippage.adjust_option_mae(mae),
            hit_tp_first=1 if status == WatchStatus.HIT_TP else 0,
            entry_price=watch.entry_price,
            spot_at_alert=watch.spot_at_alert,
            alert_time=watch.alert_time,
            window_duration_hours=window_hours,
        )

        logger.info(
            "Watch outcome determined",
            watch_id=watch.watch_id,
            status=status.value,
            hit_tp_first=outcome.hit_tp_first,
            mfe=mfe,
            mae=mae,
        )

        return outcome

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
        else:
            return WatchStatus.WATCHING, None


def outcome_to_label_row(outcome: WatchOutcome) -> dict[str, Any]:
    """Convert WatchOutcome to a label row for Gold storage.

    Args:
        outcome: Completed watch outcome

    Returns:
        Dict suitable for DataFrame row
    """
    return {
        # Identifiers
        "alert_id": outcome.alert_id,
        "watch_id": outcome.watch_id,
        "instrument_key": outcome.alert_id,  # For Feast entity
        # Contract info
        "occ_symbol": outcome.occ_symbol,
        "underlying": outcome.underlying,
        "put_call": outcome.put_call,
        # Timing
        "ts_event": outcome.alert_time,
        "ts_available": outcome.outcome_time,
        "horizon": outcome.horizon,
        # THE LABEL
        "contract_hit_tp_first": outcome.hit_tp_first,
        "outcome_reason": outcome.status,
        # Path stats
        "contract_mfe": outcome.mfe,
        "contract_mae": outcome.mae,
        "contract_mfe_adj": outcome.mfe_adj,
        "contract_mae_adj": outcome.mae_adj,
        "contract_bars_to_hit": outcome.bars_to_hit,
        "outcome_return": outcome.outcome_return,
        # Context
        "entry_price": outcome.entry_price,
        "spot_at_alert": outcome.spot_at_alert,
        "window_duration_hours": outcome.window_duration_hours,
    }
