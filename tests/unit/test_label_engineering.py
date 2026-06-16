"""Unit tests for label engineering features: edge_ratio and time_efficiency.

Tests the two additional label-derived features added to the alert barrier
labels pipeline in _compute_barrier_outcome().

- edge_ratio: MFE / abs(MAE), capped at 10.0. Measures entry quality.
- time_efficiency: 1 - (bars_to_hit / max_window_bars). How quickly target was hit.
"""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from heber.features.templates.alert_labels import (
    _compute_barrier_outcome,
    compute_barrier_labels,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_price_path(entry: float, moves: list[float]) -> np.ndarray:
    """Build a price path from entry + cumulative moves.

    Args:
        entry: Starting price.
        moves: Sequence of absolute price changes from entry.

    Returns:
        np.ndarray of prices (NOT including the entry itself).
    """
    return np.array([entry + m for m in moves], dtype=float)


def _make_bars_df(
    instrument: str,
    prices: list[float],
    start_ts: pd.Timestamp | None = None,
    freq_minutes: int = 5,
) -> pd.DataFrame:
    """Build a minimal bars DataFrame with OHLC and ATR."""
    if start_ts is None:
        start_ts = pd.Timestamp("2025-06-01 09:30:00", tz="UTC")
    n = len(prices)
    timestamps = [start_ts + timedelta(minutes=freq_minutes * i) for i in range(n)]
    df = pd.DataFrame(
        {
            "instrument_key": instrument,
            "bar_start_ts": timestamps,
            "open": prices,
            "high": [p * 1.002 for p in prices],
            "low": [p * 0.998 for p in prices],
            "close": prices,
            "atr": [1.0] * n,  # Fixed ATR of 1.0 for simplicity
        }
    )
    return df


def _make_alert(
    underlying: str = "equity:AAPL",
    ts_event: pd.Timestamp | None = None,
    put_call: str = "C",
    expiry: date | None = None,
    spot_px: float = 100.0,
    event_id: str = "test-alert-1",
) -> pd.Series:
    """Build a minimal alert Series."""
    if ts_event is None:
        ts_event = pd.Timestamp("2025-06-01 09:35:00", tz="UTC")
    if expiry is None:
        expiry = date(2025, 6, 2)  # 1 DTE -> intraday
    return pd.Series(
        {
            "event_id": event_id,
            "underlying": underlying,
            "ts_event": ts_event,
            "put_call": put_call,
            "expiry": expiry,
            "spot_px": spot_px,
        }
    )


# ---------------------------------------------------------------------------
# edge_ratio unit tests
# ---------------------------------------------------------------------------


class TestEdgeRatio:
    """Tests for edge_ratio = MFE / abs(MAE), capped at 10.0."""

    def test_edge_ratio_winner(self) -> None:
        """MFE=0.05, MAE=-0.02 -> edge_ratio=2.5."""
        # Price path: entry=100, goes down 2% then up 5%
        # Returns: [-0.02, 0.05] so mfe=0.05, mae=-0.02
        price_path = _make_price_path(100.0, [-2.0, 5.0, 3.0])
        result = _compute_barrier_outcome(
            price_path=price_path,
            entry_price=100.0,
            is_call=True,
            tp_threshold=0.10,  # High TP so it won't hit
            sl_threshold=0.10,  # High SL so it won't hit
        )
        assert result["mfe"] == pytest.approx(0.05, abs=1e-6)
        assert result["mae"] == pytest.approx(-0.02, abs=1e-6)
        assert result["edge_ratio"] == pytest.approx(2.5, abs=1e-6)

    def test_edge_ratio_loser(self) -> None:
        """MFE=0.01, MAE=-0.04 -> edge_ratio=0.25."""
        price_path = _make_price_path(100.0, [1.0, -4.0, -2.0])
        result = _compute_barrier_outcome(
            price_path=price_path,
            entry_price=100.0,
            is_call=True,
            tp_threshold=0.10,
            sl_threshold=0.10,
        )
        assert result["mfe"] == pytest.approx(0.01, abs=1e-6)
        assert result["mae"] == pytest.approx(-0.04, abs=1e-6)
        assert result["edge_ratio"] == pytest.approx(0.25, abs=1e-6)

    def test_edge_ratio_zero_mae(self) -> None:
        """MFE=0.05, MAE=0 (never went against) -> edge_ratio capped at 10.0."""
        # Price only goes up from entry
        price_path = _make_price_path(100.0, [1.0, 3.0, 5.0])
        result = _compute_barrier_outcome(
            price_path=price_path,
            entry_price=100.0,
            is_call=True,
            tp_threshold=0.10,
            sl_threshold=0.10,
        )
        assert result["mfe"] == pytest.approx(0.05, abs=1e-6)
        assert result["mae"] == pytest.approx(0.01, abs=1e-6)
        # MAE is 0.01 (min of the favorable returns), not 0
        # For a pure up move, mae = min(returns) = 0.01 > 0
        # So edge_ratio = 0.05 / 0.01 = 5.0 ... but mae is positive.
        # Actually let's construct a path where mae is truly 0.
        # Entry=100, first bar is exactly 100 (return=0), rest go up.
        price_path2 = _make_price_path(100.0, [0.0, 3.0, 5.0])
        result2 = _compute_barrier_outcome(
            price_path=price_path2,
            entry_price=100.0,
            is_call=True,
            tp_threshold=0.10,
            sl_threshold=0.10,
        )
        assert result2["mae"] == pytest.approx(0.0, abs=1e-10)
        assert result2["mfe"] == pytest.approx(0.05, abs=1e-6)
        assert result2["edge_ratio"] == pytest.approx(10.0, abs=1e-6)

    def test_edge_ratio_both_zero(self) -> None:
        """MFE=0, MAE=0 -> edge_ratio=1.0 (neutral)."""
        # All bars at exactly entry price
        price_path = _make_price_path(100.0, [0.0, 0.0, 0.0])
        result = _compute_barrier_outcome(
            price_path=price_path,
            entry_price=100.0,
            is_call=True,
            tp_threshold=0.10,
            sl_threshold=0.10,
        )
        assert result["mfe"] == pytest.approx(0.0, abs=1e-10)
        assert result["mae"] == pytest.approx(0.0, abs=1e-10)
        assert result["edge_ratio"] == pytest.approx(1.0, abs=1e-6)

    def test_edge_ratio_capped_at_10(self) -> None:
        """Edge ratio should cap at 10.0 even when mae is near zero."""
        # Very small MAE, large MFE
        price_path = _make_price_path(100.0, [-0.001, 10.0, 8.0])
        result = _compute_barrier_outcome(
            price_path=price_path,
            entry_price=100.0,
            is_call=True,
            tp_threshold=0.50,
            sl_threshold=0.50,
        )
        # mfe = 0.10, mae = -0.00001 -> ratio would be huge
        assert result["edge_ratio"] <= 10.0

    def test_edge_ratio_empty_path(self) -> None:
        """Empty price path -> edge_ratio is NaN."""
        result = _compute_barrier_outcome(
            price_path=np.array([]),
            entry_price=100.0,
            is_call=True,
            tp_threshold=0.05,
            sl_threshold=0.05,
        )
        assert np.isnan(result["edge_ratio"])


# ---------------------------------------------------------------------------
# time_efficiency unit tests
# ---------------------------------------------------------------------------


class TestTimeEfficiency:
    """Tests for time_efficiency = 1 - (bars_to_hit / max_window_bars)."""

    def test_time_efficiency_quick_hit(self) -> None:
        """bars_to_hit=2, max_window=24 -> ~0.917."""
        # Create path that hits TP quickly (after 2 bars)
        # entry=100, TP threshold = 0.02 (2%), so need 102+
        # Bar 1: 100.5 (0.5%), Bar 2: 102.5 (2.5%) -> hits TP at bar 2
        price_path = _make_price_path(100.0, [0.5, 2.5, 1.0, 0.5])
        result = _compute_barrier_outcome(
            price_path=price_path,
            entry_price=100.0,
            is_call=True,
            tp_threshold=0.02,  # 2% TP
            sl_threshold=0.10,  # 10% SL (won't hit)
            max_window_bars=24,
        )
        assert result["hit_tp_first"] == 1
        assert result["bars_to_hit"] == 2  # Hit on second bar (1-indexed)
        assert result["time_efficiency"] == pytest.approx(1.0 - (2 / 24), abs=1e-6)

    def test_time_efficiency_slow_hit(self) -> None:
        """bars_to_hit=22, max_window=24 -> ~0.083."""
        # Build a path that meanders then hits TP late
        moves = [0.1 * i for i in range(22)]  # Gradually rise
        moves[-1] = 3.0  # Finally hit 3% at bar 22
        moves.extend([3.0, 3.0])  # Filler after
        price_path = _make_price_path(100.0, moves)
        result = _compute_barrier_outcome(
            price_path=price_path,
            entry_price=100.0,
            is_call=True,
            tp_threshold=0.029,  # Set so it just barely hits at bar 22
            sl_threshold=0.10,
            max_window_bars=24,
        )
        # The first bar to breach 2.9% is where price = 103.0 (move=3.0 at index 21)
        assert result["hit_tp_first"] == 1
        assert result["bars_to_hit"] == 22
        assert result["time_efficiency"] == pytest.approx(1.0 - (22 / 24), abs=1e-6)

    def test_time_efficiency_no_hit(self) -> None:
        """hit_tp_first=0 -> time_efficiency is NaN."""
        # Price goes sideways, never hits TP or SL
        price_path = _make_price_path(100.0, [0.1, -0.1, 0.1, -0.1])
        result = _compute_barrier_outcome(
            price_path=price_path,
            entry_price=100.0,
            is_call=True,
            tp_threshold=0.10,  # 10% TP (won't hit)
            sl_threshold=0.10,  # 10% SL (won't hit)
            max_window_bars=24,
        )
        assert result["hit_tp_first"] == 0
        assert np.isnan(result["time_efficiency"])

    def test_time_efficiency_last_bar(self) -> None:
        """bars_to_hit == max_window -> time_efficiency = 0.0."""
        # Hit TP at exactly the last bar
        max_window = 5
        moves = [0.1] * (max_window - 1) + [3.0]  # Hit TP on bar 5
        price_path = _make_price_path(100.0, moves)
        result = _compute_barrier_outcome(
            price_path=price_path,
            entry_price=100.0,
            is_call=True,
            tp_threshold=0.029,  # 2.9%, breached at 3.0%
            sl_threshold=0.10,
            max_window_bars=max_window,
        )
        assert result["hit_tp_first"] == 1
        assert result["bars_to_hit"] == max_window
        assert result["time_efficiency"] == pytest.approx(0.0, abs=1e-6)

    def test_time_efficiency_first_bar(self) -> None:
        """Hit TP on bar 1 -> time_efficiency close to 1.0."""
        price_path = _make_price_path(100.0, [5.0, 3.0, 1.0])
        result = _compute_barrier_outcome(
            price_path=price_path,
            entry_price=100.0,
            is_call=True,
            tp_threshold=0.02,
            sl_threshold=0.10,
            max_window_bars=24,
        )
        assert result["hit_tp_first"] == 1
        assert result["bars_to_hit"] == 1
        assert result["time_efficiency"] == pytest.approx(1.0 - (1 / 24), abs=1e-6)

    def test_time_efficiency_empty_path(self) -> None:
        """Empty price path -> time_efficiency is NaN."""
        result = _compute_barrier_outcome(
            price_path=np.array([]),
            entry_price=100.0,
            is_call=True,
            tp_threshold=0.05,
            sl_threshold=0.05,
            max_window_bars=24,
        )
        assert np.isnan(result["time_efficiency"])

    def test_time_efficiency_sl_hit(self) -> None:
        """SL hit (not TP) -> time_efficiency is NaN."""
        # Price drops hard immediately
        price_path = _make_price_path(100.0, [-5.0, -8.0, -3.0])
        result = _compute_barrier_outcome(
            price_path=price_path,
            entry_price=100.0,
            is_call=True,
            tp_threshold=0.10,
            sl_threshold=0.03,  # 3% SL, hit on bar 1 (-5%)
            max_window_bars=24,
        )
        assert result["hit_tp_first"] == 0
        assert np.isnan(result["time_efficiency"])


# ---------------------------------------------------------------------------
# Integration: features present in _compute_barrier_outcome output
# ---------------------------------------------------------------------------


class TestBarrierOutputIntegration:
    """Verify edge_ratio and time_efficiency flow through the barrier output."""

    def test_edge_ratio_in_barrier_output(self) -> None:
        """_compute_barrier_outcome result dict contains edge_ratio."""
        price_path = _make_price_path(100.0, [1.0, -2.0, 3.0])
        result = _compute_barrier_outcome(
            price_path=price_path,
            entry_price=100.0,
            is_call=True,
            tp_threshold=0.10,
            sl_threshold=0.10,
            max_window_bars=24,
        )
        assert "edge_ratio" in result
        assert isinstance(result["edge_ratio"], float)

    def test_time_efficiency_in_barrier_output(self) -> None:
        """_compute_barrier_outcome result dict contains time_efficiency."""
        price_path = _make_price_path(100.0, [1.0, -2.0, 3.0])
        result = _compute_barrier_outcome(
            price_path=price_path,
            entry_price=100.0,
            is_call=True,
            tp_threshold=0.10,
            sl_threshold=0.10,
            max_window_bars=24,
        )
        assert "time_efficiency" in result
        assert isinstance(result["time_efficiency"], float) or np.isnan(result["time_efficiency"])

    def test_features_in_compute_barrier_labels(self) -> None:
        """compute_barrier_labels output contains both new columns."""
        ts_base = pd.Timestamp("2025-06-01 09:30:00", tz="UTC")
        underlying = "equity:AAPL"

        # Build bars: 30 bars at 5-min intervals, steadily rising
        prices = [100.0 + i * 0.5 for i in range(30)]
        bars = _make_bars_df(underlying, prices, start_ts=ts_base, freq_minutes=5)

        alerts = pd.DataFrame(
            [
                {
                    "event_id": "a1",
                    "underlying": underlying,
                    "ts_event": ts_base + timedelta(minutes=10),
                    "put_call": "C",
                    "expiry": date(2025, 6, 2),
                    "spot_px": prices[2],
                }
            ]
        )

        result_df = compute_barrier_labels(alerts, bars)
        assert "edge_ratio" in result_df.columns
        assert "time_efficiency" in result_df.columns

    def test_empty_result_has_new_fields(self) -> None:
        """When there's no data, the empty result should still have the new fields as NaN."""
        ts_base = pd.Timestamp("2025-06-01 09:30:00", tz="UTC")
        underlying = "equity:AAPL"

        # Bars for a DIFFERENT instrument so alert can't find data
        bars = _make_bars_df("equity:MSFT", [100.0] * 10, start_ts=ts_base)

        alerts = pd.DataFrame(
            [
                {
                    "event_id": "a1",
                    "underlying": underlying,
                    "ts_event": ts_base + timedelta(minutes=10),
                    "put_call": "C",
                    "expiry": date(2025, 6, 2),
                    "spot_px": 100.0,
                }
            ]
        )

        result_df = compute_barrier_labels(alerts, bars)
        assert "edge_ratio" in result_df.columns
        assert "time_efficiency" in result_df.columns
        assert np.isnan(result_df.iloc[0]["edge_ratio"])
        assert np.isnan(result_df.iloc[0]["time_efficiency"])
