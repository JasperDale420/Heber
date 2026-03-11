"""Unit tests for ticker base rates compute function.

Tests the standalone compute_ticker_base_rates function independently with synthetic DataFrames.
Does NOT test the TickerBaseRatesPipeline class (that requires a real Heber volume).
"""

from __future__ import annotations

from datetime import timedelta

import numpy as np
import pandas as pd

from heber.features.pipelines.ticker_base_rates import (
    EXPECTED_OUTPUT_COLUMNS,
    compute_ticker_base_rates,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_labels(
    rows: list[dict],
) -> pd.DataFrame:
    """Build a labels DataFrame from a list of row dicts.

    Each row should have at minimum: underlying (or instrument_key), ts_event, hit_tp_first.
    Missing alert_id values are auto-generated.
    """
    df = pd.DataFrame(rows)
    if "alert_id" not in df.columns:
        df["alert_id"] = [f"alert_{i}" for i in range(len(df))]
    if "ts_event" in df.columns:
        df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    return df


def _make_multi_day_labels(
    ticker: str,
    start: str,
    num_days: int,
    outcomes: list[int],
) -> pd.DataFrame:
    """Create labels spread across multiple days for a single ticker.

    Args:
        ticker: Underlying symbol (e.g., "AAPL" or "equity:AAPL").
        start: ISO date for first alert.
        num_days: Number of calendar days to spread alerts across.
        outcomes: List of hit_tp_first values (0 or 1), one per alert.
    """
    base = pd.Timestamp(start, tz="UTC")
    rows = []
    for i, outcome in enumerate(outcomes):
        day_offset = int(i * num_days / max(len(outcomes), 1))
        rows.append(
            {
                "underlying": ticker,
                "ts_event": base + timedelta(days=day_offset),
                "hit_tp_first": outcome,
            }
        )
    return _make_labels(rows)


# ===========================================================================
# test_empty_input
# ===========================================================================


class TestEmptyInput:
    """Empty DataFrame should produce empty output with correct schema."""

    def test_empty_dataframe(self) -> None:
        result = compute_ticker_base_rates(pd.DataFrame())
        assert result.empty
        assert set(EXPECTED_OUTPUT_COLUMNS).issubset(set(result.columns))

    def test_empty_with_columns(self) -> None:
        df = pd.DataFrame(columns=["underlying", "ts_event", "hit_tp_first"])
        result = compute_ticker_base_rates(df)
        assert result.empty


# ===========================================================================
# test_single_ticker_single_day
# ===========================================================================


class TestSingleTickerSingleDay:
    """One alert on one day for one ticker."""

    def test_single_win(self) -> None:
        labels = _make_labels(
            [
                {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
            ]
        )
        result = compute_ticker_base_rates(labels)
        assert len(result) == 1
        assert result["ticker_win_rate_90d"].iloc[0] == 1.0
        assert result["ticker_alert_frequency"].iloc[0] == 1

    def test_single_loss(self) -> None:
        labels = _make_labels(
            [
                {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 0},
            ]
        )
        result = compute_ticker_base_rates(labels)
        assert len(result) == 1
        assert result["ticker_win_rate_90d"].iloc[0] == 0.0
        assert result["ticker_alert_frequency"].iloc[0] == 1


# ===========================================================================
# test_win_rate_calculation
# ===========================================================================


class TestWinRateCalculation:
    """Verify win rate is computed correctly for a known set of outcomes."""

    def test_seven_out_of_ten(self) -> None:
        """10 alerts, 7 wins -> 0.7 win rate."""
        outcomes = [1, 1, 1, 1, 1, 1, 1, 0, 0, 0]
        labels = _make_multi_day_labels("AAPL", "2025-03-01", num_days=30, outcomes=outcomes)
        result = compute_ticker_base_rates(labels)

        # The last date should see all 10 alerts in its 90-day lookback
        last_row = result.sort_values("ts_event").iloc[-1]
        assert abs(last_row["ticker_win_rate_90d"] - 0.7) < 1e-10

    def test_all_wins(self) -> None:
        outcomes = [1, 1, 1, 1, 1]
        labels = _make_multi_day_labels("TSLA", "2025-03-01", num_days=10, outcomes=outcomes)
        result = compute_ticker_base_rates(labels)
        last_row = result.sort_values("ts_event").iloc[-1]
        assert last_row["ticker_win_rate_90d"] == 1.0

    def test_all_losses(self) -> None:
        outcomes = [0, 0, 0, 0, 0]
        labels = _make_multi_day_labels("TSLA", "2025-03-01", num_days=10, outcomes=outcomes)
        result = compute_ticker_base_rates(labels)
        last_row = result.sort_values("ts_event").iloc[-1]
        assert last_row["ticker_win_rate_90d"] == 0.0


# ===========================================================================
# test_predictability_high
# ===========================================================================


class TestPredictabilityHigh:
    """High win rate or loss rate should yield high predictability."""

    def test_ninety_pct_win_rate(self) -> None:
        """90% win rate -> predictability = |0.9 - 0.5| * 2 = 0.8."""
        outcomes = [1] * 9 + [0]
        labels = _make_multi_day_labels("AAPL", "2025-03-01", num_days=30, outcomes=outcomes)
        result = compute_ticker_base_rates(labels)
        last_row = result.sort_values("ts_event").iloc[-1]
        assert abs(last_row["ticker_flow_predictability"] - 0.8) < 1e-10

    def test_ten_pct_win_rate(self) -> None:
        """10% win rate -> predictability = |0.1 - 0.5| * 2 = 0.8 (symmetric)."""
        outcomes = [0] * 9 + [1]
        labels = _make_multi_day_labels("AAPL", "2025-03-01", num_days=30, outcomes=outcomes)
        result = compute_ticker_base_rates(labels)
        last_row = result.sort_values("ts_event").iloc[-1]
        assert abs(last_row["ticker_flow_predictability"] - 0.8) < 1e-10

    def test_perfect_win_rate(self) -> None:
        """100% win rate -> predictability = 1.0."""
        outcomes = [1] * 10
        labels = _make_multi_day_labels("AAPL", "2025-03-01", num_days=30, outcomes=outcomes)
        result = compute_ticker_base_rates(labels)
        last_row = result.sort_values("ts_event").iloc[-1]
        assert last_row["ticker_flow_predictability"] == 1.0


# ===========================================================================
# test_predictability_low
# ===========================================================================


class TestPredictabilityLow:
    """50% win rate should yield zero predictability."""

    def test_fifty_pct_win_rate(self) -> None:
        """50% win rate -> predictability = |0.5 - 0.5| * 2 = 0.0."""
        outcomes = [1, 0] * 5
        labels = _make_multi_day_labels("AAPL", "2025-03-01", num_days=30, outcomes=outcomes)
        result = compute_ticker_base_rates(labels)
        last_row = result.sort_values("ts_event").iloc[-1]
        assert abs(last_row["ticker_flow_predictability"] - 0.0) < 1e-10


# ===========================================================================
# test_rolling_window_90d
# ===========================================================================


class TestRollingWindow90d:
    """Alerts older than 90 days should be excluded from calculation."""

    def test_old_alerts_excluded(self) -> None:
        """An alert from 100 days ago should not count in a row computed today."""
        base = pd.Timestamp("2025-06-01", tz="UTC")
        rows = [
            # Old alert: 100 days before "today" (2025-06-01)
            {"underlying": "AAPL", "ts_event": base - timedelta(days=100), "hit_tp_first": 1},
            # Recent alert: 10 days before "today"
            {"underlying": "AAPL", "ts_event": base - timedelta(days=10), "hit_tp_first": 0},
            # "Today"
            {"underlying": "AAPL", "ts_event": base, "hit_tp_first": 0},
        ]
        labels = _make_labels(rows)
        result = compute_ticker_base_rates(labels, window_days=90)

        # On the "today" row, the old alert should be excluded
        today_row = result[result["ts_event"] == base]
        assert len(today_row) == 1
        # Only the two recent alerts should count (both are 0)
        assert today_row["ticker_win_rate_90d"].iloc[0] == 0.0
        assert today_row["ticker_alert_frequency"].iloc[0] == 2

    def test_alert_exactly_90_days_included(self) -> None:
        """An alert exactly 90 days old should be included."""
        base = pd.Timestamp("2025-06-01", tz="UTC")
        rows = [
            {"underlying": "AAPL", "ts_event": base - timedelta(days=90), "hit_tp_first": 1},
            {"underlying": "AAPL", "ts_event": base, "hit_tp_first": 0},
        ]
        labels = _make_labels(rows)
        result = compute_ticker_base_rates(labels, window_days=90)

        today_row = result[result["ts_event"] == base]
        assert len(today_row) == 1
        # Both alerts are within the window
        assert today_row["ticker_alert_frequency"].iloc[0] == 2
        assert today_row["ticker_win_rate_90d"].iloc[0] == 0.5

    def test_alert_91_days_excluded(self) -> None:
        """An alert 91 days old should be excluded."""
        base = pd.Timestamp("2025-06-01", tz="UTC")
        rows = [
            {"underlying": "AAPL", "ts_event": base - timedelta(days=91), "hit_tp_first": 1},
            {"underlying": "AAPL", "ts_event": base, "hit_tp_first": 0},
        ]
        labels = _make_labels(rows)
        result = compute_ticker_base_rates(labels, window_days=90)

        today_row = result[result["ts_event"] == base]
        assert len(today_row) == 1
        # Only the "today" alert is within the window
        assert today_row["ticker_alert_frequency"].iloc[0] == 1
        assert today_row["ticker_win_rate_90d"].iloc[0] == 0.0


# ===========================================================================
# test_multi_ticker_independent
# ===========================================================================


class TestMultiTickerIndependent:
    """Two tickers should be computed independently."""

    def test_independent_computation(self) -> None:
        rows = [
            # AAPL: 100% win rate
            {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
            {"underlying": "AAPL", "ts_event": "2025-06-02", "hit_tp_first": 1},
            # TSLA: 0% win rate
            {"underlying": "TSLA", "ts_event": "2025-06-01", "hit_tp_first": 0},
            {"underlying": "TSLA", "ts_event": "2025-06-02", "hit_tp_first": 0},
        ]
        labels = _make_labels(rows)
        result = compute_ticker_base_rates(labels)

        aapl = result[result["instrument_key"] == "equity:AAPL"].sort_values("ts_event")
        tsla = result[result["instrument_key"] == "equity:TSLA"].sort_values("ts_event")

        # Last day for each ticker
        aapl_last = aapl.iloc[-1]
        tsla_last = tsla.iloc[-1]

        assert aapl_last["ticker_win_rate_90d"] == 1.0
        assert tsla_last["ticker_win_rate_90d"] == 0.0

    def test_different_frequencies(self) -> None:
        """Different tickers can have different alert counts."""
        rows = [
            {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
            {"underlying": "AAPL", "ts_event": "2025-06-02", "hit_tp_first": 0},
            {"underlying": "AAPL", "ts_event": "2025-06-03", "hit_tp_first": 1},
            {"underlying": "TSLA", "ts_event": "2025-06-03", "hit_tp_first": 0},
        ]
        labels = _make_labels(rows)
        result = compute_ticker_base_rates(labels)

        aapl_last = result[result["instrument_key"] == "equity:AAPL"].sort_values("ts_event").iloc[-1]
        tsla_last = result[result["instrument_key"] == "equity:TSLA"].sort_values("ts_event").iloc[-1]

        assert aapl_last["ticker_alert_frequency"] == 3
        assert tsla_last["ticker_alert_frequency"] == 1


# ===========================================================================
# test_output_columns
# ===========================================================================


class TestOutputColumns:
    """Output DataFrame must contain the expected columns."""

    def test_required_columns_present(self) -> None:
        labels = _make_labels(
            [
                {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
            ]
        )
        result = compute_ticker_base_rates(labels)
        expected = {
            "instrument_key",
            "ts_event",
            "ts_available",
            "ticker_win_rate_90d",
            "ticker_alert_frequency",
            "ticker_flow_predictability",
        }
        assert expected.issubset(set(result.columns))

    def test_ts_event_is_datetime(self) -> None:
        labels = _make_labels(
            [
                {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
            ]
        )
        result = compute_ticker_base_rates(labels)
        assert pd.api.types.is_datetime64_any_dtype(result["ts_event"])

    def test_ts_available_is_datetime(self) -> None:
        labels = _make_labels(
            [
                {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
            ]
        )
        result = compute_ticker_base_rates(labels)
        assert pd.api.types.is_datetime64_any_dtype(result["ts_available"])

    def test_instrument_key_format(self) -> None:
        """instrument_key should be in canonical format (equity:TICKER)."""
        labels = _make_labels(
            [
                {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
            ]
        )
        result = compute_ticker_base_rates(labels)
        assert all(result["instrument_key"].str.startswith("equity:"))


# ===========================================================================
# test_frequency_count
# ===========================================================================


class TestFrequencyCount:
    """Verify alert count matches expected values."""

    def test_exact_count(self) -> None:
        """5 alerts on same day -> frequency = 5."""
        rows = [
            {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
            {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 0},
            {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
            {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 0},
            {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
        ]
        labels = _make_labels(rows)
        result = compute_ticker_base_rates(labels)
        assert result["ticker_alert_frequency"].iloc[0] == 5

    def test_cumulative_across_days(self) -> None:
        """Alerts from multiple days within the window should all count."""
        rows = [
            {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
            {"underlying": "AAPL", "ts_event": "2025-06-15", "hit_tp_first": 0},
            {"underlying": "AAPL", "ts_event": "2025-06-30", "hit_tp_first": 1},
        ]
        labels = _make_labels(rows)
        result = compute_ticker_base_rates(labels)

        last_row = result.sort_values("ts_event").iloc[-1]
        assert last_row["ticker_alert_frequency"] == 3

    def test_frequency_is_integer(self) -> None:
        """ticker_alert_frequency should be integer-typed."""
        labels = _make_labels(
            [
                {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
                {"underlying": "AAPL", "ts_event": "2025-06-02", "hit_tp_first": 0},
            ]
        )
        result = compute_ticker_base_rates(labels)
        assert result["ticker_alert_frequency"].dtype in (np.int64, np.int32, int, pd.Int64Dtype())


# ===========================================================================
# test_instrument_key_normalization
# ===========================================================================


class TestInstrumentKeyNormalization:
    """Handles both raw tickers and canonical equity:TICKER format."""

    def test_raw_ticker_normalized(self) -> None:
        """Raw ticker 'AAPL' should become 'equity:AAPL'."""
        labels = _make_labels(
            [
                {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
            ]
        )
        result = compute_ticker_base_rates(labels)
        assert result["instrument_key"].iloc[0] == "equity:AAPL"

    def test_canonical_key_preserved(self) -> None:
        """Already canonical 'equity:AAPL' should stay unchanged."""
        labels = _make_labels(
            [
                {"instrument_key": "equity:AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
            ]
        )
        result = compute_ticker_base_rates(labels)
        assert result["instrument_key"].iloc[0] == "equity:AAPL"

    def test_mixed_formats_unified(self) -> None:
        """Mix of raw and canonical should unify to canonical."""
        labels = _make_labels(
            [
                {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
                {"instrument_key": "equity:AAPL", "ts_event": "2025-06-02", "hit_tp_first": 0},
            ]
        )
        result = compute_ticker_base_rates(labels)
        assert (result["instrument_key"] == "equity:AAPL").all()


# ===========================================================================
# test_predictability_bounds
# ===========================================================================


class TestPredictabilityBounds:
    """Predictability should always be in [0, 1]."""

    def test_bounds(self) -> None:
        outcomes = [1, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 1, 1, 0]
        labels = _make_multi_day_labels("AAPL", "2025-03-01", num_days=60, outcomes=outcomes)
        result = compute_ticker_base_rates(labels)
        assert (result["ticker_flow_predictability"] >= 0.0).all()
        assert (result["ticker_flow_predictability"] <= 1.0).all()
