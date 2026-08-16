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
    """One alert on one day for one ticker — no prior history available."""

    def test_single_alert_has_nan_win_rate(self) -> None:
        """A single alert has no prior history so win_rate is NaN."""
        labels = _make_labels(
            [
                {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
            ]
        )
        result = compute_ticker_base_rates(labels)
        assert len(result) == 1
        assert np.isnan(result["ticker_win_rate_90d"].iloc[0])
        assert result["ticker_alert_frequency"].iloc[0] == 0

    def test_single_loss_has_nan_win_rate(self) -> None:
        labels = _make_labels(
            [
                {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 0},
            ]
        )
        result = compute_ticker_base_rates(labels)
        assert len(result) == 1
        assert np.isnan(result["ticker_win_rate_90d"].iloc[0])
        assert result["ticker_alert_frequency"].iloc[0] == 0


# ===========================================================================
# test_win_rate_calculation
# ===========================================================================


class TestWinRateCalculation:
    """Verify win rate is computed correctly for a known set of outcomes.

    Win rate excludes the current alert (no self-leakage), so the last alert
    sees N-1 prior alerts in its window.
    """

    def test_seven_out_of_ten(self) -> None:
        """10 alerts, 7 wins -> last alert sees 9 priors with 7 wins among first 9."""
        outcomes = [1, 1, 1, 1, 1, 1, 1, 0, 0, 0]
        labels = _make_multi_day_labels("AAPL", "2025-03-01", num_days=30, outcomes=outcomes)
        result = compute_ticker_base_rates(labels)

        # Last row sees 9 prior alerts: 7 wins in first 9 outcomes = 7/9
        last_row = result.sort_values("ts_event").iloc[-1]
        assert abs(last_row["ticker_win_rate_90d"] - 7 / 9) < 1e-10

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
        """10 alerts, 9 wins. Last alert sees 9 priors: 9 wins in 9 = 100% -> predictability 1.0.
        Second-to-last sees 8 priors: 8 wins in 8 = 100% -> predictability 1.0."""
        outcomes = [1] * 9 + [0]
        labels = _make_multi_day_labels("AAPL", "2025-03-01", num_days=30, outcomes=outcomes)
        result = compute_ticker_base_rates(labels)
        # Second-to-last row sees 8/8 = 100% win rate
        second_last = result.sort_values("ts_event").iloc[-2]
        assert abs(second_last["ticker_flow_predictability"] - 1.0) < 1e-10

    def test_ten_pct_win_rate(self) -> None:
        """10 alerts, 1 win (first). Last sees 9 priors: 1 win / 9 = 11.1% -> predictability 0.778."""
        outcomes = [0] * 9 + [1]
        labels = _make_multi_day_labels("AAPL", "2025-03-01", num_days=30, outcomes=outcomes)
        result = compute_ticker_base_rates(labels)
        last_row = result.sort_values("ts_event").iloc[-1]
        expected_wr = 0 / 9  # all 9 priors are losses
        expected_pred = abs(expected_wr - 0.5) * 2
        assert abs(last_row["ticker_flow_predictability"] - expected_pred) < 1e-10

    def test_perfect_win_rate(self) -> None:
        """100% win rate: last alert sees all-win priors -> predictability 1.0."""
        outcomes = [1] * 10
        labels = _make_multi_day_labels("AAPL", "2025-03-01", num_days=30, outcomes=outcomes)
        result = compute_ticker_base_rates(labels)
        last_row = result.sort_values("ts_event").iloc[-1]
        assert last_row["ticker_flow_predictability"] == 1.0


# ===========================================================================
# test_predictability_low
# ===========================================================================


class TestPredictabilityLow:
    """50% win rate should yield near-zero predictability."""

    def test_fifty_pct_win_rate(self) -> None:
        """Alternating wins/losses: last alert sees odd number of priors, near 50%."""
        outcomes = [1, 0] * 5
        labels = _make_multi_day_labels("AAPL", "2025-03-01", num_days=30, outcomes=outcomes)
        result = compute_ticker_base_rates(labels)
        last_row = result.sort_values("ts_event").iloc[-1]
        # Last alert sees 9 priors: [1,0,1,0,1,0,1,0,1] = 5/9 ≈ 0.556
        expected_wr = 5 / 9
        expected_pred = abs(expected_wr - 0.5) * 2
        assert abs(last_row["ticker_flow_predictability"] - expected_pred) < 1e-10


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

        # On the "today" row, the old alert should be excluded; only the -10d alert counts
        today_row = result[result["ts_event"] == base]
        assert len(today_row) == 1
        assert today_row["ticker_win_rate_90d"].iloc[0] == 0.0  # only the -10d alert (loss) is prior
        assert today_row["ticker_alert_frequency"].iloc[0] == 1

    def test_alert_exactly_90_days_included(self) -> None:
        """An alert exactly 90 days old should be included as a prior."""
        base = pd.Timestamp("2025-06-01", tz="UTC")
        rows = [
            {"underlying": "AAPL", "ts_event": base - timedelta(days=90), "hit_tp_first": 1},
            {"underlying": "AAPL", "ts_event": base, "hit_tp_first": 0},
        ]
        labels = _make_labels(rows)
        result = compute_ticker_base_rates(labels, window_days=90)

        today_row = result[result["ts_event"] == base]
        assert len(today_row) == 1
        # The -90d alert is within the window and serves as a prior
        assert today_row["ticker_alert_frequency"].iloc[0] == 1
        assert today_row["ticker_win_rate_90d"].iloc[0] == 1.0  # 1 prior win

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
        # No prior alerts within window
        assert today_row["ticker_alert_frequency"].iloc[0] == 0
        assert np.isnan(today_row["ticker_win_rate_90d"].iloc[0])


# ===========================================================================
# test_multi_ticker_independent
# ===========================================================================


class TestMultiTickerIndependent:
    """Two tickers should be computed independently."""

    def test_independent_computation(self) -> None:
        rows = [
            # AAPL: all wins
            {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
            {"underlying": "AAPL", "ts_event": "2025-06-02", "hit_tp_first": 1},
            # TSLA: all losses
            {"underlying": "TSLA", "ts_event": "2025-06-01", "hit_tp_first": 0},
            {"underlying": "TSLA", "ts_event": "2025-06-02", "hit_tp_first": 0},
        ]
        labels = _make_labels(rows)
        result = compute_ticker_base_rates(labels)

        aapl = result[result["instrument_key"] == "equity:AAPL"].sort_values("ts_event")
        tsla = result[result["instrument_key"] == "equity:TSLA"].sort_values("ts_event")

        # Last day for each ticker sees 1 prior alert
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

        # AAPL last alert sees 2 priors; TSLA last alert sees 0 priors
        assert aapl_last["ticker_alert_frequency"] == 2
        assert tsla_last["ticker_alert_frequency"] == 0


# ===========================================================================
# test_output_columns
# ===========================================================================


class TestOutputColumns:
    """Output DataFrame must contain the expected columns."""

    def test_required_columns_present(self) -> None:
        labels = _make_labels(
            [
                {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
                {"underlying": "AAPL", "ts_event": "2025-06-02", "hit_tp_first": 0},
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
                {"underlying": "AAPL", "ts_event": "2025-06-02", "hit_tp_first": 0},
            ]
        )
        result = compute_ticker_base_rates(labels)
        assert pd.api.types.is_datetime64_any_dtype(result["ts_event"])

    def test_ts_available_is_datetime(self) -> None:
        labels = _make_labels(
            [
                {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
                {"underlying": "AAPL", "ts_event": "2025-06-02", "hit_tp_first": 0},
            ]
        )
        result = compute_ticker_base_rates(labels)
        assert pd.api.types.is_datetime64_any_dtype(result["ts_available"])

    def test_instrument_key_format(self) -> None:
        """instrument_key should be in canonical format (equity:TICKER)."""
        labels = _make_labels(
            [
                {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
                {"underlying": "AAPL", "ts_event": "2025-06-02", "hit_tp_first": 0},
            ]
        )
        result = compute_ticker_base_rates(labels)
        assert all(result["instrument_key"].str.startswith("equity:"))


# ===========================================================================
# test_frequency_count
# ===========================================================================


class TestFrequencyCount:
    """Verify alert count matches expected values."""

    def test_same_day_alerts_have_zero_prior_frequency(self) -> None:
        """5 alerts at the same timestamp: each sees 0 prior alerts."""
        rows = [
            {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
            {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 0},
            {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
            {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 0},
            {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
        ]
        labels = _make_labels(rows)
        result = compute_ticker_base_rates(labels)
        # Deduplicated to 1 row (all same ts_event), frequency = 0
        assert result["ticker_alert_frequency"].iloc[0] == 0

    def test_cumulative_across_days(self) -> None:
        """Alerts from multiple days: last alert sees 2 prior alerts."""
        rows = [
            {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
            {"underlying": "AAPL", "ts_event": "2025-06-15", "hit_tp_first": 0},
            {"underlying": "AAPL", "ts_event": "2025-06-30", "hit_tp_first": 1},
        ]
        labels = _make_labels(rows)
        result = compute_ticker_base_rates(labels)

        last_row = result.sort_values("ts_event").iloc[-1]
        assert last_row["ticker_alert_frequency"] == 2  # 2 priors

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
                {"underlying": "AAPL", "ts_event": "2025-06-02", "hit_tp_first": 0},
            ]
        )
        result = compute_ticker_base_rates(labels)
        assert (result["instrument_key"] == "equity:AAPL").all()

    def test_canonical_key_preserved(self) -> None:
        """Already canonical 'equity:AAPL' should stay unchanged."""
        labels = _make_labels(
            [
                {"instrument_key": "equity:AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
                {"instrument_key": "equity:AAPL", "ts_event": "2025-06-02", "hit_tp_first": 0},
            ]
        )
        result = compute_ticker_base_rates(labels)
        assert (result["instrument_key"] == "equity:AAPL").all()

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
    """Predictability should always be in [0, 1] (where non-NaN)."""

    def test_bounds(self) -> None:
        outcomes = [1, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 1, 1, 0]
        labels = _make_multi_day_labels("AAPL", "2025-03-01", num_days=60, outcomes=outcomes)
        result = compute_ticker_base_rates(labels)
        non_nan = result.dropna(subset=["ticker_flow_predictability"])
        assert (non_nan["ticker_flow_predictability"] >= 0.0).all()
        assert (non_nan["ticker_flow_predictability"] <= 1.0).all()


# ===========================================================================
# test_quality_flag_filtering
# ===========================================================================


class TestQualityFlagFiltering:
    """Rows carrying quality_flags=['label_window_truncated'] must not count.

    These labels were resolved against a collapsed barrier-check window
    (see MarketCalendar.add_trading_hours session-boundary bug), so their
    hit_tp_first answers "did TP hit in ~3-5h" rather than the alert's real
    horizon. MetaLabelDatasetBuilder already excludes them as training
    targets; ticker_base_rates must exclude them as feature inputs too.
    """

    def test_flagged_alert_excluded_from_later_windows(self) -> None:
        """A flagged win must not count as a prior for a later clean alert."""
        rows = [
            {
                "underlying": "AAPL",
                "ts_event": "2025-06-01",
                "hit_tp_first": 1,
                "quality_flags": ["label_window_truncated"],
            },
            {"underlying": "AAPL", "ts_event": "2025-06-15", "hit_tp_first": 0},
        ]
        labels = _make_labels(rows)
        result = compute_ticker_base_rates(labels)

        clean_row = result[result["ts_event"] == pd.Timestamp("2025-06-15", tz="UTC")]
        assert len(clean_row) == 1
        # The flagged win must not be counted as a prior alert.
        assert clean_row["ticker_alert_frequency"].iloc[0] == 0
        assert np.isnan(clean_row["ticker_win_rate_90d"].iloc[0])

    def test_flagged_alert_has_no_own_output_row(self) -> None:
        """A flagged alert must not produce a ticker_base_rates row itself."""
        rows = [
            {
                "underlying": "AAPL",
                "ts_event": "2025-06-01",
                "hit_tp_first": 1,
                "quality_flags": ["label_window_truncated"],
            },
        ]
        labels = _make_labels(rows)
        result = compute_ticker_base_rates(labels)
        assert result.empty

    def test_mixed_flagged_and_clean_priors(self) -> None:
        """Window stats must only reflect clean priors, not flagged ones."""
        rows = [
            {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
            {
                "underlying": "AAPL",
                "ts_event": "2025-06-05",
                "hit_tp_first": 0,
                "quality_flags": ["label_window_truncated"],
            },
            {"underlying": "AAPL", "ts_event": "2025-06-10", "hit_tp_first": 1},
            {"underlying": "AAPL", "ts_event": "2025-06-20", "hit_tp_first": 0},
        ]
        labels = _make_labels(rows)
        result = compute_ticker_base_rates(labels)

        last_row = result.sort_values("ts_event").iloc[-1]
        # Only the 06-01 (win) and 06-10 (win) clean alerts should count as
        # priors for the 06-20 row — the flagged 06-05 loss is excluded, so
        # win rate is 2/2 = 1.0, not 2/3.
        assert last_row["ticker_alert_frequency"] == 2
        assert last_row["ticker_win_rate_90d"] == 1.0

    def test_missing_quality_flags_column_treated_as_clean(self) -> None:
        """No quality_flags column at all (legacy rows) means nothing is filtered."""
        rows = [
            {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
            {"underlying": "AAPL", "ts_event": "2025-06-02", "hit_tp_first": 0},
        ]
        labels = _make_labels(rows)
        assert "quality_flags" not in labels.columns
        result = compute_ticker_base_rates(labels)
        last_row = result.sort_values("ts_event").iloc[-1]
        assert last_row["ticker_alert_frequency"] == 1


# ===========================================================================
# test_no_self_leakage
# ===========================================================================


class TestNoSelfLeakage:
    """The current alert's own outcome must not be included in its own win rate."""

    def test_first_alert_no_prior(self) -> None:
        """First alert for a ticker has no prior history."""
        labels = _make_labels(
            [
                {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
            ]
        )
        result = compute_ticker_base_rates(labels)
        assert len(result) == 1
        assert result["ticker_alert_frequency"].iloc[0] == 0
        assert np.isnan(result["ticker_win_rate_90d"].iloc[0])

    def test_second_alert_sees_only_first(self) -> None:
        """Second alert should see only the first alert, not itself."""
        labels = _make_labels(
            [
                {"underlying": "AAPL", "ts_event": "2025-06-01", "hit_tp_first": 1},
                {"underlying": "AAPL", "ts_event": "2025-06-02", "hit_tp_first": 0},
            ]
        )
        result = compute_ticker_base_rates(labels)
        second = result.sort_values("ts_event").iloc[-1]
        # Sees 1 prior alert (the win)
        assert second["ticker_alert_frequency"] == 1
        assert second["ticker_win_rate_90d"] == 1.0  # 1/1 = 100%
