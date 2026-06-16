"""Unit tests for flow context feature compute functions.

Tests the standalone compute_flow_context_features function with synthetic DataFrames.
Does NOT test the FlowContextPipeline class (that requires a real Heber volume).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from heber.features.pipelines.flow_context_features import compute_flow_context_features

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_alert(
    event_id: str,
    underlying: str,
    ts_event: str | pd.Timestamp,
    put_call: str = "C",
) -> dict:
    """Create a single flow alert row dict."""
    ts = pd.Timestamp(ts_event) if isinstance(ts_event, str) else ts_event
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return {
        "event_id": event_id,
        "underlying": underlying,
        "ts_event": ts,
        "put_call": put_call,
    }


def _alerts_df(alerts: list[dict]) -> pd.DataFrame:
    """Convert list of alert dicts to DataFrame."""
    df = pd.DataFrame(alerts)
    if not df.empty:
        df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    return df


# ===========================================================================
# test_empty_input
# ===========================================================================


class TestEmptyInput:
    """Empty input should produce empty output with correct columns."""

    def test_empty_dataframe(self) -> None:
        result = compute_flow_context_features(pd.DataFrame())
        assert result.empty

    def test_empty_has_expected_columns(self) -> None:
        result = compute_flow_context_features(pd.DataFrame())
        expected_cols = {
            "alert_id",
            "instrument_key",
            "ts_event",
            "ts_available",
            "same_ticker_alerts_1h",
            "directional_agreement_4h",
            "repeat_ticker_days_5d",
        }
        assert expected_cols == set(result.columns)


# ===========================================================================
# test_single_alert
# ===========================================================================


class TestSingleAlert:
    """A single alert alone has no prior history."""

    def test_single_alert_values(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 10:00:00", "C"),
            ]
        )
        result = compute_flow_context_features(alerts)

        assert len(result) == 1
        row = result.iloc[0]
        assert row["alert_id"] == "a1"
        assert row["same_ticker_alerts_1h"] == 0
        assert row["directional_agreement_4h"] == 0.5  # default
        assert row["repeat_ticker_days_5d"] == 0


# ===========================================================================
# test_two_alerts_same_ticker_within_1h
# ===========================================================================


class TestTwoAlertsSameTickerWithin1h:
    """Second alert should see the first in its 1h lookback window."""

    def test_second_alert_sees_first(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 10:00:00", "C"),
                _make_alert("a2", "AAPL", "2026-01-15 10:30:00", "C"),
            ]
        )
        result = compute_flow_context_features(alerts)

        row_a1 = result[result["alert_id"] == "a1"].iloc[0]
        row_a2 = result[result["alert_id"] == "a2"].iloc[0]

        # First alert has no prior history
        assert row_a1["same_ticker_alerts_1h"] == 0

        # Second alert should see the first one in 1h window
        assert row_a2["same_ticker_alerts_1h"] == 1


# ===========================================================================
# test_alerts_outside_1h_window
# ===========================================================================


class TestAlertsOutside1hWindow:
    """Alerts 2 hours apart should NOT see each other in the 1h window."""

    def test_outside_window(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 10:00:00", "C"),
                _make_alert("a2", "AAPL", "2026-01-15 12:01:00", "C"),
            ]
        )
        result = compute_flow_context_features(alerts)

        row_a2 = result[result["alert_id"] == "a2"].iloc[0]
        assert row_a2["same_ticker_alerts_1h"] == 0


# ===========================================================================
# test_directional_agreement_all_calls
# ===========================================================================


class TestDirectionalAgreementAllCalls:
    """If all prior alerts in 4h are calls, agreement should be 1.0."""

    def test_all_calls_agreement(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 10:00:00", "C"),
                _make_alert("a2", "AAPL", "2026-01-15 11:00:00", "C"),
                _make_alert("a3", "AAPL", "2026-01-15 12:00:00", "C"),
                _make_alert("a4", "AAPL", "2026-01-15 13:00:00", "C"),
            ]
        )
        result = compute_flow_context_features(alerts)

        row_a4 = result[result["alert_id"] == "a4"].iloc[0]
        assert row_a4["directional_agreement_4h"] == 1.0


# ===========================================================================
# test_directional_agreement_mixed
# ===========================================================================


class TestDirectionalAgreementMixed:
    """Mixed directions should give fractional agreement."""

    def test_mixed_agreement(self) -> None:
        # a4 is a Call, prior alerts: 2 Calls + 1 Put => agreement = 2/3
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 10:00:00", "C"),
                _make_alert("a2", "AAPL", "2026-01-15 11:00:00", "C"),
                _make_alert("a3", "AAPL", "2026-01-15 12:00:00", "P"),
                _make_alert("a4", "AAPL", "2026-01-15 13:00:00", "C"),
            ]
        )
        result = compute_flow_context_features(alerts)

        row_a4 = result[result["alert_id"] == "a4"].iloc[0]
        assert abs(row_a4["directional_agreement_4h"] - 2 / 3) < 1e-10


# ===========================================================================
# test_repeat_ticker_days
# ===========================================================================


class TestRepeatTickerDays:
    """Alerts on 3 different days should give repeat_ticker_days=3 for subsequent alert."""

    def test_three_distinct_days(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-13 10:00:00", "C"),  # Day 1 (Mon)
                _make_alert("a2", "AAPL", "2026-01-14 10:00:00", "C"),  # Day 2 (Tue)
                _make_alert("a3", "AAPL", "2026-01-15 10:00:00", "C"),  # Day 3 (Wed)
                _make_alert("a4", "AAPL", "2026-01-16 10:00:00", "C"),  # Day 4 (Thu) - target
            ]
        )
        result = compute_flow_context_features(alerts)

        row_a4 = result[result["alert_id"] == "a4"].iloc[0]
        # a4 looks back 5 calendar days from Jan 16 -> Jan 11
        # Days with alerts: Jan 13, 14, 15 => 3 distinct days
        assert row_a4["repeat_ticker_days_5d"] == 3


# ===========================================================================
# test_repeat_ticker_days_same_day
# ===========================================================================


class TestRepeatTickerDaysSameDay:
    """Multiple alerts on the same day count as 1 day."""

    def test_same_day_counts_once(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 10:00:00", "C"),
                _make_alert("a2", "AAPL", "2026-01-15 11:00:00", "C"),
                _make_alert("a3", "AAPL", "2026-01-15 12:00:00", "C"),
                _make_alert("a4", "AAPL", "2026-01-16 10:00:00", "C"),
            ]
        )
        result = compute_flow_context_features(alerts)

        row_a4 = result[result["alert_id"] == "a4"].iloc[0]
        # Only Jan 15 has prior alerts -> 1 distinct day
        assert row_a4["repeat_ticker_days_5d"] == 1


# ===========================================================================
# test_multi_ticker_independent
# ===========================================================================


class TestMultiTickerIndependent:
    """Alerts for different tickers should NOT affect each other's counts."""

    def test_independent_tickers(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 10:00:00", "C"),
                _make_alert("a2", "AAPL", "2026-01-15 10:30:00", "C"),
                _make_alert("a3", "MSFT", "2026-01-15 10:30:00", "P"),
            ]
        )
        result = compute_flow_context_features(alerts)

        row_aapl_2 = result[result["alert_id"] == "a2"].iloc[0]
        row_msft = result[result["alert_id"] == "a3"].iloc[0]

        # AAPL second alert sees the first AAPL alert
        assert row_aapl_2["same_ticker_alerts_1h"] == 1

        # MSFT should NOT see the AAPL alerts
        assert row_msft["same_ticker_alerts_1h"] == 0
        assert row_msft["directional_agreement_4h"] == 0.5  # default


# ===========================================================================
# test_no_future_leakage
# ===========================================================================


class TestNoFutureLeakage:
    """Alert A at t=10 should NOT count alert B at t=11 (strictly backward-looking)."""

    def test_no_leakage(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 10:00:00", "C"),
                _make_alert("a2", "AAPL", "2026-01-15 10:05:00", "C"),
            ]
        )
        result = compute_flow_context_features(alerts)

        row_a1 = result[result["alert_id"] == "a1"].iloc[0]
        # a1 is the first alert - it must NOT see a2
        assert row_a1["same_ticker_alerts_1h"] == 0
        assert row_a1["directional_agreement_4h"] == 0.5
        assert row_a1["repeat_ticker_days_5d"] == 0


# ===========================================================================
# test_output_columns
# ===========================================================================


class TestOutputColumns:
    """Verify output DataFrame has all required columns."""

    def test_all_columns_present(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 10:00:00", "C"),
            ]
        )
        result = compute_flow_context_features(alerts)

        expected_cols = {
            "alert_id",
            "instrument_key",
            "ts_event",
            "ts_available",
            "same_ticker_alerts_1h",
            "directional_agreement_4h",
            "repeat_ticker_days_5d",
        }
        assert expected_cols == set(result.columns)

    def test_alert_id_from_event_id(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("evt-123", "AAPL", "2026-01-15 10:00:00", "C"),
            ]
        )
        result = compute_flow_context_features(alerts)
        assert result.iloc[0]["alert_id"] == "evt-123"

    def test_instrument_key_from_underlying(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "TSLA", "2026-01-15 10:00:00", "C"),
            ]
        )
        result = compute_flow_context_features(alerts)
        assert result.iloc[0]["instrument_key"] == "TSLA"

    def test_ts_available_equals_ts_event(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 10:00:00", "C"),
            ]
        )
        result = compute_flow_context_features(alerts)
        assert result.iloc[0]["ts_available"] == result.iloc[0]["ts_event"]

    def test_ts_event_is_datetime(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 10:00:00", "C"),
            ]
        )
        result = compute_flow_context_features(alerts)
        assert pd.api.types.is_datetime64_any_dtype(result["ts_event"])

    def test_same_ticker_alerts_is_integer(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 10:00:00", "C"),
            ]
        )
        result = compute_flow_context_features(alerts)
        # Should be integer-like (0, 1, 2, ...)
        assert result["same_ticker_alerts_1h"].dtype in (np.int64, np.int32, int, pd.Int64Dtype())

    def test_repeat_ticker_days_is_integer(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 10:00:00", "C"),
            ]
        )
        result = compute_flow_context_features(alerts)
        assert result["repeat_ticker_days_5d"].dtype in (np.int64, np.int32, int, pd.Int64Dtype())

    def test_directional_agreement_is_float(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 10:00:00", "C"),
            ]
        )
        result = compute_flow_context_features(alerts)
        assert pd.api.types.is_float_dtype(result["directional_agreement_4h"])


# ===========================================================================
# Edge cases and boundary conditions
# ===========================================================================


class TestEdgeCases:
    """Additional edge cases for robustness."""

    def test_alert_exactly_at_1h_boundary(self) -> None:
        """Alert exactly 1 hour before should NOT be counted (exclusive boundary)."""
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 09:00:00", "C"),
                _make_alert("a2", "AAPL", "2026-01-15 10:00:00", "C"),
            ]
        )
        result = compute_flow_context_features(alerts)

        row_a2 = result[result["alert_id"] == "a2"].iloc[0]
        # a1 is exactly 1h before a2 — (ts_event - 1h, ts_event) is open interval
        # at the 1h boundary, a1 should NOT be included
        assert row_a2["same_ticker_alerts_1h"] == 0

    def test_alert_just_inside_1h_boundary(self) -> None:
        """Alert 59 minutes before should be counted."""
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 09:01:00", "C"),
                _make_alert("a2", "AAPL", "2026-01-15 10:00:00", "C"),
            ]
        )
        result = compute_flow_context_features(alerts)

        row_a2 = result[result["alert_id"] == "a2"].iloc[0]
        assert row_a2["same_ticker_alerts_1h"] == 1

    def test_repeat_days_beyond_5d_excluded(self) -> None:
        """Alert older than 5 calendar days should NOT count toward repeat_ticker_days_5d."""
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-09 10:00:00", "C"),  # 7 days before a2
                _make_alert("a2", "AAPL", "2026-01-16 10:00:00", "C"),
            ]
        )
        result = compute_flow_context_features(alerts)

        row_a2 = result[result["alert_id"] == "a2"].iloc[0]
        # a1 is 7 days before, outside 5-day window
        assert row_a2["repeat_ticker_days_5d"] == 0

    def test_directional_agreement_put_alert(self) -> None:
        """A put alert should measure agreement against its own direction."""
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 10:00:00", "P"),
                _make_alert("a2", "AAPL", "2026-01-15 11:00:00", "P"),
                _make_alert("a3", "AAPL", "2026-01-15 12:00:00", "P"),
            ]
        )
        result = compute_flow_context_features(alerts)

        row_a3 = result[result["alert_id"] == "a3"].iloc[0]
        # All prior are puts, a3 is a put -> agreement = 1.0
        assert row_a3["directional_agreement_4h"] == 1.0

    def test_many_alerts_performance(self) -> None:
        """Verify function handles a moderate number of alerts without error."""
        n_alerts = 500
        alerts = []
        base = pd.Timestamp("2026-01-15 09:30:00", tz="UTC")
        for i in range(n_alerts):
            alerts.append(
                _make_alert(
                    f"a{i}",
                    "AAPL" if i % 2 == 0 else "MSFT",
                    base + pd.Timedelta(minutes=i),
                    "C" if i % 3 != 0 else "P",
                )
            )
        df = _alerts_df(alerts)
        result = compute_flow_context_features(df)
        assert len(result) == n_alerts
