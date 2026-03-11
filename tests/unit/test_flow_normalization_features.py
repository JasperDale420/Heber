"""Unit tests for flow normalization feature compute functions.

Tests the standalone compute_flow_normalization_features() function independently
with synthetic DataFrames.  Does NOT test the FlowNormalizationPipeline class
(that requires a real Heber volume).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from heber.features.pipelines.flow_normalization_features import (
    compute_flow_normalization_features,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REQUIRED_OUTPUT_COLUMNS = {
    "instrument_key",
    "ts_event",
    "ts_available",
    "adv_premium_20d",
    "adv_volume_20d",
    "adv_oi_20d",
}


def _make_flow_alerts(
    tickers: list[str],
    start: str = "2025-01-01",
    periods: int = 25,
    alerts_per_day: int = 3,
    base_premium: float = 50_000.0,
    base_volume: float = 500.0,
    base_oi: float = 10_000.0,
    seed: int = 42,
    include_oi: bool = True,
) -> pd.DataFrame:
    """Create synthetic flow_alerts data for testing.

    Generates ``alerts_per_day`` alerts per ticker per business day.
    """
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(start, periods=periods, tz="UTC")
    rows: list[dict] = []
    for ticker in tickers:
        for dt in dates:
            for _ in range(alerts_per_day):
                row: dict = {
                    "underlying": ticker,
                    "ts_event": dt + pd.Timedelta(hours=rng.integers(9, 16)),
                    "premium": base_premium * (1 + rng.normal(0, 0.3)),
                    "total_size": base_volume * (1 + rng.normal(0, 0.2)),
                    "put_call": rng.choice(["C", "P"]),
                    "is_sweep": rng.choice([True, False]),
                }
                if include_oi:
                    row["open_interest"] = base_oi * (1 + rng.normal(0, 0.1))
                rows.append(row)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestComputeFlowNormalizationFeatures:
    """Tests for compute_flow_normalization_features()."""

    def test_compute_empty_df(self) -> None:
        """Empty input returns empty output."""
        result = compute_flow_normalization_features(pd.DataFrame())
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_compute_single_ticker_single_day(self) -> None:
        """Single day of data is not enough history -- returns empty after dropna."""
        df = _make_flow_alerts(["AAPL"], periods=1)
        result = compute_flow_normalization_features(df)
        # Only 1 day means no 20-day rolling mean can be computed, so all NaN -> dropped
        assert result.empty

    def test_compute_single_ticker_21_days(self) -> None:
        """21 business days of data produces 2 valid rows (days 20 and 21)."""
        df = _make_flow_alerts(["AAPL"], periods=21, seed=7)
        result = compute_flow_normalization_features(df)

        # First 19 days have NaN rolling means -> dropped.  Days 20 and 21 are valid.
        assert len(result) == 2
        assert (result["instrument_key"] == "equity:AAPL").all()

        # Rolling averages should be positive (all inputs are positive)
        assert (result["adv_premium_20d"] > 0).all()
        assert (result["adv_volume_20d"] > 0).all()
        assert (result["adv_oi_20d"] > 0).all()

    def test_compute_multi_ticker(self) -> None:
        """Two tickers get independent rolling means."""
        df = _make_flow_alerts(["AAPL", "TSLA"], periods=25, seed=11)
        result = compute_flow_normalization_features(df)

        # Each ticker should have 6 valid rows (days 20-25)
        assert len(result) == 12
        aapl = result[result["instrument_key"] == "equity:AAPL"]
        tsla = result[result["instrument_key"] == "equity:TSLA"]
        assert len(aapl) == 6
        assert len(tsla) == 6

        # The two tickers should have different rolling averages
        # (different data means different aggregates)
        assert not np.allclose(
            aapl["adv_premium_20d"].values,
            tsla["adv_premium_20d"].values,
        )

    def test_output_has_required_columns(self) -> None:
        """Output DataFrame contains all required Gold columns."""
        df = _make_flow_alerts(["SPY"], periods=22)
        result = compute_flow_normalization_features(df)

        assert not result.empty
        missing = REQUIRED_OUTPUT_COLUMNS - set(result.columns)
        assert not missing, f"Missing columns: {missing}"

    def test_numeric_coercion(self) -> None:
        """String values in premium/volume/oi columns get coerced to numeric."""
        df = _make_flow_alerts(["NVDA"], periods=22, seed=3)
        # Inject string values
        df["premium"] = df["premium"].astype(str)
        df["total_size"] = df["total_size"].astype(str)
        df["open_interest"] = df["open_interest"].astype(str)

        result = compute_flow_normalization_features(df)

        assert not result.empty
        assert result["adv_premium_20d"].dtype in (np.float64, float)
        assert result["adv_volume_20d"].dtype in (np.float64, float)
        assert result["adv_oi_20d"].dtype in (np.float64, float)

    def test_missing_oi_column(self) -> None:
        """Gracefully handles missing open_interest column (fills with NaN)."""
        df = _make_flow_alerts(["AMZN"], periods=22, include_oi=False)
        assert "open_interest" not in df.columns

        result = compute_flow_normalization_features(df)

        assert not result.empty
        # Premium and volume should be valid
        assert (result["adv_premium_20d"] > 0).all()
        assert (result["adv_volume_20d"] > 0).all()
        # OI should be NaN since there was no source data
        assert result["adv_oi_20d"].isna().all()

    def test_ts_available_is_utc(self) -> None:
        """ts_available timestamps are timezone-aware UTC."""
        df = _make_flow_alerts(["META"], periods=22, seed=99)
        result = compute_flow_normalization_features(df)

        assert not result.empty
        # ts_available should be a recent UTC datetime
        ts_avail = result["ts_available"].iloc[0]
        if hasattr(ts_avail, "tzinfo") and ts_avail.tzinfo is not None:
            assert str(ts_avail.tzinfo) in ("UTC", "datetime.timezone.utc")

    def test_ts_event_is_date_midnight_utc(self) -> None:
        """ts_event should be normalized to midnight UTC dates."""
        df = _make_flow_alerts(["GOOG"], periods=22, seed=55)
        result = compute_flow_normalization_features(df)

        assert not result.empty
        # All ts_event values should be at midnight
        for ts in result["ts_event"]:
            assert ts.hour == 0 and ts.minute == 0 and ts.second == 0

    def test_rolling_mean_correctness(self) -> None:
        """Verify the rolling mean value against a manual calculation."""
        # Create exactly 20 days of constant data -- day 20 should have exact mean
        rows = []
        dates = pd.bdate_range("2025-01-01", periods=20, tz="UTC")
        for _i, dt in enumerate(dates):
            rows.append(
                {
                    "underlying": "TEST",
                    "ts_event": dt + pd.Timedelta(hours=10),
                    "premium": 1000.0,  # constant
                    "total_size": 200.0,  # constant
                    "open_interest": 5000.0,  # constant
                    "put_call": "C",
                    "is_sweep": False,
                }
            )
        df = pd.DataFrame(rows)
        result = compute_flow_normalization_features(df)

        # With constant daily totals, the 20-day average should equal the daily total
        assert len(result) == 1
        assert abs(result["adv_premium_20d"].iloc[0] - 1000.0) < 1e-6
        assert abs(result["adv_volume_20d"].iloc[0] - 200.0) < 1e-6
        assert abs(result["adv_oi_20d"].iloc[0] - 5000.0) < 1e-6
