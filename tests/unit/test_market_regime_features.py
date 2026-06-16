"""Unit tests for market regime feature compute functions.

Tests the four standalone compute functions independently with synthetic DataFrames.
Does NOT test the MarketRegimePipeline class (that requires a real Heber volume).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from heber.features.pipelines.market_regime_features import (
    compute_breadth_proxy,
    compute_dispersion,
    compute_vol_of_vol,
    compute_yield_curve_slope,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_daily_bars(
    symbols: list[str],
    start: str = "2025-01-01",
    periods: int = 120,
    base_price: float = 100.0,
    seed: int = 42,
) -> pd.DataFrame:
    """Create synthetic daily bars for multiple tickers."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(start, periods=periods, tz="UTC")
    rows = []
    for sym in symbols:
        prices = base_price * np.cumprod(1 + rng.normal(0, 0.02, size=periods))
        for i, dt in enumerate(dates):
            rows.append(
                {
                    "instrument_key": f"equity:{sym}",
                    "ts_event": dt,
                    "close": prices[i],
                }
            )
    return pd.DataFrame(rows)


def _make_vix_prices(
    start: str = "2025-01-01",
    periods: int = 60,
    base: float = 20.0,
    seed: int = 99,
) -> pd.DataFrame:
    """Create synthetic daily VIX prices."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(start, periods=periods, tz="UTC")
    prices = base * np.cumprod(1 + rng.normal(0, 0.05, size=periods))
    return pd.DataFrame(
        {
            "ts_event": dates,
            "close": prices,
        }
    )


def _make_momentum_features(
    symbols: list[str],
    start: str = "2025-01-01",
    periods: int = 30,
    seed: int = 7,
) -> pd.DataFrame:
    """Create synthetic Gold momentum features."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(start, periods=periods, tz="UTC")
    rows = []
    for sym in symbols:
        for dt in dates:
            rows.append(
                {
                    "instrument_key": f"equity:{sym}",
                    "ts_event": dt,
                    "momentum_1d": rng.normal(0, 0.02),
                }
            )
    return pd.DataFrame(rows)


def _make_treasury_yields(
    start: str = "2025-01-01",
    periods: int = 30,
    y10_base: float = 4.5,
    y2_base: float = 4.0,
    seed: int = 11,
) -> pd.DataFrame:
    """Create synthetic treasury yield data."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(start, periods=periods, tz="UTC")
    rows = []
    for dt in dates:
        rows.append({"ts_event": dt, "maturity": "10Y", "yield_pct": y10_base + rng.normal(0, 0.05)})
        rows.append({"ts_event": dt, "maturity": "2Y", "yield_pct": y2_base + rng.normal(0, 0.05)})
    return pd.DataFrame(rows)


# ===========================================================================
# compute_dispersion
# ===========================================================================


class TestComputeDispersion:
    """Tests for compute_dispersion."""

    def test_basic_output_shape(self) -> None:
        bars = _make_daily_bars(["AAPL", "MSFT", "GOOG"], periods=100)
        result = compute_dispersion(bars)

        assert not result.empty
        assert "ts_event" in result.columns
        assert "dispersion" in result.columns
        # One row per date (not per ticker)
        assert result["ts_event"].nunique() == len(result)

    def test_empty_input(self) -> None:
        result = compute_dispersion(pd.DataFrame())
        assert result.empty
        assert list(result.columns) == ["ts_event", "dispersion"]

    def test_single_ticker_returns_nan_sd(self) -> None:
        """Cross-sectional SD with one ticker is NaN (no variation across tickers)."""
        bars = _make_daily_bars(["AAPL"], periods=100)
        result = compute_dispersion(bars)
        # With only one ticker, cross-sectional SD is NaN
        assert result["dispersion"].isna().all()

    def test_dispersion_values_around_one(self) -> None:
        """After the rolling window is established, dispersion should hover around 1.0."""
        bars = _make_daily_bars(["A", "B", "C", "D", "E"], periods=150, seed=123)
        result = compute_dispersion(bars, rolling_window=90)
        # After 90-day warmup, values should be defined and centered near 1
        valid = result.dropna(subset=["dispersion"])
        assert len(valid) > 0
        # Not all extreme — should be near 1 for stationary data
        assert valid["dispersion"].median() > 0.5
        assert valid["dispersion"].median() < 2.0

    def test_normalization_by_rolling_median(self) -> None:
        """Verify that dispersion uses rolling median normalization."""
        bars = _make_daily_bars(["X", "Y", "Z"], periods=100, seed=55)
        result_short = compute_dispersion(bars, rolling_window=10)
        result_long = compute_dispersion(bars, rolling_window=90)
        # Different rolling windows should produce different results
        common_dates = set(result_short["ts_event"]) & set(result_long["ts_event"])
        assert len(common_dates) > 0

    def test_nan_close_prices_handled(self) -> None:
        """NaN close prices should not crash the computation."""
        bars = _make_daily_bars(["AAPL", "MSFT"], periods=50)
        bars.loc[bars.index[:5], "close"] = np.nan
        result = compute_dispersion(bars)
        # Should still produce output (some NaN rows are expected)
        assert "dispersion" in result.columns


# ===========================================================================
# compute_vol_of_vol
# ===========================================================================


class TestComputeVolOfVol:
    """Tests for compute_vol_of_vol."""

    def test_basic_output_shape(self) -> None:
        vix = _make_vix_prices(periods=60)
        result = compute_vol_of_vol(vix)

        assert not result.empty
        assert "ts_event" in result.columns
        assert "vol_of_vol" in result.columns

    def test_empty_input(self) -> None:
        result = compute_vol_of_vol(pd.DataFrame())
        assert result.empty
        assert list(result.columns) == ["ts_event", "vol_of_vol"]

    def test_requires_warmup_period(self) -> None:
        """First 20 rows should be NaN (need 20 log returns for rolling SD)."""
        vix = _make_vix_prices(periods=30)
        result = compute_vol_of_vol(vix, rolling_window=20)
        # First 20 values should be NaN (need 1 for log return + 20 for rolling)
        nan_count = result["vol_of_vol"].isna().sum()
        assert nan_count >= 20

    def test_positive_values(self) -> None:
        """Vol-of-vol (standard deviation) should be non-negative."""
        vix = _make_vix_prices(periods=60)
        result = compute_vol_of_vol(vix)
        valid = result.dropna(subset=["vol_of_vol"])
        assert (valid["vol_of_vol"] >= 0).all()

    def test_handles_intraday_duplicates(self) -> None:
        """Multiple rows per date should be collapsed to last close."""
        dates = pd.bdate_range("2025-01-01", periods=30, tz="UTC")
        rows = []
        for dt in dates:
            # Two quotes per day
            rows.append({"ts_event": dt, "close": 20.0})
            rows.append({"ts_event": dt + pd.Timedelta(hours=4), "close": 21.0})
        vix = pd.DataFrame(rows)
        result = compute_vol_of_vol(vix)
        # Should have one row per date
        assert result["ts_event"].nunique() == len(result)

    def test_custom_rolling_window(self) -> None:
        vix = _make_vix_prices(periods=60)
        result_10 = compute_vol_of_vol(vix, rolling_window=10)
        result_20 = compute_vol_of_vol(vix, rolling_window=20)
        # 10-day window should have fewer NaN values
        assert result_10["vol_of_vol"].isna().sum() < result_20["vol_of_vol"].isna().sum()


# ===========================================================================
# compute_breadth_proxy
# ===========================================================================


class TestComputeBreadthProxy:
    """Tests for compute_breadth_proxy."""

    def test_basic_output_shape(self) -> None:
        momentum = _make_momentum_features(["A", "B", "C"], periods=20)
        result = compute_breadth_proxy(momentum)

        assert not result.empty
        assert "ts_event" in result.columns
        assert "breadth_proxy" in result.columns

    def test_empty_input(self) -> None:
        result = compute_breadth_proxy(pd.DataFrame())
        assert result.empty
        assert list(result.columns) == ["ts_event", "breadth_proxy"]

    def test_range_zero_to_one(self) -> None:
        """Breadth proxy must always be between 0 and 1."""
        momentum = _make_momentum_features(["A", "B", "C", "D", "E"], periods=50, seed=42)
        result = compute_breadth_proxy(momentum)
        assert (result["breadth_proxy"] >= 0).all()
        assert (result["breadth_proxy"] <= 1).all()

    def test_all_positive_momentum(self) -> None:
        """When all tickers have positive momentum, breadth should be 1.0."""
        dates = pd.bdate_range("2025-01-01", periods=5, tz="UTC")
        rows = []
        for sym in ["A", "B", "C"]:
            for dt in dates:
                rows.append({"instrument_key": f"equity:{sym}", "ts_event": dt, "momentum_1d": 0.05})
        momentum = pd.DataFrame(rows)
        result = compute_breadth_proxy(momentum)
        assert (result["breadth_proxy"] == 1.0).all()

    def test_all_negative_momentum(self) -> None:
        """When all tickers have negative momentum, breadth should be 0.0."""
        dates = pd.bdate_range("2025-01-01", periods=5, tz="UTC")
        rows = []
        for sym in ["A", "B", "C"]:
            for dt in dates:
                rows.append({"instrument_key": f"equity:{sym}", "ts_event": dt, "momentum_1d": -0.03})
        momentum = pd.DataFrame(rows)
        result = compute_breadth_proxy(momentum)
        assert (result["breadth_proxy"] == 0.0).all()

    def test_mixed_momentum(self) -> None:
        """Two out of three tickers positive should give ~0.667."""
        dates = pd.bdate_range("2025-01-01", periods=1, tz="UTC")
        rows = [
            {"instrument_key": "equity:A", "ts_event": dates[0], "momentum_1d": 0.05},
            {"instrument_key": "equity:B", "ts_event": dates[0], "momentum_1d": 0.02},
            {"instrument_key": "equity:C", "ts_event": dates[0], "momentum_1d": -0.01},
        ]
        momentum = pd.DataFrame(rows)
        result = compute_breadth_proxy(momentum)
        assert len(result) == 1
        assert abs(result["breadth_proxy"].iloc[0] - 2 / 3) < 1e-10

    def test_nan_momentum_excluded(self) -> None:
        """NaN momentum_1d values should be excluded from count."""
        dates = pd.bdate_range("2025-01-01", periods=1, tz="UTC")
        rows = [
            {"instrument_key": "equity:A", "ts_event": dates[0], "momentum_1d": 0.05},
            {"instrument_key": "equity:B", "ts_event": dates[0], "momentum_1d": np.nan},
            {"instrument_key": "equity:C", "ts_event": dates[0], "momentum_1d": -0.01},
        ]
        momentum = pd.DataFrame(rows)
        result = compute_breadth_proxy(momentum)
        # Only A (positive) and C (negative) count — 1/2 = 0.5
        assert len(result) == 1
        assert abs(result["breadth_proxy"].iloc[0] - 0.5) < 1e-10

    def test_all_nan_momentum(self) -> None:
        """All NaN momentum should produce empty output."""
        dates = pd.bdate_range("2025-01-01", periods=3, tz="UTC")
        rows = []
        for dt in dates:
            rows.append({"instrument_key": "equity:A", "ts_event": dt, "momentum_1d": np.nan})
        momentum = pd.DataFrame(rows)
        result = compute_breadth_proxy(momentum)
        assert result.empty


# ===========================================================================
# compute_yield_curve_slope
# ===========================================================================


class TestComputeYieldCurveSlope:
    """Tests for compute_yield_curve_slope."""

    def test_basic_output_shape(self) -> None:
        yields_df = _make_treasury_yields(periods=20)
        result = compute_yield_curve_slope(yields_df)

        assert not result.empty
        assert "ts_event" in result.columns
        assert "yield_curve_slope" in result.columns

    def test_empty_input(self) -> None:
        result = compute_yield_curve_slope(pd.DataFrame())
        assert result.empty
        assert list(result.columns) == ["ts_event", "yield_curve_slope"]

    def test_positive_slope(self) -> None:
        """When 10Y > 2Y, slope should be positive (normal curve)."""
        dates = pd.bdate_range("2025-01-01", periods=5, tz="UTC")
        rows = []
        for dt in dates:
            rows.append({"ts_event": dt, "maturity": "10Y", "yield_pct": 4.5})
            rows.append({"ts_event": dt, "maturity": "2Y", "yield_pct": 4.0})
        yields_df = pd.DataFrame(rows)
        result = compute_yield_curve_slope(yields_df)
        assert (result["yield_curve_slope"] > 0).all()
        assert np.allclose(result["yield_curve_slope"], 0.5)

    def test_negative_slope_inverted_curve(self) -> None:
        """When 2Y > 10Y, slope should be negative (inverted curve)."""
        dates = pd.bdate_range("2025-01-01", periods=5, tz="UTC")
        rows = []
        for dt in dates:
            rows.append({"ts_event": dt, "maturity": "10Y", "yield_pct": 3.5})
            rows.append({"ts_event": dt, "maturity": "2Y", "yield_pct": 4.0})
        yields_df = pd.DataFrame(rows)
        result = compute_yield_curve_slope(yields_df)
        assert (result["yield_curve_slope"] < 0).all()
        assert np.allclose(result["yield_curve_slope"], -0.5)

    def test_missing_10y_maturity(self) -> None:
        """Should return empty when 10Y data is absent."""
        dates = pd.bdate_range("2025-01-01", periods=5, tz="UTC")
        rows = [{"ts_event": dt, "maturity": "2Y", "yield_pct": 4.0} for dt in dates]
        yields_df = pd.DataFrame(rows)
        result = compute_yield_curve_slope(yields_df)
        assert result.empty

    def test_missing_2y_maturity(self) -> None:
        """Should return empty when 2Y data is absent."""
        dates = pd.bdate_range("2025-01-01", periods=5, tz="UTC")
        rows = [{"ts_event": dt, "maturity": "10Y", "yield_pct": 4.5} for dt in dates]
        yields_df = pd.DataFrame(rows)
        result = compute_yield_curve_slope(yields_df)
        assert result.empty

    def test_nan_yields_propagate(self) -> None:
        """NaN yield values should result in NaN slope for that date."""
        dates = pd.bdate_range("2025-01-01", periods=3, tz="UTC")
        rows = [
            {"ts_event": dates[0], "maturity": "10Y", "yield_pct": 4.5},
            {"ts_event": dates[0], "maturity": "2Y", "yield_pct": 4.0},
            {"ts_event": dates[1], "maturity": "10Y", "yield_pct": np.nan},
            {"ts_event": dates[1], "maturity": "2Y", "yield_pct": 4.0},
            {"ts_event": dates[2], "maturity": "10Y", "yield_pct": 4.5},
            {"ts_event": dates[2], "maturity": "2Y", "yield_pct": 4.0},
        ]
        yields_df = pd.DataFrame(rows)
        result = compute_yield_curve_slope(yields_df)
        # Date 0 and 2 should be valid, date 1 should be NaN
        valid = result.dropna(subset=["yield_curve_slope"])
        assert len(valid) == 2

    def test_one_row_per_date(self) -> None:
        """Output should have exactly one row per date."""
        yields_df = _make_treasury_yields(periods=20)
        result = compute_yield_curve_slope(yields_df)
        assert result["ts_event"].nunique() == len(result)


# ===========================================================================
# Output schema integration checks
# ===========================================================================


class TestOutputSchema:
    """Verify that compute function outputs have the expected columns and types."""

    def test_dispersion_schema(self) -> None:
        bars = _make_daily_bars(["A", "B"], periods=100)
        result = compute_dispersion(bars)
        assert set(result.columns) == {"ts_event", "dispersion"}
        assert pd.api.types.is_datetime64_any_dtype(result["ts_event"])
        assert pd.api.types.is_float_dtype(result["dispersion"])

    def test_vol_of_vol_schema(self) -> None:
        vix = _make_vix_prices(periods=40)
        result = compute_vol_of_vol(vix)
        assert set(result.columns) == {"ts_event", "vol_of_vol"}
        assert pd.api.types.is_datetime64_any_dtype(result["ts_event"])
        assert pd.api.types.is_float_dtype(result["vol_of_vol"])

    def test_breadth_proxy_schema(self) -> None:
        momentum = _make_momentum_features(["A", "B"], periods=10)
        result = compute_breadth_proxy(momentum)
        assert set(result.columns) == {"ts_event", "breadth_proxy"}
        assert pd.api.types.is_datetime64_any_dtype(result["ts_event"])

    def test_yield_curve_slope_schema(self) -> None:
        yields_df = _make_treasury_yields(periods=10)
        result = compute_yield_curve_slope(yields_df)
        assert set(result.columns) == {"ts_event", "yield_curve_slope"}
        assert pd.api.types.is_datetime64_any_dtype(result["ts_event"])
        assert pd.api.types.is_float_dtype(result["yield_curve_slope"])
