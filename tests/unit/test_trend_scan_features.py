"""Unit tests for trend-scanning label compute functions.

Tests the standalone compute functions and OLS helper independently with
synthetic DataFrames. Pipeline class tests use mocked HeberReader.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from heber.features.pipelines.trend_scan_features import (
    TrendScanPipeline,
    compute_ols_t_stat,
    compute_trend_scan_for_series,
    compute_trend_scan_labels,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_uptrend_prices(
    start: str = "2025-01-01",
    periods: int = 40,
    base_price: float = 100.0,
    daily_return: float = 0.005,
) -> pd.Series:
    """Create a synthetic uptrend price series (deterministic)."""
    dates = pd.bdate_range(start, periods=periods, tz="UTC")
    prices = base_price * (1 + daily_return) ** np.arange(periods)
    return pd.Series(prices, index=dates, name="close")


def _make_downtrend_prices(
    start: str = "2025-01-01",
    periods: int = 40,
    base_price: float = 100.0,
    daily_return: float = -0.005,
) -> pd.Series:
    """Create a synthetic downtrend price series (deterministic)."""
    dates = pd.bdate_range(start, periods=periods, tz="UTC")
    prices = base_price * (1 + daily_return) ** np.arange(periods)
    return pd.Series(prices, index=dates, name="close")


def _make_flat_prices(
    start: str = "2025-01-01",
    periods: int = 40,
    price: float = 100.0,
) -> pd.Series:
    """Create a constant price series."""
    dates = pd.bdate_range(start, periods=periods, tz="UTC")
    return pd.Series([price] * periods, index=dates, name="close")


def _make_daily_bars(
    symbols: list[str],
    start: str = "2025-01-01",
    periods: int = 40,
    base_price: float = 100.0,
    seed: int = 42,
) -> pd.DataFrame:
    """Create synthetic daily bars for multiple tickers."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(start, periods=periods, tz="UTC")
    rows = []
    for sym in symbols:
        prices = base_price * np.cumprod(1 + rng.normal(0.001, 0.02, size=periods))
        for i, dt in enumerate(dates):
            rows.append(
                {
                    "instrument_key": f"equity:{sym}",
                    "ts_event": dt,
                    "close": prices[i],
                }
            )
    return pd.DataFrame(rows)


# ===========================================================================
# compute_ols_t_stat
# ===========================================================================


class TestComputeOlsTStat:
    """Tests for the OLS t-statistic helper."""

    def test_perfect_uptrend(self) -> None:
        """A perfect linear uptrend should have a large positive t-stat."""
        prices = np.array([100.0, 101.0, 102.0, 103.0, 104.0])
        beta, t_stat = compute_ols_t_stat(prices)
        assert beta > 0
        # Perfect linear fit — t-stat should be very large (or inf)
        assert t_stat > 100 or np.isinf(t_stat) or np.isnan(t_stat)

    def test_perfect_downtrend(self) -> None:
        """A perfect linear downtrend should have a large negative t-stat."""
        prices = np.array([104.0, 103.0, 102.0, 101.0, 100.0])
        beta, t_stat = compute_ols_t_stat(prices)
        assert beta < 0
        assert t_stat < -100 or np.isinf(t_stat) or np.isnan(t_stat)

    def test_single_point_returns_nan(self) -> None:
        """A single data point cannot produce a valid regression."""
        prices = np.array([100.0])
        beta, t_stat = compute_ols_t_stat(prices)
        assert np.isnan(beta)
        assert np.isnan(t_stat)

    def test_two_points(self) -> None:
        """Two points should produce a valid slope but SE may be degenerate."""
        prices = np.array([100.0, 105.0])
        beta, t_stat = compute_ols_t_stat(prices)
        assert beta == pytest.approx(5.0)
        # With n=2, dof=0, MSE=0/0 — t_stat may be NaN
        # This is expected behavior

    def test_noisy_uptrend_positive_t(self) -> None:
        """A noisy uptrend should still have a positive t-stat."""
        rng = np.random.default_rng(42)
        prices = 100.0 + 2.0 * np.arange(20) + rng.normal(0, 1, size=20)
        beta, t_stat = compute_ols_t_stat(prices)
        assert beta > 0
        assert t_stat > 0

    def test_constant_prices_returns_nan(self) -> None:
        """Constant prices have zero SD, so regression is degenerate (NaN)."""
        prices = np.array([50.0, 50.0, 50.0, 50.0, 50.0])
        beta, t_stat = compute_ols_t_stat(prices)
        # Zero variance in y means the function returns early with NaN
        assert np.isnan(beta)
        assert np.isnan(t_stat)


# ===========================================================================
# compute_trend_scan_for_series
# ===========================================================================


class TestComputeTrendScanForSeries:
    """Tests for single-ticker trend scanning."""

    def test_uptrend_positive_t_values(self) -> None:
        """An uptrend should produce positive t-values."""
        prices = _make_uptrend_prices(periods=40, daily_return=0.01)
        result = compute_trend_scan_for_series(prices)

        assert not result.empty
        assert "trend_scan_horizon" in result.columns
        assert "trend_scan_t_value" in result.columns
        # Most t-values should be positive for an uptrend
        positive_frac = (result["trend_scan_t_value"] > 0).mean()
        assert positive_frac > 0.8

    def test_downtrend_negative_t_values(self) -> None:
        """A downtrend should produce negative t-values."""
        prices = _make_downtrend_prices(periods=40, daily_return=-0.01)
        result = compute_trend_scan_for_series(prices)

        assert not result.empty
        negative_frac = (result["trend_scan_t_value"] < 0).mean()
        assert negative_frac > 0.8

    def test_empty_series(self) -> None:
        """Empty price series should return empty DataFrame."""
        prices = pd.Series(dtype=float)
        result = compute_trend_scan_for_series(prices)
        assert result.empty
        assert list(result.columns) == ["ts_event", "trend_scan_horizon", "trend_scan_t_value"]

    def test_horizon_within_candidates(self) -> None:
        """All selected horizons should be from the candidate list."""
        prices = _make_uptrend_prices(periods=40)
        horizons = [1, 2, 3, 5, 10, 15, 20]
        result = compute_trend_scan_for_series(prices, horizons=horizons)

        assert not result.empty
        assert set(result["trend_scan_horizon"].unique()).issubset(set(horizons))

    def test_custom_horizons(self) -> None:
        """Custom horizons should be respected."""
        prices = _make_uptrend_prices(periods=20)
        result = compute_trend_scan_for_series(prices, horizons=[1, 3, 5])

        assert not result.empty
        assert set(result["trend_scan_horizon"].unique()).issubset({1, 3, 5})

    def test_short_series_limited_horizons(self) -> None:
        """When series is shorter than max horizon, only shorter horizons are used."""
        prices = _make_uptrend_prices(periods=5)
        result = compute_trend_scan_for_series(prices, horizons=[1, 2, 3, 5, 10])

        assert not result.empty
        # Cannot use horizon=10 with only 5 data points
        assert result["trend_scan_horizon"].max() <= 4

    def test_last_rows_have_no_labels(self) -> None:
        """The last max_horizon rows should not have labels (no forward data)."""
        prices = _make_uptrend_prices(periods=30)
        result = compute_trend_scan_for_series(prices, horizons=[1, 5, 10])

        # The last data point cannot have any forward horizon
        last_date = prices.index[-1]
        assert last_date not in result["ts_event"].values


# ===========================================================================
# compute_trend_scan_labels
# ===========================================================================


class TestComputeTrendScanLabels:
    """Tests for multi-ticker trend scanning."""

    def test_basic_output_shape(self) -> None:
        bars = _make_daily_bars(["AAPL", "MSFT"], periods=40)
        result = compute_trend_scan_labels(bars)

        assert not result.empty
        assert "instrument_key" in result.columns
        assert "ts_event" in result.columns
        assert "trend_scan_horizon" in result.columns
        assert "trend_scan_t_value" in result.columns

    def test_empty_input(self) -> None:
        result = compute_trend_scan_labels(pd.DataFrame())
        assert result.empty
        assert set(result.columns) == {"instrument_key", "ts_event", "trend_scan_horizon", "trend_scan_t_value"}

    def test_multiple_tickers_independent(self) -> None:
        """Each ticker should be processed independently."""
        bars = _make_daily_bars(["AAPL", "MSFT", "GOOG"], periods=30)
        result = compute_trend_scan_labels(bars)

        # Should have results for all three tickers
        tickers = result["instrument_key"].unique()
        assert len(tickers) == 3

    def test_nan_close_prices_handled(self) -> None:
        """NaN close prices should not crash, affected observations skipped."""
        bars = _make_daily_bars(["AAPL"], periods=30)
        bars.loc[bars.index[5:8], "close"] = np.nan
        result = compute_trend_scan_labels(bars)
        # Should still produce some output
        assert "trend_scan_horizon" in result.columns

    def test_horizon_values_are_integers(self) -> None:
        """trend_scan_horizon should be integer-valued."""
        bars = _make_daily_bars(["AAPL"], periods=40)
        result = compute_trend_scan_labels(bars)
        assert not result.empty
        assert (result["trend_scan_horizon"] == result["trend_scan_horizon"].astype(int)).all()


# ===========================================================================
# TrendScanPipeline (with mocked reader)
# ===========================================================================


class TestTrendScanPipeline:
    """Tests for the pipeline class with mocked HeberReader."""

    def test_pipeline_no_data(self) -> None:
        """Pipeline should return no_data when Silver has no bars."""
        mock_reader = MagicMock()
        mock_reader.read_silver.return_value = pd.DataFrame()

        pipeline = TrendScanPipeline(reader=mock_reader)
        stats = pipeline.run(start_date="2025-01-01", end_date="2025-01-31", dry_run=True)

        result = stats["trend_scan_features"]
        assert result["status"] == "no_data"
        assert result["rows"] == 0

    def test_pipeline_dry_run_does_not_write(self) -> None:
        """Pipeline dry run should compute but not call write_gold."""
        bars = _make_daily_bars(["AAPL", "MSFT"], periods=40, start="2025-01-01")

        mock_reader = MagicMock()
        mock_reader.read_silver.return_value = bars

        pipeline = TrendScanPipeline(reader=mock_reader)
        stats = pipeline.run(start_date="2025-01-01", end_date="2025-02-28", dry_run=True)

        result = stats["trend_scan_features"]
        assert result["status"] == "success"
        assert result["rows"] > 0
        mock_reader.write_gold.assert_not_called()

    def test_pipeline_writes_on_real_run(self) -> None:
        """Pipeline should call write_gold when not in dry_run mode."""
        bars = _make_daily_bars(["AAPL"], periods=40, start="2025-01-01")

        mock_reader = MagicMock()
        mock_reader.read_silver.return_value = bars
        mock_reader.write_gold.return_value = "/fake/path/trend_scan.parquet"

        pipeline = TrendScanPipeline(reader=mock_reader)
        stats = pipeline.run(start_date="2025-01-01", end_date="2025-02-28", dry_run=False)

        assert stats["trend_scan_features"]["status"] == "success"
        mock_reader.write_gold.assert_called_once()
        call_kwargs = mock_reader.write_gold.call_args
        assert (
            call_kwargs.kwargs.get("dataset") == "trend_scan_features"
            or call_kwargs[1].get("dataset") == "trend_scan_features"
        )

    def test_pipeline_stats_contain_expected_keys(self) -> None:
        """Pipeline stats should include distribution and mean t-value."""
        bars = _make_daily_bars(["AAPL"], periods=40, start="2025-01-01")

        mock_reader = MagicMock()
        mock_reader.read_silver.return_value = bars

        pipeline = TrendScanPipeline(reader=mock_reader)
        stats = pipeline.run(start_date="2025-01-01", end_date="2025-02-28", dry_run=True)

        result = stats["trend_scan_features"]
        assert "tickers" in result
        assert "horizon_distribution" in result
        assert "mean_abs_t_value" in result
        assert result["mean_abs_t_value"] > 0
