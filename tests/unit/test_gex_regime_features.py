"""Unit tests for GEX regime feature compute functions.

Tests the three standalone compute functions independently with synthetic DataFrames,
plus pipeline-level tests with mocked HeberReader.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import numpy as np
import pandas as pd

from heber.features.pipelines.gex_regime_features import (
    ALL_DATASETS,
    INSTRUMENT_KEY,
    OUTPUT_DATASET,
    GexRegimePipeline,
    compute_gex_flip_distance,
    compute_gex_regime,
    compute_net_gex,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_greek_exposure(
    symbols: list[str],
    start: str = "2025-01-01",
    periods: int = 60,
    seed: int = 42,
    call_gamma_base: float = 0.05,
    put_gamma_base: float = 0.03,
) -> pd.DataFrame:
    """Create synthetic Silver greek_exposure data.

    Each symbol gets one row per date with call_gamma and put_gamma values
    that fluctuate around the provided base values.
    """
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(start, periods=periods, tz="UTC")
    rows = []
    for sym in symbols:
        for dt in dates:
            rows.append(
                {
                    "instrument_key": f"option:{sym}",
                    "ts_event": dt,
                    "call_gamma": call_gamma_base + rng.normal(0, 0.01),
                    "put_gamma": put_gamma_base + rng.normal(0, 0.01),
                    "call_delta": 0.5 + rng.normal(0, 0.1),
                    "put_delta": -0.5 + rng.normal(0, 0.1),
                    "strike": 150.0 + rng.uniform(-20, 20),
                    "expiry": "2025-06-20",
                    "dte": 120,
                }
            )
    return pd.DataFrame(rows)


def _make_net_gex_series(
    start: str = "2025-01-01",
    periods: int = 60,
    seed: int = 42,
    mean: float = 0.0,
    std: float = 0.02,
) -> pd.DataFrame:
    """Create a synthetic net_gex time series (output of compute_net_gex)."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(start, periods=periods, tz="UTC")
    values = mean + rng.normal(0, std, size=periods)
    return pd.DataFrame({"ts_event": dates, "net_gex": values})


# ===========================================================================
# compute_net_gex
# ===========================================================================


class TestComputeNetGex:
    """Tests for compute_net_gex."""

    def test_basic_output_shape(self) -> None:
        greek = _make_greek_exposure(["AAPL", "MSFT"], periods=30)
        result = compute_net_gex(greek)

        assert not result.empty
        assert "ts_event" in result.columns
        assert "net_gex" in result.columns
        # One row per date (not per ticker)
        assert result["ts_event"].nunique() == len(result)

    def test_empty_input(self) -> None:
        result = compute_net_gex(pd.DataFrame())
        assert result.empty
        assert list(result.columns) == ["ts_event", "net_gex"]

    def test_positive_net_gex_when_call_gamma_dominates(self) -> None:
        """When call_gamma consistently exceeds put_gamma, net_gex should be positive."""
        greek = _make_greek_exposure(
            ["AAPL"],
            periods=30,
            call_gamma_base=0.10,
            put_gamma_base=0.02,
            seed=1,
        )
        result = compute_net_gex(greek)
        # With call_gamma ~0.10 and put_gamma ~0.02, mean diff should be ~0.08
        assert result["net_gex"].mean() > 0

    def test_negative_net_gex_when_put_gamma_dominates(self) -> None:
        """When put_gamma consistently exceeds call_gamma, net_gex should be negative."""
        greek = _make_greek_exposure(
            ["AAPL"],
            periods=30,
            call_gamma_base=0.02,
            put_gamma_base=0.10,
            seed=2,
        )
        result = compute_net_gex(greek)
        assert result["net_gex"].mean() < 0

    def test_aggregates_across_tickers(self) -> None:
        """Net GEX should aggregate across all tickers for a single daily value."""
        greek = _make_greek_exposure(["AAPL", "MSFT", "GOOG"], periods=10, seed=3)
        result = compute_net_gex(greek)
        # Should have exactly 10 rows (one per business day)
        assert len(result) == 10

    def test_handles_nan_gamma_values(self) -> None:
        """NaN gamma values should not crash the computation."""
        greek = _make_greek_exposure(["AAPL"], periods=20)
        greek.loc[greek.index[:3], "call_gamma"] = np.nan
        result = compute_net_gex(greek)
        assert "net_gex" in result.columns
        # Should still produce output (some NaN rows expected)
        assert len(result) > 0


# ===========================================================================
# compute_gex_regime
# ===========================================================================


class TestComputeGexRegime:
    """Tests for compute_gex_regime."""

    def test_basic_output_shape(self) -> None:
        net_gex_df = _make_net_gex_series(periods=30)
        result = compute_gex_regime(net_gex_df)

        assert not result.empty
        assert "ts_event" in result.columns
        assert "gex_regime" in result.columns
        assert result["ts_event"].nunique() == len(result)

    def test_empty_input(self) -> None:
        result = compute_gex_regime(pd.DataFrame())
        assert result.empty
        assert list(result.columns) == ["ts_event", "gex_regime"]

    def test_positive_regime(self) -> None:
        """Strongly positive net_gex should yield regime 1.0."""
        dates = pd.bdate_range("2025-01-01", periods=30, tz="UTC")
        # Large positive values well above any threshold
        net_gex_df = pd.DataFrame(
            {
                "ts_event": dates,
                "net_gex": [5.0] * 30,
            }
        )
        result = compute_gex_regime(net_gex_df)
        # With constant value, std is 0 after warmup, so threshold = 0
        # constant 5.0 > 0 => regime = 1.0
        assert (result["gex_regime"] == 1.0).all()

    def test_negative_regime(self) -> None:
        """Strongly negative net_gex should yield regime -1.0."""
        dates = pd.bdate_range("2025-01-01", periods=30, tz="UTC")
        net_gex_df = pd.DataFrame(
            {
                "ts_event": dates,
                "net_gex": [-5.0] * 30,
            }
        )
        result = compute_gex_regime(net_gex_df)
        assert (result["gex_regime"] == -1.0).all()

    def test_neutral_regime_near_zero(self) -> None:
        """Values very close to zero with volatile history should be neutral."""
        dates = pd.bdate_range("2025-01-01", periods=30, tz="UTC")
        rng = np.random.default_rng(99)
        # Create volatile history so rolling std is large, then near-zero values
        values = list(rng.normal(0, 10.0, size=20)) + [0.001] * 10
        net_gex_df = pd.DataFrame({"ts_event": dates, "net_gex": values})
        result = compute_gex_regime(net_gex_df)
        # The last few values (0.001) should be within the neutral zone
        # because |0.001| < 0.1 * rolling_std (where std is large from volatile history)
        last_regimes = result["gex_regime"].iloc[-5:]
        assert (last_regimes == 0.0).all()

    def test_only_valid_regime_values(self) -> None:
        """Regime output should only contain -1.0, 0.0, or 1.0."""
        net_gex_df = _make_net_gex_series(periods=60, std=0.05, seed=77)
        result = compute_gex_regime(net_gex_df)
        valid = result.dropna(subset=["gex_regime"])
        assert set(valid["gex_regime"].unique()).issubset({-1.0, 0.0, 1.0})


# ===========================================================================
# compute_gex_flip_distance
# ===========================================================================


class TestComputeGexFlipDistance:
    """Tests for compute_gex_flip_distance."""

    def test_basic_output_shape(self) -> None:
        net_gex_df = _make_net_gex_series(periods=30)
        result = compute_gex_flip_distance(net_gex_df)

        assert not result.empty
        assert "ts_event" in result.columns
        assert "gex_flip_distance" in result.columns
        assert result["ts_event"].nunique() == len(result)

    def test_empty_input(self) -> None:
        result = compute_gex_flip_distance(pd.DataFrame())
        assert result.empty
        assert list(result.columns) == ["ts_event", "gex_flip_distance"]

    def test_positive_values_for_positive_gex(self) -> None:
        """When net_gex is consistently positive, flip distance should be positive."""
        net_gex_df = _make_net_gex_series(periods=30, mean=0.05, std=0.01, seed=10)
        result = compute_gex_flip_distance(net_gex_df)
        valid = result.dropna(subset=["gex_flip_distance"])
        assert (valid["gex_flip_distance"] > 0).all()

    def test_negative_values_for_negative_gex(self) -> None:
        """When net_gex is consistently negative, flip distance should be negative."""
        net_gex_df = _make_net_gex_series(periods=30, mean=-0.05, std=0.01, seed=11)
        result = compute_gex_flip_distance(net_gex_df)
        valid = result.dropna(subset=["gex_flip_distance"])
        assert (valid["gex_flip_distance"] < 0).all()

    def test_normalization_by_rolling_std(self) -> None:
        """Flip distance should be normalized — values should be in standard deviation units."""
        dates = pd.bdate_range("2025-01-01", periods=30, tz="UTC")
        # Known constant net_gex with some variance
        rng = np.random.default_rng(42)
        values = rng.normal(1.0, 0.5, size=30)
        net_gex_df = pd.DataFrame({"ts_event": dates, "net_gex": values})
        result = compute_gex_flip_distance(net_gex_df)
        valid = result.dropna(subset=["gex_flip_distance"])
        # Normalized values centered around ~2 std (1.0 / 0.5)
        assert valid["gex_flip_distance"].median() > 0

    def test_zero_std_returns_nan(self) -> None:
        """When rolling std is zero (constant series), flip distance should be NaN."""
        dates = pd.bdate_range("2025-01-01", periods=5, tz="UTC")
        net_gex_df = pd.DataFrame(
            {
                "ts_event": dates,
                "net_gex": [1.0, 1.0, 1.0, 1.0, 1.0],
            }
        )
        result = compute_gex_flip_distance(net_gex_df)
        # First value has NaN std (only 1 observation), rest have std=0
        # With min_periods=1, rolling std of a constant is 0 => NaN output
        # The first row has NaN std (n=1 gives NaN for sample std)
        # Rows 2+ have std=0 => np.where triggers NaN branch
        nan_count = result["gex_flip_distance"].isna().sum()
        assert nan_count >= 1


# ===========================================================================
# Output schema checks
# ===========================================================================


class TestOutputSchema:
    """Verify that compute function outputs have the expected columns and types."""

    def test_net_gex_schema(self) -> None:
        greek = _make_greek_exposure(["AAPL"], periods=30)
        result = compute_net_gex(greek)
        assert set(result.columns) == {"ts_event", "net_gex"}
        assert pd.api.types.is_datetime64_any_dtype(result["ts_event"])
        assert pd.api.types.is_float_dtype(result["net_gex"])

    def test_gex_regime_schema(self) -> None:
        net_gex_df = _make_net_gex_series(periods=30)
        result = compute_gex_regime(net_gex_df)
        assert set(result.columns) == {"ts_event", "gex_regime"}
        assert pd.api.types.is_datetime64_any_dtype(result["ts_event"])
        assert pd.api.types.is_float_dtype(result["gex_regime"])

    def test_gex_flip_distance_schema(self) -> None:
        net_gex_df = _make_net_gex_series(periods=30)
        result = compute_gex_flip_distance(net_gex_df)
        assert set(result.columns) == {"ts_event", "gex_flip_distance"}
        assert pd.api.types.is_datetime64_any_dtype(result["ts_event"])
        assert pd.api.types.is_float_dtype(result["gex_flip_distance"])


# ===========================================================================
# Pipeline tests (mocked reader)
# ===========================================================================


class TestGexRegimePipeline:
    """Tests for the GexRegimePipeline class with mocked HeberReader."""

    def _make_mock_reader(self, greek_exposure: pd.DataFrame) -> MagicMock:
        """Create a mock HeberReader that returns the given greek_exposure on read_silver."""
        mock_reader = MagicMock(spec=["read_silver", "write_gold"])
        mock_reader.read_silver.return_value = greek_exposure
        mock_reader.write_gold.return_value = "/fake/gold/path"
        return mock_reader

    def test_dry_run_does_not_write(self) -> None:
        """Dry run should compute but not call write_gold."""
        greek = _make_greek_exposure(["AAPL"], periods=30)
        mock_reader = self._make_mock_reader(greek)

        pipeline = GexRegimePipeline(reader=mock_reader)
        stats = pipeline.run(
            start_date="2025-01-01",
            end_date="2025-03-01",
            dry_run=True,
        )

        mock_reader.write_gold.assert_not_called()
        assert set(stats) == {OUTPUT_DATASET}
        assert stats[OUTPUT_DATASET]["path"] is None
        assert stats[OUTPUT_DATASET]["rows"] > 0

    def test_writes_gold_on_real_run(self) -> None:
        """Non-dry run should call write_gold with correct dataset name."""
        greek = _make_greek_exposure(["AAPL", "MSFT"], periods=40)
        mock_reader = self._make_mock_reader(greek)

        pipeline = GexRegimePipeline(reader=mock_reader, project="test", version="v2")
        pipeline.run(
            start_date="2025-01-01",
            end_date="2025-03-01",
            dry_run=False,
        )

        mock_reader.write_gold.assert_called_once()
        call_kwargs = mock_reader.write_gold.call_args
        assert call_kwargs.kwargs["dataset"] == "gex_regime_features"
        assert call_kwargs.kwargs["project"] == "test"
        assert call_kwargs.kwargs["version"] == "v2"

    def test_merged_output_has_all_columns(self) -> None:
        """Merged Gold output should contain all three feature columns plus schema columns."""
        greek = _make_greek_exposure(["AAPL"], periods=40)
        mock_reader = self._make_mock_reader(greek)

        pipeline = GexRegimePipeline(reader=mock_reader)
        pipeline.run(start_date="2025-01-01", end_date="2025-03-01", dry_run=False)

        written_df = mock_reader.write_gold.call_args.kwargs["df"]
        assert "net_gex" in written_df.columns
        assert "gex_regime" in written_df.columns
        assert "gex_flip_distance" in written_df.columns
        assert "ts_available" in written_df.columns
        assert "instrument_key" in written_df.columns
        assert (written_df["instrument_key"] == INSTRUMENT_KEY).all()

    def test_empty_silver_data(self) -> None:
        """Pipeline should handle empty Silver data gracefully."""
        mock_reader = self._make_mock_reader(pd.DataFrame())

        pipeline = GexRegimePipeline(reader=mock_reader)
        stats = pipeline.run(start_date="2025-01-01", end_date="2025-03-01")

        mock_reader.write_gold.assert_not_called()
        assert set(stats) == {OUTPUT_DATASET}
        assert stats[OUTPUT_DATASET]["status"] == "no_data"
        for ds_name in ALL_DATASETS:
            assert stats[OUTPUT_DATASET]["components"][ds_name]["status"] == "no_data"

    def test_subset_datasets(self) -> None:
        """Pipeline should only compute requested datasets."""
        greek = _make_greek_exposure(["AAPL"], periods=40)
        mock_reader = self._make_mock_reader(greek)

        pipeline = GexRegimePipeline(reader=mock_reader)
        stats = pipeline.run(
            start_date="2025-01-01",
            end_date="2025-03-01",
            datasets=("net_gex",),
            dry_run=True,
        )

        assert set(stats) == {OUTPUT_DATASET}
        components = stats[OUTPUT_DATASET]["components"]
        assert "net_gex" in components
        assert components["net_gex"]["status"] == "success"
        # gex_regime and gex_flip_distance should not be in stats
        assert "gex_regime" not in components
        assert "gex_flip_distance" not in components

    def test_reader_exception_handled(self) -> None:
        """Pipeline should handle reader exceptions gracefully."""
        mock_reader = MagicMock(spec=["read_silver", "write_gold"])
        mock_reader.read_silver.side_effect = RuntimeError("Connection failed")

        pipeline = GexRegimePipeline(reader=mock_reader)
        stats = pipeline.run(start_date="2025-01-01", end_date="2025-03-01")

        assert set(stats) == {OUTPUT_DATASET}
        assert stats[OUTPUT_DATASET]["status"] == "error"
        for ds_name in ALL_DATASETS:
            assert stats[OUTPUT_DATASET]["components"][ds_name]["status"] == "error"
