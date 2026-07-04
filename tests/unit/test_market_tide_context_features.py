"""Unit tests for market tide context feature compute functions.

Tests the standalone compute functions and MarketTideContextPipeline with mocked reader.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import numpy as np
import pandas as pd

from heber.features.pipelines.market_tide_context_features import (
    OUTPUT_DATASET,
    MarketTideContextPipeline,
    compute_market_premium_momentum,
    compute_market_sentiment_score,
)
from heber.reader import HeberReader

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_market_tide(
    start: str = "2025-01-01",
    periods: int = 30,
    freq: str = "h",
    call_base: float = 1_000_000.0,
    put_base: float = 800_000.0,
    seed: int = 42,
) -> pd.DataFrame:
    """Create synthetic hourly market_tide data."""
    rng = np.random.default_rng(seed)
    timestamps = pd.date_range(start, periods=periods, freq=freq, tz="UTC")
    return pd.DataFrame(
        {
            "ts_event": timestamps,
            "total_call_premium": call_base + rng.normal(0, 50_000, size=periods),
            "total_put_premium": put_base + rng.normal(0, 50_000, size=periods),
            "net_volume": rng.integers(-10_000, 10_000, size=periods),
            "sentiment": rng.choice(["bullish", "bearish", "neutral"], size=periods),
            "call_put_ratio": rng.uniform(0.8, 1.5, size=periods),
            "instrument_key": "market:tide",
        }
    )


def _make_daily_sentiment(
    start: str = "2025-01-01",
    periods: int = 20,
    scores: list[float] | None = None,
    seed: int = 7,
) -> pd.DataFrame:
    """Create synthetic daily sentiment series."""
    dates = pd.bdate_range(start, periods=periods, tz="UTC")
    if scores is None:
        rng = np.random.default_rng(seed)
        scores = rng.uniform(-0.5, 0.5, size=periods).tolist()
    return pd.DataFrame(
        {
            "ts_event": dates[: len(scores)],
            "market_sentiment_score": scores,
        }
    )


# ===========================================================================
# compute_market_sentiment_score
# ===========================================================================


class TestComputeMarketSentimentScore:
    """Tests for compute_market_sentiment_score."""

    def test_equal_call_put_gives_zero(self) -> None:
        """When call premium equals put premium, score should be 0.0."""
        dates = pd.date_range("2025-01-01", periods=3, freq="h", tz="UTC")
        df = pd.DataFrame(
            {
                "ts_event": dates,
                "total_call_premium": [100.0, 200.0, 300.0],
                "total_put_premium": [100.0, 200.0, 300.0],
            }
        )
        result = compute_market_sentiment_score(df)
        assert not result.empty
        assert np.allclose(result["market_sentiment_score"].values, 0.0)

    def test_all_calls_gives_one(self) -> None:
        """When put premium is 0, score should be 1.0 (max bullish)."""
        dates = pd.date_range("2025-01-01", periods=3, freq="h", tz="UTC")
        df = pd.DataFrame(
            {
                "ts_event": dates,
                "total_call_premium": [500.0, 600.0, 700.0],
                "total_put_premium": [0.0, 0.0, 0.0],
            }
        )
        result = compute_market_sentiment_score(df)
        assert not result.empty
        assert np.allclose(result["market_sentiment_score"].values, 1.0)

    def test_all_puts_gives_negative_one(self) -> None:
        """When call premium is 0, score should be -1.0 (max bearish)."""
        dates = pd.date_range("2025-01-01", periods=3, freq="h", tz="UTC")
        df = pd.DataFrame(
            {
                "ts_event": dates,
                "total_call_premium": [0.0, 0.0, 0.0],
                "total_put_premium": [500.0, 600.0, 700.0],
            }
        )
        result = compute_market_sentiment_score(df)
        assert not result.empty
        assert np.allclose(result["market_sentiment_score"].values, -1.0)

    def test_zero_premium_handled(self) -> None:
        """When both call and put premiums are zero, score should be 0.0 (not NaN/inf)."""
        dates = pd.date_range("2025-01-01", periods=2, freq="h", tz="UTC")
        df = pd.DataFrame(
            {
                "ts_event": dates,
                "total_call_premium": [0.0, 0.0],
                "total_put_premium": [0.0, 0.0],
            }
        )
        result = compute_market_sentiment_score(df)
        assert not result.empty
        assert np.allclose(result["market_sentiment_score"].values, 0.0)
        assert not result["market_sentiment_score"].isna().any()

    def test_empty_dataframe(self) -> None:
        """Empty input should return empty DataFrame with correct columns."""
        result = compute_market_sentiment_score(pd.DataFrame())
        assert result.empty
        assert list(result.columns) == ["ts_event", "market_sentiment_score"]

    def test_daily_resampling_from_hourly(self) -> None:
        """Multiple hourly rows per day should be resampled to one row per date using last."""
        # 24 hours of data across 1 day, premiums shift over the day
        dates = pd.date_range("2025-01-02 09:00", periods=8, freq="h", tz="UTC")
        df = pd.DataFrame(
            {
                "ts_event": dates,
                "total_call_premium": [100.0, 110.0, 120.0, 130.0, 140.0, 150.0, 160.0, 200.0],
                "total_put_premium": [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0],
            }
        )
        result = compute_market_sentiment_score(df)
        # Should be 1 row (one date)
        assert len(result) == 1
        # Should use last observation: call=200, put=100 -> (200-100)/(200+100) = 1/3
        expected = (200.0 - 100.0) / (200.0 + 100.0)
        assert abs(result["market_sentiment_score"].iloc[0] - expected) < 1e-10

    def test_output_range_minus_one_to_one(self) -> None:
        """Sentiment score should always be in [-1, 1]."""
        market_tide = _make_market_tide(periods=200, freq="h", seed=99)
        result = compute_market_sentiment_score(market_tide)
        valid = result.dropna(subset=["market_sentiment_score"])
        assert (valid["market_sentiment_score"] >= -1.0).all()
        assert (valid["market_sentiment_score"] <= 1.0).all()

    def test_output_schema(self) -> None:
        """Output should have exactly ts_event and market_sentiment_score columns."""
        market_tide = _make_market_tide(periods=50)
        result = compute_market_sentiment_score(market_tide)
        assert set(result.columns) == {"ts_event", "market_sentiment_score"}
        assert pd.api.types.is_datetime64_any_dtype(result["ts_event"])
        assert pd.api.types.is_float_dtype(result["market_sentiment_score"])


# ===========================================================================
# compute_market_premium_momentum
# ===========================================================================


class TestComputeMarketPremiumMomentum:
    """Tests for compute_market_premium_momentum."""

    def test_momentum_is_today_minus_rolling_mean(self) -> None:
        """Momentum = today's score - 5-day rolling mean."""
        scores = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7]
        sentiment = _make_daily_sentiment(periods=7, scores=scores)
        result = compute_market_premium_momentum(sentiment, window=5)

        # First 4 rows should be NaN (need 5 values for rolling mean)
        assert result["market_premium_momentum"].isna().sum() == 4

        # Row 5 (index 4, score=0.5): rolling mean of [0.1,0.2,0.3,0.4,0.5] = 0.3
        # momentum = 0.5 - 0.3 = 0.2
        valid = result.dropna(subset=["market_premium_momentum"])
        expected_5 = 0.5 - np.mean([0.1, 0.2, 0.3, 0.4, 0.5])
        assert abs(valid.iloc[0]["market_premium_momentum"] - expected_5) < 1e-10

        # Row 6 (index 5, score=0.6): rolling mean of [0.2,0.3,0.4,0.5,0.6] = 0.4
        # momentum = 0.6 - 0.4 = 0.2
        expected_6 = 0.6 - np.mean([0.2, 0.3, 0.4, 0.5, 0.6])
        assert abs(valid.iloc[1]["market_premium_momentum"] - expected_6) < 1e-10

    def test_empty_input(self) -> None:
        """Empty input should return empty DataFrame with correct columns."""
        result = compute_market_premium_momentum(pd.DataFrame())
        assert result.empty
        assert list(result.columns) == ["ts_event", "market_premium_momentum"]

    def test_constant_score_gives_zero_momentum(self) -> None:
        """Constant sentiment score should yield zero momentum after warmup."""
        scores = [0.3] * 10
        sentiment = _make_daily_sentiment(periods=10, scores=scores)
        result = compute_market_premium_momentum(sentiment, window=5)
        valid = result.dropna(subset=["market_premium_momentum"])
        assert len(valid) > 0
        assert np.allclose(valid["market_premium_momentum"].values, 0.0, atol=1e-10)

    def test_increasing_scores_give_positive_momentum(self) -> None:
        """Steadily increasing scores should produce positive momentum."""
        scores = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]
        sentiment = _make_daily_sentiment(periods=9, scores=scores)
        result = compute_market_premium_momentum(sentiment, window=5)
        valid = result.dropna(subset=["market_premium_momentum"])
        assert (valid["market_premium_momentum"] > 0).all()

    def test_output_schema(self) -> None:
        """Output should have exactly ts_event and market_premium_momentum columns."""
        sentiment = _make_daily_sentiment(periods=15)
        result = compute_market_premium_momentum(sentiment)
        assert set(result.columns) == {"ts_event", "market_premium_momentum"}
        assert pd.api.types.is_datetime64_any_dtype(result["ts_event"])

    def test_insufficient_data_all_nan(self) -> None:
        """Fewer rows than window size should produce all NaN momentum."""
        scores = [0.1, 0.2, 0.3]
        sentiment = _make_daily_sentiment(periods=3, scores=scores)
        result = compute_market_premium_momentum(sentiment, window=5)
        assert result["market_premium_momentum"].isna().all()


# ===========================================================================
# MarketTideContextPipeline (mocked reader)
# ===========================================================================


class TestMarketTideContextPipeline:
    """Tests for MarketTideContextPipeline with mocked HeberReader."""

    def _make_mock_reader(self, market_tide_df: pd.DataFrame) -> MagicMock:
        """Create a mock HeberReader that returns given data."""
        reader = MagicMock(spec=HeberReader)
        reader.read_silver.return_value = market_tide_df
        reader.write_gold.return_value = "/mock/gold/path"
        return reader

    def test_pipeline_runs_end_to_end(self) -> None:
        """Pipeline should compute both features and write to Gold."""
        market_tide = _make_market_tide(start="2025-01-01", periods=480, freq="h")
        reader = self._make_mock_reader(market_tide)

        pipeline = MarketTideContextPipeline(reader=reader)
        stats = pipeline.run(start_date="2025-01-01", end_date="2025-01-20")

        assert set(stats) == {OUTPUT_DATASET}
        assert stats[OUTPUT_DATASET]["status"] == "success"
        assert stats[OUTPUT_DATASET]["rows"] > 0
        components = stats[OUTPUT_DATASET]["components"]
        assert components["market_sentiment_score"]["status"] == "success"
        assert components["market_premium_momentum"]["status"] == "success"

        # Verify write_gold was called with correct dataset
        reader.write_gold.assert_called_once()
        call_kwargs = reader.write_gold.call_args
        assert call_kwargs.kwargs.get("dataset") or call_kwargs[1].get("dataset") == "market_tide_context_features"

    def test_pipeline_dry_run_does_not_write(self) -> None:
        """Dry run should compute features but not write to Gold."""
        market_tide = _make_market_tide(start="2025-01-01", periods=480, freq="h")
        reader = self._make_mock_reader(market_tide)

        pipeline = MarketTideContextPipeline(reader=reader)
        stats = pipeline.run(start_date="2025-01-01", end_date="2025-01-20", dry_run=True)

        assert stats[OUTPUT_DATASET]["status"] == "success"
        assert stats[OUTPUT_DATASET]["path"] is None
        reader.write_gold.assert_not_called()

    def test_pipeline_empty_silver_returns_no_data(self) -> None:
        """Pipeline with empty Silver data should report no_data status."""
        reader = self._make_mock_reader(pd.DataFrame())

        pipeline = MarketTideContextPipeline(reader=reader)
        stats = pipeline.run(start_date="2025-01-01", end_date="2025-01-20")

        assert set(stats) == {OUTPUT_DATASET}
        assert stats[OUTPUT_DATASET]["status"] == "no_data"
        components = stats[OUTPUT_DATASET]["components"]
        assert components["market_sentiment_score"]["status"] == "no_data"
        assert components["market_premium_momentum"]["status"] == "no_data"
        reader.write_gold.assert_not_called()

    def test_pipeline_writes_ts_available_and_instrument_key(self) -> None:
        """Gold output should include ts_available and instrument_key columns."""
        market_tide = _make_market_tide(start="2025-01-01", periods=480, freq="h")
        reader = self._make_mock_reader(market_tide)

        pipeline = MarketTideContextPipeline(reader=reader)
        pipeline.run(start_date="2025-01-01", end_date="2025-01-20")

        # Inspect the DataFrame passed to write_gold
        written_df = reader.write_gold.call_args.kwargs.get("df")
        if written_df is None:
            # Positional args
            written_df = reader.write_gold.call_args[1].get("df", reader.write_gold.call_args[0][1])
        assert "ts_available" in written_df.columns
        assert "instrument_key" in written_df.columns
        assert (written_df["instrument_key"] == "market:tide_context").all()

    def test_pipeline_reader_error_handled(self) -> None:
        """Pipeline should handle reader errors gracefully."""
        reader = MagicMock(spec=HeberReader)
        reader.read_silver.side_effect = RuntimeError("Connection failed")

        pipeline = MarketTideContextPipeline(reader=reader)
        stats = pipeline.run(start_date="2025-01-01", end_date="2025-01-20")

        assert set(stats) == {OUTPUT_DATASET}
        assert stats[OUTPUT_DATASET]["status"] == "error"
        components = stats[OUTPUT_DATASET]["components"]
        assert components["market_sentiment_score"]["status"] == "error"
        assert components["market_premium_momentum"]["status"] == "error"
        reader.write_gold.assert_not_called()
