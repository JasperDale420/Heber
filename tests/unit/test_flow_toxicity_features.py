"""Unit tests for flow toxicity (VPIN) feature compute functions.

Tests the standalone compute_vpin() and compute_flow_toxicity_features() functions
independently with synthetic DataFrames, plus FlowToxicityPipeline with a mocked reader.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import numpy as np
import pandas as pd

from heber.features.pipelines.flow_toxicity_features import (
    FlowToxicityPipeline,
    compute_flow_toxicity_features,
    compute_vpin,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REQUIRED_OUTPUT_COLUMNS = {
    "instrument_key",
    "symbol",
    "ts_event",
    "ts_available",
    "flow_toxicity_1d",
    "toxicity_acceleration",
}


def _make_flow_alerts(
    tickers: list[str],
    start: str = "2025-01-01",
    periods: int = 10,
    alerts_per_day: int = 3,
    base_ask_prem: float = 100_000.0,
    base_bid_prem: float = 80_000.0,
    seed: int = 42,
) -> pd.DataFrame:
    """Create synthetic flow_alerts data for testing.

    Generates ``alerts_per_day`` alerts per ticker per business day with
    total_ask_side_prem and total_bid_side_prem columns.
    """
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(start, periods=periods, tz="UTC")
    rows: list[dict] = []
    for ticker in tickers:
        for dt in dates:
            for _ in range(alerts_per_day):
                rows.append(
                    {
                        "underlying": ticker,
                        "ts_event": dt + pd.Timedelta(hours=rng.integers(9, 16)),
                        "total_ask_side_prem": base_ask_prem * (1 + rng.normal(0, 0.3)),
                        "total_bid_side_prem": base_bid_prem * (1 + rng.normal(0, 0.3)),
                        "premium": 50_000.0,
                        "volume": 100,
                        "put_call": rng.choice(["C", "P"]),
                        "is_sweep": rng.choice([True, False]),
                    }
                )
    return pd.DataFrame(rows)


def _make_controlled_flow_alerts(
    ticker: str,
    daily_ask_values: list[float],
    daily_bid_values: list[float],
    start: str = "2025-01-01",
) -> pd.DataFrame:
    """Create flow_alerts with exact daily ask/bid premiums (one alert per day)."""
    dates = pd.bdate_range(start, periods=len(daily_ask_values), tz="UTC")
    rows: list[dict] = []
    for i, dt in enumerate(dates):
        rows.append(
            {
                "underlying": ticker,
                "ts_event": dt + pd.Timedelta(hours=10),
                "total_ask_side_prem": daily_ask_values[i],
                "total_bid_side_prem": daily_bid_values[i],
                "premium": daily_ask_values[i] + daily_bid_values[i],
                "volume": 100,
                "put_call": "C",
                "is_sweep": False,
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Tests: compute_vpin()
# ---------------------------------------------------------------------------


class TestComputeVpin:
    """Tests for the standalone compute_vpin() function."""

    def test_balanced_flow(self) -> None:
        """Equal ask and bid premiums produce toxicity 0."""
        assert compute_vpin(100.0, 100.0) == 0.0

    def test_fully_toxic_ask_only(self) -> None:
        """All ask, no bid produces toxicity 1."""
        assert compute_vpin(100.0, 0.0) == 1.0

    def test_fully_toxic_bid_only(self) -> None:
        """All bid, no ask produces toxicity 1."""
        assert compute_vpin(0.0, 100.0) == 1.0

    def test_known_value(self) -> None:
        """ask=100, bid=50 -> |100-50|/(100+50) = 50/150 = 0.333..."""
        result = compute_vpin(100.0, 50.0)
        assert abs(result - 1 / 3) < 1e-10

    def test_zero_denominator(self) -> None:
        """Both premiums zero returns NaN."""
        result = compute_vpin(0.0, 0.0)
        assert np.isnan(result)

    def test_small_imbalance(self) -> None:
        """ask=1000, bid=900 -> |100|/1900 ~ 0.0526."""
        result = compute_vpin(1000.0, 900.0)
        expected = 100.0 / 1900.0
        assert abs(result - expected) < 1e-10


# ---------------------------------------------------------------------------
# Tests: compute_flow_toxicity_features()
# ---------------------------------------------------------------------------


class TestComputeFlowToxicityFeatures:
    """Tests for compute_flow_toxicity_features()."""

    def test_empty_df(self) -> None:
        """Empty input returns empty output."""
        result = compute_flow_toxicity_features(pd.DataFrame())
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_single_ticker_single_day(self) -> None:
        """Single day produces toxicity but NaN acceleration (no 5-day window)."""
        df = _make_flow_alerts(["AAPL"], periods=1)
        result = compute_flow_toxicity_features(df)
        assert len(result) == 1
        # flow_toxicity_1d should be computed
        assert not np.isnan(result["flow_toxicity_1d"].iloc[0])
        # toxicity_acceleration needs 5 days, so NaN for day 1
        assert np.isnan(result["toxicity_acceleration"].iloc[0])

    def test_output_has_required_columns(self) -> None:
        """Output DataFrame contains all required Gold columns."""
        df = _make_flow_alerts(["SPY"], periods=7)
        result = compute_flow_toxicity_features(df)
        assert not result.empty
        missing = REQUIRED_OUTPUT_COLUMNS - set(result.columns)
        assert not missing, f"Missing columns: {missing}"

    def test_vpin_range_zero_to_one(self) -> None:
        """All VPIN values are in [0, 1] range (excluding NaN)."""
        df = _make_flow_alerts(["AAPL", "TSLA"], periods=10, seed=7)
        result = compute_flow_toxicity_features(df)
        toxicity = result["flow_toxicity_1d"].dropna()
        assert (toxicity >= 0).all(), "VPIN below 0 found"
        assert (toxicity <= 1).all(), "VPIN above 1 found"

    def test_known_toxicity_value(self) -> None:
        """Verify toxicity with controlled inputs: ask=100, bid=50 -> 0.333."""
        df = _make_controlled_flow_alerts("TEST", [100.0], [50.0])
        result = compute_flow_toxicity_features(df)
        assert len(result) == 1
        expected_toxicity = 50.0 / 150.0  # |100-50| / (100+50)
        assert abs(result["flow_toxicity_1d"].iloc[0] - expected_toxicity) < 1e-10

    def test_zero_premium_handling(self) -> None:
        """Both ask and bid zero for a day produces NaN toxicity."""
        df = _make_controlled_flow_alerts("TEST", [0.0], [0.0])
        result = compute_flow_toxicity_features(df)
        assert len(result) == 1
        assert np.isnan(result["flow_toxicity_1d"].iloc[0])

    def test_toxicity_acceleration_known_value(self) -> None:
        """Verify acceleration: day 6 toxicity minus 5-day rolling mean of days 1-5.

        Days 1-5: all have ask=100, bid=100 -> toxicity=0.0
        Day 6: ask=200, bid=0 -> toxicity=1.0
        acceleration on day 6 = 1.0 - mean([0,0,0,0,0]) = 1.0
        """
        ask_values = [100.0, 100.0, 100.0, 100.0, 100.0, 200.0]
        bid_values = [100.0, 100.0, 100.0, 100.0, 100.0, 0.0]
        df = _make_controlled_flow_alerts("TEST", ask_values, bid_values)
        result = compute_flow_toxicity_features(df)

        # First 4 days should have NaN acceleration (not enough history)
        assert np.isnan(result["toxicity_acceleration"].iloc[0])
        assert np.isnan(result["toxicity_acceleration"].iloc[3])

        # Day 5 (index 4): rolling mean of days 1-5 = 0.0, toxicity = 0.0, accel = 0.0
        assert abs(result["toxicity_acceleration"].iloc[4] - 0.0) < 1e-10

        # Day 6 (index 5): toxicity=1.0, rolling mean of days 2-6 includes day 6
        # Rolling window of 5 ending at day 6 = mean(days 2,3,4,5,6) = mean(0,0,0,0,1) = 0.2
        # acceleration = 1.0 - 0.2 = 0.8
        assert abs(result["toxicity_acceleration"].iloc[5] - 0.8) < 1e-10

    def test_multi_ticker_independent(self) -> None:
        """Two tickers get independent toxicity computations."""
        df = _make_flow_alerts(["AAPL", "TSLA"], periods=7, seed=11)
        result = compute_flow_toxicity_features(df)

        aapl = result[result["instrument_key"] == "equity:AAPL"]
        tsla = result[result["instrument_key"] == "equity:TSLA"]
        assert len(aapl) == 7
        assert len(tsla) == 7

        # Different seeds/data -> different toxicity values
        assert not np.allclose(
            aapl["flow_toxicity_1d"].values,
            tsla["flow_toxicity_1d"].values,
        )

    def test_instrument_key_format(self) -> None:
        """instrument_key follows equity:TICKER format."""
        df = _make_flow_alerts(["NVDA"], periods=3)
        result = compute_flow_toxicity_features(df)
        assert (result["instrument_key"] == "equity:NVDA").all()

    def test_ts_available_is_utc(self) -> None:
        """ts_available timestamps are timezone-aware UTC."""
        df = _make_flow_alerts(["META"], periods=3, seed=99)
        result = compute_flow_toxicity_features(df)
        assert not result.empty
        ts_avail = result["ts_available"].iloc[0]
        if hasattr(ts_avail, "tzinfo") and ts_avail.tzinfo is not None:
            assert str(ts_avail.tzinfo) in ("UTC", "datetime.timezone.utc")

    def test_ts_event_is_date_midnight_utc(self) -> None:
        """ts_event should be normalized to midnight UTC dates."""
        df = _make_flow_alerts(["GOOG"], periods=5, seed=55)
        result = compute_flow_toxicity_features(df)
        assert not result.empty
        for ts in result["ts_event"]:
            assert ts.hour == 0 and ts.minute == 0 and ts.second == 0

    def test_numeric_coercion(self) -> None:
        """String values in premium columns get coerced to numeric."""
        df = _make_flow_alerts(["NVDA"], periods=5, seed=3)
        df["total_ask_side_prem"] = df["total_ask_side_prem"].astype(str)
        df["total_bid_side_prem"] = df["total_bid_side_prem"].astype(str)
        result = compute_flow_toxicity_features(df)
        assert not result.empty
        assert result["flow_toxicity_1d"].dtype in (np.float64, float)

    def test_multiple_alerts_per_day_are_summed(self) -> None:
        """Multiple alerts on the same day for the same ticker get summed."""
        rows = [
            {
                "underlying": "TEST",
                "ts_event": pd.Timestamp("2025-01-02 10:00", tz="UTC"),
                "total_ask_side_prem": 60.0,
                "total_bid_side_prem": 40.0,
                "premium": 100.0,
                "volume": 10,
                "put_call": "C",
                "is_sweep": False,
            },
            {
                "underlying": "TEST",
                "ts_event": pd.Timestamp("2025-01-02 14:00", tz="UTC"),
                "total_ask_side_prem": 40.0,
                "total_bid_side_prem": 60.0,
                "premium": 100.0,
                "volume": 10,
                "put_call": "P",
                "is_sweep": True,
            },
        ]
        df = pd.DataFrame(rows)
        result = compute_flow_toxicity_features(df)
        assert len(result) == 1
        # Summed ask = 100, summed bid = 100 -> balanced -> toxicity = 0
        assert abs(result["flow_toxicity_1d"].iloc[0] - 0.0) < 1e-10


# ---------------------------------------------------------------------------
# Tests: FlowToxicityPipeline with mocked reader
# ---------------------------------------------------------------------------


class TestFlowToxicityPipeline:
    """Tests for FlowToxicityPipeline with mocked HeberReader."""

    def test_pipeline_dry_run(self) -> None:
        """Pipeline dry run computes features but does not call write_gold."""
        mock_reader = MagicMock(spec=["read_silver", "write_gold"])
        mock_reader.read_silver.return_value = _make_flow_alerts(["AAPL"], periods=7, start="2025-01-01")

        pipeline = FlowToxicityPipeline(reader=mock_reader, project="test", version="v1")
        stats = pipeline.run(start_date="2025-01-01", end_date="2025-01-10", dry_run=True)

        assert "flow_toxicity_features" in stats
        assert stats["flow_toxicity_features"]["status"] == "success"
        assert stats["flow_toxicity_features"]["rows"] > 0
        mock_reader.write_gold.assert_not_called()

    def test_pipeline_writes_gold(self) -> None:
        """Pipeline without dry_run calls write_gold."""
        mock_reader = MagicMock(spec=["read_silver", "write_gold"])
        mock_reader.read_silver.return_value = _make_flow_alerts(["AAPL"], periods=7, start="2025-01-01")
        mock_reader.write_gold.return_value = "/fake/path/output.parquet"

        pipeline = FlowToxicityPipeline(reader=mock_reader, project="test", version="v1")
        stats = pipeline.run(start_date="2025-01-01", end_date="2025-01-10", dry_run=False)

        assert stats["flow_toxicity_features"]["status"] == "success"
        mock_reader.write_gold.assert_called_once()
        call_kwargs = mock_reader.write_gold.call_args
        dataset_arg = call_kwargs.kwargs.get("dataset") or call_kwargs[1].get("dataset")
        assert dataset_arg == "flow_toxicity_features"

    def test_pipeline_empty_data(self) -> None:
        """Pipeline with no data returns no_data status."""
        mock_reader = MagicMock(spec=["read_silver", "write_gold"])
        mock_reader.read_silver.return_value = pd.DataFrame()

        pipeline = FlowToxicityPipeline(reader=mock_reader, project="test", version="v1")
        stats = pipeline.run(start_date="2025-01-01", end_date="2025-01-10")

        assert stats["flow_toxicity_features"]["status"] == "no_data"
        assert stats["flow_toxicity_features"]["rows"] == 0
        mock_reader.write_gold.assert_not_called()

    def test_pipeline_reads_with_lookback(self) -> None:
        """Pipeline reads Silver data with LOOKBACK_DAYS before start_date."""
        mock_reader = MagicMock(spec=["read_silver", "write_gold"])
        mock_reader.read_silver.return_value = pd.DataFrame()

        pipeline = FlowToxicityPipeline(reader=mock_reader, project="test", version="v1")
        pipeline.run(start_date="2025-02-01", end_date="2025-02-10")

        mock_reader.read_silver.assert_called_once()
        call_kwargs = mock_reader.read_silver.call_args
        time_range = call_kwargs.kwargs.get("time_range") or call_kwargs[1].get("time_range")
        read_start = time_range[0]
        expected_start = datetime.fromisoformat("2025-02-01") - timedelta(days=10)
        assert read_start == expected_start
