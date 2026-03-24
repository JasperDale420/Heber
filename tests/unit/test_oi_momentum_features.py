"""Unit tests for OI momentum feature pipeline.

Tests the pure computation function compute_oi_momentum and the OiMomentumPipeline
orchestrator class with mocked HeberReader.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from unittest.mock import MagicMock

import pandas as pd
import pytest

from heber.features.pipelines.oi_momentum_features import (
    LOOKBACK_DAYS,
    OiMomentumPipeline,
    compute_oi_momentum,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_oi_change_row(
    symbol: str,
    ts_event: str | datetime,
    strike: float = 150.0,
    call_oi: float = 1000.0,
    put_oi: float = 500.0,
    call_oi_change: float = 100.0,
    put_oi_change: float = 50.0,
    volume: float = 200.0,
    prev_oi: float = 800.0,
) -> dict:
    """Build a single oi_change Silver row."""
    return {
        "symbol": symbol,
        "ts_event": ts_event,
        "strike": strike,
        "expiry": "2026-06-19",
        "call_oi": call_oi,
        "put_oi": put_oi,
        "call_oi_change": call_oi_change,
        "put_oi_change": put_oi_change,
        "avg_price": 5.0,
        "prev_oi": prev_oi,
        "volume": volume,
        "trades": 10,
        "instrument_key": f"equity:{symbol}",
    }


def _make_multi_day_df(
    symbol: str,
    n_days: int,
    start_date: date | None = None,
    call_oi_change: float = 100.0,
    put_oi_change: float = 50.0,
    call_oi: float = 1000.0,
    put_oi: float = 500.0,
    volume: float = 200.0,
    prev_oi: float = 800.0,
) -> pd.DataFrame:
    """Create a multi-day oi_change DataFrame for a single ticker."""
    if start_date is None:
        start_date = date(2026, 1, 1)
    rows = []
    for i in range(n_days):
        day = start_date + timedelta(days=i)
        ts = datetime(day.year, day.month, day.day, 14, 0, 0, tzinfo=UTC)
        rows.append(
            _make_oi_change_row(
                symbol=symbol,
                ts_event=ts.isoformat(),
                call_oi_change=call_oi_change,
                put_oi_change=put_oi_change,
                call_oi=call_oi,
                put_oi=put_oi,
                volume=volume,
                prev_oi=prev_oi,
            )
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Test: oi_buildup_ratio
# ---------------------------------------------------------------------------


class TestOiBuildupRatio:
    """Test oi_buildup_ratio = total_oi_change / avg_oi."""

    def test_basic_ratio_with_known_values(self):
        """Single ticker, single day, single strike: ratio should be exact."""
        # call_oi_change=100, put_oi_change=50 => total_oi_change=150
        # call_oi=1000, put_oi=500 => avg_oi (mean across 1 strike) = 1000+500 = 1500
        # ratio = 150 / 1500 = 0.1
        df = _make_multi_day_df("AAPL", n_days=5, call_oi_change=100, put_oi_change=50, call_oi=1000, put_oi=500)
        result = compute_oi_momentum(df)
        assert len(result) == 1  # Only day 5 has enough data for 5d window
        assert pytest.approx(result["oi_buildup_ratio"].iloc[0], abs=1e-6) == 0.1

    def test_negative_ratio_indicates_position_closing(self):
        """Negative OI changes produce a negative buildup ratio."""
        df = _make_multi_day_df("AAPL", n_days=5, call_oi_change=-200, put_oi_change=-100, call_oi=1000, put_oi=500)
        result = compute_oi_momentum(df)
        assert len(result) == 1
        assert result["oi_buildup_ratio"].iloc[0] < 0

    def test_multiple_strikes_aggregate_correctly(self):
        """Multiple strikes per (ticker, day) aggregate: sum OI changes, mean OI."""
        rows = []
        # Need 5 days for the rolling window
        for i in range(5):
            day_ts = datetime(2026, 1, 1 + i, 14, 0, 0, tzinfo=UTC).isoformat()
            # Strike 150: call_oi_change=100, put_oi_change=50, call_oi=1000, put_oi=500
            rows.append(
                _make_oi_change_row(
                    symbol="TSLA",
                    ts_event=day_ts,
                    strike=150,
                    call_oi_change=100,
                    put_oi_change=50,
                    call_oi=1000,
                    put_oi=500,
                    volume=200,
                    prev_oi=800,
                )
            )
            # Strike 160: call_oi_change=200, put_oi_change=100, call_oi=2000, put_oi=1000
            rows.append(
                _make_oi_change_row(
                    symbol="TSLA",
                    ts_event=day_ts,
                    strike=160,
                    call_oi_change=200,
                    put_oi_change=100,
                    call_oi=2000,
                    put_oi=1000,
                    volume=300,
                    prev_oi=1200,
                )
            )

        df = pd.DataFrame(rows)
        result = compute_oi_momentum(df)

        # total_oi_change per day = (100+50) + (200+100) = 450
        # avg_call_oi = mean(1000, 2000) = 1500, avg_put_oi = mean(500, 1000) = 750
        # avg_oi = 1500 + 750 = 2250
        # ratio = 450 / 2250 = 0.2
        assert len(result) == 1
        assert pytest.approx(result["oi_buildup_ratio"].iloc[0], abs=1e-6) == 0.2


# ---------------------------------------------------------------------------
# Test: new_position_signal
# ---------------------------------------------------------------------------


class TestNewPositionSignal:
    """Test new_position_signal binary logic."""

    def test_signal_fires_when_both_conditions_met(self):
        """Signal = 1.0 when oi_buildup_ratio > 0 AND volume/prev_oi > 1.5."""
        # volume=2000, prev_oi=800 => ratio=2.5 > 1.5 => condition met
        # positive OI change => condition met
        df = _make_multi_day_df("AAPL", n_days=5, call_oi_change=100, put_oi_change=50, volume=2000, prev_oi=800)
        result = compute_oi_momentum(df)
        assert result["new_position_signal"].iloc[0] == 1.0

    def test_signal_zero_when_buildup_negative(self):
        """Signal = 0.0 when OI is decreasing even with high volume ratio."""
        df = _make_multi_day_df("AAPL", n_days=5, call_oi_change=-100, put_oi_change=-50, volume=2000, prev_oi=800)
        result = compute_oi_momentum(df)
        assert result["new_position_signal"].iloc[0] == 0.0

    def test_signal_zero_when_volume_ratio_too_low(self):
        """Signal = 0.0 when volume/prev_oi < 1.5 even with positive buildup."""
        # volume=100, prev_oi=800 => ratio=0.125 < 1.5
        df = _make_multi_day_df("AAPL", n_days=5, call_oi_change=100, put_oi_change=50, volume=100, prev_oi=800)
        result = compute_oi_momentum(df)
        assert result["new_position_signal"].iloc[0] == 0.0


# ---------------------------------------------------------------------------
# Test: oi_change_momentum_5d
# ---------------------------------------------------------------------------


class TestOiChangeMomentum5d:
    """Test 5-day rolling sum of oi_buildup_ratio."""

    def test_rolling_sum_with_constant_ratio(self):
        """With constant daily ratio, 5d sum = 5 * daily ratio."""
        # 7 days so we get at least some output after the 5-day window
        df = _make_multi_day_df("AAPL", n_days=7, call_oi_change=100, put_oi_change=50, call_oi=1000, put_oi=500)
        result = compute_oi_momentum(df)

        # daily ratio = 150/1500 = 0.1, so 5d momentum = 0.5
        for _, row in result.iterrows():
            assert pytest.approx(row["oi_change_momentum_5d"], abs=1e-6) == 0.5

    def test_insufficient_days_produces_no_output(self):
        """Fewer than 5 days => no rows (rolling window requires min_periods=5)."""
        df = _make_multi_day_df("AAPL", n_days=4)
        result = compute_oi_momentum(df)
        assert result.empty


# ---------------------------------------------------------------------------
# Test: Multi-ticker independence
# ---------------------------------------------------------------------------


class TestMultiTickerIndependence:
    """Ensure per-ticker isolation in rolling computations."""

    def test_two_tickers_independent_rolling(self):
        """Each ticker's 5d momentum is independent of the other."""
        df_aapl = _make_multi_day_df("AAPL", n_days=6, call_oi_change=100, put_oi_change=50, call_oi=1000, put_oi=500)
        df_tsla = _make_multi_day_df("TSLA", n_days=6, call_oi_change=300, put_oi_change=150, call_oi=1000, put_oi=500)
        df = pd.concat([df_aapl, df_tsla], ignore_index=True)
        result = compute_oi_momentum(df)

        aapl = result[result["symbol"] == "AAPL"]
        tsla = result[result["symbol"] == "TSLA"]

        assert len(aapl) >= 1
        assert len(tsla) >= 1

        # AAPL: ratio = 150/1500 = 0.1, 5d sum = 0.5
        assert pytest.approx(aapl["oi_change_momentum_5d"].iloc[0], abs=1e-6) == 0.5
        # TSLA: ratio = 450/1500 = 0.3, 5d sum = 1.5
        assert pytest.approx(tsla["oi_change_momentum_5d"].iloc[0], abs=1e-6) == 1.5


# ---------------------------------------------------------------------------
# Test: Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Edge cases: empty input, zero OI, missing columns."""

    def test_empty_dataframe_returns_empty(self):
        """Empty input produces empty output without errors."""
        result = compute_oi_momentum(pd.DataFrame())
        assert result.empty

    def test_zero_oi_avoids_division_by_zero(self):
        """When avg_oi is zero, oi_buildup_ratio should be 0 (not crash)."""
        df = _make_multi_day_df("AAPL", n_days=5, call_oi=0, put_oi=0, call_oi_change=0, put_oi_change=0)
        result = compute_oi_momentum(df)
        # With zero OI and zero changes, ratio should be 0
        assert len(result) == 1
        assert result["oi_buildup_ratio"].iloc[0] == 0.0

    def test_zero_prev_oi_avoids_division_by_zero(self):
        """When prev_oi is zero, volume/prev_oi should not crash."""
        df = _make_multi_day_df(
            "AAPL", n_days=5, prev_oi=0, volume=100, call_oi_change=100, put_oi_change=50, call_oi=1000, put_oi=500
        )
        result = compute_oi_momentum(df)
        assert len(result) == 1
        # new_position_signal should be 0.0 because volume_prev_oi_ratio is 0 (not > 1.5)
        assert result["new_position_signal"].iloc[0] == 0.0

    def test_output_has_required_gold_columns(self):
        """Output must contain instrument_key, ts_event, ts_available for Gold writes."""
        df = _make_multi_day_df("AAPL", n_days=5)
        result = compute_oi_momentum(df)
        assert not result.empty
        for col in [
            "instrument_key",
            "symbol",
            "ts_event",
            "ts_available",
            "oi_buildup_ratio",
            "new_position_signal",
            "oi_change_momentum_5d",
        ]:
            assert col in result.columns, f"Missing column: {col}"

    def test_instrument_key_format(self):
        """instrument_key should be equity:SYMBOL for plain tickers."""
        df = _make_multi_day_df("MSFT", n_days=5)
        result = compute_oi_momentum(df)
        assert result["instrument_key"].iloc[0] == "equity:MSFT"


# ---------------------------------------------------------------------------
# Test: Pipeline orchestrator with mocked reader
# ---------------------------------------------------------------------------


class TestOiMomentumPipeline:
    """Test the OiMomentumPipeline class with a mocked HeberReader."""

    def test_pipeline_dry_run_does_not_write(self):
        """Dry run computes features but does not call write_gold."""
        mock_reader = MagicMock()
        mock_reader.read_silver.return_value = _make_multi_day_df("AAPL", n_days=8, start_date=date(2026, 1, 1))

        pipeline = OiMomentumPipeline(reader=mock_reader)
        stats = pipeline.run(start_date="2026-01-01", end_date="2026-01-10", dry_run=True)

        mock_reader.write_gold.assert_not_called()
        assert stats["oi_momentum_features"]["status"] == "success"
        assert stats["oi_momentum_features"]["rows"] > 0

    def test_pipeline_writes_gold_when_not_dry_run(self):
        """Non-dry-run writes to Gold via reader.write_gold."""
        mock_reader = MagicMock()
        mock_reader.read_silver.return_value = _make_multi_day_df("AAPL", n_days=8, start_date=date(2026, 1, 1))
        mock_reader.write_gold.return_value = "/fake/gold/path"

        pipeline = OiMomentumPipeline(reader=mock_reader)
        stats = pipeline.run(start_date="2026-01-01", end_date="2026-01-10", dry_run=False)

        mock_reader.write_gold.assert_called_once()
        call_kwargs = mock_reader.write_gold.call_args
        assert call_kwargs.kwargs["dataset"] == "oi_momentum_features"
        assert stats["oi_momentum_features"]["status"] == "success"

    def test_pipeline_returns_no_data_on_empty_silver(self):
        """When Silver returns empty, pipeline reports no_data."""
        mock_reader = MagicMock()
        mock_reader.read_silver.return_value = pd.DataFrame()

        pipeline = OiMomentumPipeline(reader=mock_reader)
        stats = pipeline.run(start_date="2026-01-01", end_date="2026-01-10")

        assert stats["oi_momentum_features"]["status"] == "no_data"
        assert stats["oi_momentum_features"]["rows"] == 0

    def test_pipeline_reads_silver_with_lookback(self):
        """Pipeline should request data starting LOOKBACK_DAYS before start_date."""
        mock_reader = MagicMock()
        mock_reader.read_silver.return_value = pd.DataFrame()

        pipeline = OiMomentumPipeline(reader=mock_reader)
        pipeline.run(start_date="2026-01-15", end_date="2026-01-20")

        call_args = mock_reader.read_silver.call_args
        assert call_args.args[0] == "oi_change"
        time_range = call_args.kwargs["time_range"]
        read_start = time_range[0]
        expected_start = datetime.fromisoformat("2026-01-15") - timedelta(days=LOOKBACK_DAYS)
        assert read_start == expected_start
