"""Unit tests for darkpool feature compute functions.

Tests the standalone compute_darkpool_features function with synthetic DataFrames.
Does NOT test the DarkpoolPipeline class (that requires a real Heber volume).
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

import heber.features.pipelines.darkpool_features as darkpool_module
from heber.features.pipelines.darkpool_features import (
    OUTPUT_COLUMNS,
    DarkpoolPipeline,
    compute_darkpool_features,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_darkpool_trade(
    underlying: str,
    ts_event: str | pd.Timestamp,
    notional: float = 1_000_000.0,
    size: int = 10_000,
    price: float = 100.0,
    venue: str = "DARK1",
) -> dict:
    """Create a single darkpool trade row dict."""
    ts = pd.Timestamp(ts_event) if isinstance(ts_event, str) else ts_event
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return {
        "underlying": underlying,
        "ts_event": ts,
        "notional": notional,
        "size": size,
        "price": price,
        "venue": venue,
        "instrument_key": f"equity:{underlying}",
    }


def _make_flow_alert(
    underlying: str,
    ts_event: str | pd.Timestamp,
    premium: float = 50_000.0,
) -> dict:
    """Create a single flow alert row dict for premium data."""
    ts = pd.Timestamp(ts_event) if isinstance(ts_event, str) else ts_event
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return {
        "underlying": underlying,
        "ts_event": ts,
        "premium": premium,
        "put_call": "C",
    }


def _dp_df(trades: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(trades)
    if not df.empty:
        df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    return df


def _fa_df(alerts: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(alerts)
    if not df.empty:
        df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    return df


def _make_multi_day_data(
    tickers: list[str],
    n_days: int = 25,
    trades_per_day: int = 3,
    base_notional: float = 1_000_000.0,
    base_premium: float = 50_000.0,
    seed: int = 42,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create multi-day synthetic darkpool and flow_alerts data."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2025-01-01", periods=n_days, tz="UTC")

    dp_rows: list[dict] = []
    fa_rows: list[dict] = []

    for ticker in tickers:
        for dt in dates:
            for _ in range(trades_per_day):
                dp_rows.append(
                    _make_darkpool_trade(
                        ticker,
                        dt + pd.Timedelta(hours=int(rng.integers(9, 16))),
                        notional=base_notional * (1 + rng.normal(0, 0.3)),
                    )
                )
                fa_rows.append(
                    _make_flow_alert(
                        ticker,
                        dt + pd.Timedelta(hours=int(rng.integers(9, 16))),
                        premium=base_premium * (1 + rng.normal(0, 0.3)),
                    )
                )

    return pd.DataFrame(dp_rows), pd.DataFrame(fa_rows)


# ===========================================================================
# test_empty_input
# ===========================================================================


class TestEmptyInput:
    """Empty input should produce empty output with correct columns."""

    def test_empty_darkpool(self) -> None:
        result = compute_darkpool_features(pd.DataFrame(), pd.DataFrame())
        assert result.empty

    def test_empty_has_expected_columns(self) -> None:
        result = compute_darkpool_features(pd.DataFrame(), pd.DataFrame())
        expected_cols = set(OUTPUT_COLUMNS)
        assert expected_cols == set(result.columns)


# ===========================================================================
# test_single_day_single_ticker
# ===========================================================================


class TestSingleDaySingleTicker:
    """Single day with one ticker should produce one row but no z-score (insufficient history)."""

    def test_single_day(self) -> None:
        dp = _dp_df(
            [
                _make_darkpool_trade("AAPL", "2026-01-15 10:00:00", notional=500_000),
                _make_darkpool_trade("AAPL", "2026-01-15 11:00:00", notional=300_000),
            ]
        )
        fa = _fa_df(
            [
                _make_flow_alert("AAPL", "2026-01-15 10:00:00", premium=100_000),
            ]
        )
        result = compute_darkpool_features(dp, fa)

        assert len(result) == 1
        row = result.iloc[0]
        assert row["symbol"] == "AAPL"
        assert row["instrument_key"] == "equity:AAPL"
        assert row["darkpool_notional_1d"] == 800_000.0
        # Premium ratio = 800_000 / 100_000 = 8.0
        assert abs(row["darkpool_premium_ratio"] - 8.0) < 1e-6
        # Z-score should be NaN (only 1 day, need 20)
        assert np.isnan(row["darkpool_activity_zscore"])


# ===========================================================================
# test_notional_aggregation
# ===========================================================================


class TestNotionalAggregation:
    """Multiple darkpool trades on the same ticker/day should be summed."""

    def test_daily_sum(self) -> None:
        dp = _dp_df(
            [
                _make_darkpool_trade("MSFT", "2026-01-15 10:00:00", notional=100_000),
                _make_darkpool_trade("MSFT", "2026-01-15 11:00:00", notional=200_000),
                _make_darkpool_trade("MSFT", "2026-01-15 14:00:00", notional=300_000),
            ]
        )
        result = compute_darkpool_features(dp, pd.DataFrame())

        assert len(result) == 1
        assert result.iloc[0]["darkpool_notional_1d"] == 600_000.0


# ===========================================================================
# test_premium_ratio_no_premium
# ===========================================================================


class TestPremiumRatioNoPremium:
    """When there's no options premium for a ticker/day, premium_ratio should be NaN."""

    def test_no_premium_gives_nan(self) -> None:
        dp = _dp_df(
            [
                _make_darkpool_trade("AAPL", "2026-01-15 10:00:00", notional=500_000),
            ]
        )
        result = compute_darkpool_features(dp, pd.DataFrame())

        assert len(result) == 1
        assert np.isnan(result.iloc[0]["darkpool_premium_ratio"])

    def test_zero_premium_gives_nan(self) -> None:
        dp = _dp_df(
            [
                _make_darkpool_trade("AAPL", "2026-01-15 10:00:00", notional=500_000),
            ]
        )
        fa = _fa_df(
            [
                _make_flow_alert("AAPL", "2026-01-15 10:00:00", premium=0),
            ]
        )
        result = compute_darkpool_features(dp, fa)

        assert len(result) == 1
        assert np.isnan(result.iloc[0]["darkpool_premium_ratio"])


# ===========================================================================
# test_zscore_with_enough_history
# ===========================================================================


class TestZscoreWithEnoughHistory:
    """With 20+ days of constant data, z-score for day 21 should be ~0."""

    def test_constant_data_zscore_near_zero(self) -> None:
        # 21 business days of constant notional
        dates = pd.bdate_range("2025-01-01", periods=21, tz="UTC")
        dp_rows = []
        for dt in dates:
            dp_rows.append(_make_darkpool_trade("TEST", dt + pd.Timedelta(hours=10), notional=1_000_000))

        dp = pd.DataFrame(dp_rows)
        result = compute_darkpool_features(dp, pd.DataFrame())

        # First 19 days have NaN z-score (insufficient window), day 20 should have
        # rolling mean available but std=0 for constant data
        valid_rows = result.dropna(subset=["darkpool_activity_zscore"])
        assert len(valid_rows) >= 1

        # With constant notional, std=0 => z-score should be 0.0
        for _, row in valid_rows.iterrows():
            assert abs(row["darkpool_activity_zscore"]) < 1e-6


# ===========================================================================
# test_zscore_spike_detection
# ===========================================================================


class TestZscoreSpikeDetection:
    """A large notional spike after steady data should yield a high z-score."""

    def test_spike_has_high_zscore(self) -> None:
        dates = pd.bdate_range("2025-01-01", periods=22, tz="UTC")
        dp_rows = []

        # 21 days of steady notional
        for dt in dates[:21]:
            dp_rows.append(_make_darkpool_trade("SPY", dt + pd.Timedelta(hours=10), notional=1_000_000))

        # Day 22: spike at 10x normal
        dp_rows.append(_make_darkpool_trade("SPY", dates[21] + pd.Timedelta(hours=10), notional=10_000_000))

        dp = pd.DataFrame(dp_rows)
        result = compute_darkpool_features(dp, pd.DataFrame())

        # The last row (spike day) should have a very high z-score
        last_row = result.iloc[-1]
        if not np.isnan(last_row["darkpool_activity_zscore"]):
            assert last_row["darkpool_activity_zscore"] > 2.0


# ===========================================================================
# test_multi_ticker_independence
# ===========================================================================


class TestMultiTickerIndependence:
    """Different tickers should have independent z-scores and ratios."""

    def test_independent_tickers(self) -> None:
        dp, fa = _make_multi_day_data(["AAPL", "MSFT"], n_days=22, seed=99)
        result = compute_darkpool_features(dp, fa)

        aapl = result[result["symbol"] == "AAPL"]
        msft = result[result["symbol"] == "MSFT"]

        # Both should have rows
        assert len(aapl) > 0
        assert len(msft) > 0

        # Each ticker has its own data, should be different
        assert len(aapl) == len(msft)


# ===========================================================================
# test_output_columns_and_types
# ===========================================================================


class TestOutputColumnsAndTypes:
    """Verify output DataFrame has all required columns and correct types."""

    def test_all_columns_present(self) -> None:
        dp = _dp_df(
            [
                _make_darkpool_trade("AAPL", "2026-01-15 10:00:00"),
            ]
        )
        result = compute_darkpool_features(dp, pd.DataFrame())

        expected_cols = set(OUTPUT_COLUMNS)
        assert expected_cols == set(result.columns)

    def test_ts_event_is_datetime(self) -> None:
        dp = _dp_df(
            [
                _make_darkpool_trade("AAPL", "2026-01-15 10:00:00"),
            ]
        )
        result = compute_darkpool_features(dp, pd.DataFrame())
        assert pd.api.types.is_datetime64_any_dtype(result["ts_event"])

    def test_notional_is_float(self) -> None:
        dp = _dp_df(
            [
                _make_darkpool_trade("AAPL", "2026-01-15 10:00:00"),
            ]
        )
        result = compute_darkpool_features(dp, pd.DataFrame())
        assert pd.api.types.is_float_dtype(result["darkpool_notional_1d"])

    def test_instrument_key_has_equity_prefix(self) -> None:
        dp = _dp_df(
            [
                _make_darkpool_trade("NVDA", "2026-01-15 10:00:00"),
            ]
        )
        result = compute_darkpool_features(dp, pd.DataFrame())
        assert result.iloc[0]["instrument_key"] == "equity:NVDA"


# ===========================================================================
# test_numeric_coercion
# ===========================================================================


class TestNumericCoercion:
    """String values in numeric columns should be coerced gracefully."""

    def test_string_notional_coerced(self) -> None:
        dp = _dp_df(
            [
                _make_darkpool_trade("AAPL", "2026-01-15 10:00:00", notional=500_000),
            ]
        )
        dp["notional"] = dp["notional"].astype(str)

        result = compute_darkpool_features(dp, pd.DataFrame())
        assert len(result) == 1
        assert result.iloc[0]["darkpool_notional_1d"] == 500_000.0


# ===========================================================================
# test_many_records_performance
# ===========================================================================


class TestManyRecordsPerformance:
    """Verify function handles a moderate dataset without error."""

    def test_1000_trades(self) -> None:
        dp, fa = _make_multi_day_data(["AAPL", "TSLA"], n_days=25, trades_per_day=5, seed=7)
        result = compute_darkpool_features(dp, fa)
        # 25 days * 2 tickers = 50 rows
        assert len(result) == 50


class _DailyBoundedReader:
    """In-memory reader that records the pipeline's Silver read boundaries."""

    def __init__(self, frames: dict[str, pd.DataFrame]) -> None:
        self.frames = frames
        self.calls: list[tuple[str, tuple[datetime, datetime], dict[str, object]]] = []
        self.written: pd.DataFrame | None = None

    def read_silver(
        self,
        dataset: str,
        *,
        time_range: tuple[datetime, datetime],
        **kwargs: object,
    ) -> pd.DataFrame:
        self.calls.append((dataset, time_range, kwargs))
        start, end = (pd.Timestamp(value) for value in time_range)
        source = self.frames[dataset]
        event_times = pd.to_datetime(source["ts_event"], utc=True)
        return source.loc[(event_times >= start) & (event_times <= end)].copy()

    def write_gold(self, *, df: pd.DataFrame, **_kwargs: object) -> Path:
        self.written = df.copy()
        return Path("/tmp/darkpool-features.parquet")


class TestDarkpoolPipelineReduction:
    """The orchestration reads one physical day at a time before rolling work."""

    def test_daily_reads_reduce_source_frames_without_changing_features(self, monkeypatch) -> None:
        darkpool = _dp_df(
            [
                _make_darkpool_trade("AAPL", "2026-03-01 10:00:00", notional=100),
                _make_darkpool_trade("AAPL", "2026-03-01 14:00:00", notional=150),
                _make_darkpool_trade("AAPL", "2026-03-02 10:00:00", notional=300),
                _make_darkpool_trade("MSFT", "2026-03-03 11:00:00", notional=500),
                _make_darkpool_trade("AAPL", "2026-03-03 13:00:00", notional=200),
            ]
        )
        flow_alerts = _fa_df(
            [
                _make_flow_alert("AAPL", "2026-03-01 10:00:00", premium=25),
                _make_flow_alert("AAPL", "2026-03-02 10:00:00", premium=75),
                _make_flow_alert("MSFT", "2026-03-03 11:00:00", premium=100),
                _make_flow_alert("AAPL", "2026-03-03 13:00:00", premium=50),
            ]
        )
        reader = _DailyBoundedReader({"darkpool": darkpool, "flow_alerts": flow_alerts})
        monkeypatch.setattr(darkpool_module, "LOOKBACK_DAYS", 1)

        result = DarkpoolPipeline(reader=reader).run("2026-03-02", "2026-03-03")

        assert result["darkpool_features"]["status"] == "success"
        assert len(reader.calls) == 6
        for dataset, (start, end), kwargs in reader.calls:
            assert dataset in {"darkpool", "flow_alerts"}
            assert start.tzinfo == UTC
            assert end.tzinfo == UTC
            assert start.date() == end.date()
            assert end - start == pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
            assert kwargs["prune_by_dt"] is True
            assert kwargs["columns"] == ["underlying", "notional" if dataset == "darkpool" else "premium", "ts_event"]

        expected = compute_darkpool_features(darkpool, flow_alerts)
        expected = (
            expected[
                (expected["ts_event"] >= pd.Timestamp("2026-03-02", tz="UTC"))
                & (expected["ts_event"] <= pd.Timestamp("2026-03-03 23:59:59", tz="UTC"))
            ]
            .drop(columns=["ts_available"])
            .sort_values(["symbol", "date"])
            .reset_index(drop=True)
        )
        assert reader.written is not None
        written = reader.written.drop(columns=["ts_available"]).sort_values(["symbol", "date"]).reset_index(drop=True)
        assert_frame_equal(written, expected)
