"""Unit tests for straddle momentum feature compute functions.

Tests the standalone compute functions independently with synthetic DataFrames.
Pipeline class tests use mocked HeberReader.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from heber.features.pipelines.straddle_momentum_features import (
    StraddleMomentumPipeline,
    _expand_chain_json,
    _select_eod_snapshots,
    compute_straddle_returns,
    select_atm_options,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_option_chain(
    underlyings: list[str],
    start: str = "2025-01-01",
    periods: int = 30,
    base_strike: float = 100.0,
    strikes_per_side: int = 5,
    seed: int = 42,
) -> pd.DataFrame:
    """Create a synthetic option chain snapshot with calls and puts.

    Generates multiple strikes around base_strike for each (underlying, date)
    with realistic-ish bid/ask/delta values.
    """
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(start, periods=periods, tz="UTC")
    rows = []

    for underlying in underlyings:
        for dt in dates:
            for offset in range(-strikes_per_side, strikes_per_side + 1):
                strike = base_strike + offset * 5.0

                # Call option
                call_mid = max(0.5, 10.0 - abs(offset) * 1.5 + rng.normal(0, 0.3))
                call_delta = max(0.05, min(0.95, 0.5 - offset * 0.08 + rng.normal(0, 0.02)))
                rows.append(
                    {
                        "underlying": underlying,
                        "ts_event": dt,
                        "strike": strike,
                        "put_call": "C",
                        "delta": call_delta,
                        "bid": max(0.1, call_mid - 0.2),
                        "ask": call_mid + 0.2,
                        "last": call_mid,
                        "instrument_key": f"option:OCC:{underlying}250117C{int(strike * 1000):08d}",
                        "expiry": "2025-01-17",
                        "iv": 0.25 + rng.normal(0, 0.03),
                        "volume": int(rng.integers(10, 1000)),
                        "open_interest": int(rng.integers(100, 10000)),
                        "gamma": 0.05 + rng.normal(0, 0.01),
                        "theta": -0.05 + rng.normal(0, 0.01),
                        "vega": 0.15 + rng.normal(0, 0.02),
                    }
                )

                # Put option
                put_mid = max(0.5, 10.0 - abs(offset) * 1.5 + rng.normal(0, 0.3))
                put_delta = max(-0.95, min(-0.05, -0.5 + offset * 0.08 + rng.normal(0, 0.02)))
                rows.append(
                    {
                        "underlying": underlying,
                        "ts_event": dt,
                        "strike": strike,
                        "put_call": "P",
                        "delta": put_delta,
                        "bid": max(0.1, put_mid - 0.2),
                        "ask": put_mid + 0.2,
                        "last": put_mid,
                        "instrument_key": f"option:OCC:{underlying}250117P{int(strike * 1000):08d}",
                        "expiry": "2025-01-17",
                        "iv": 0.25 + rng.normal(0, 0.03),
                        "volume": int(rng.integers(10, 1000)),
                        "open_interest": int(rng.integers(100, 10000)),
                        "gamma": 0.05 + rng.normal(0, 0.01),
                        "theta": -0.05 + rng.normal(0, 0.01),
                        "vega": 0.15 + rng.normal(0, 0.02),
                    }
                )

    return pd.DataFrame(rows)


def _make_straddle_values(
    underlyings: list[str],
    start: str = "2025-01-01",
    periods: int = 70,
    base_value: float = 10.0,
    seed: int = 42,
) -> pd.DataFrame:
    """Create synthetic daily straddle values."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(start, periods=periods, tz="UTC")
    rows = []
    for underlying in underlyings:
        value = base_value
        for dt in dates:
            value *= 1 + rng.normal(-0.002, 0.05)  # Slight decay (options lose value)
            value = max(0.5, value)
            rows.append(
                {
                    "underlying": underlying,
                    "date": dt.date(),
                    "straddle_value": value,
                }
            )
    return pd.DataFrame(rows)


def _per_day_reader(chain: pd.DataFrame):
    """Return a ``read_silver`` side_effect that slices ``chain`` to the requested
    ``time_range`` — simulates per-partition (per-day) Silver reads.
    """

    def _read(dataset: str, time_range=None, columns=None, **kwargs) -> pd.DataFrame:
        if time_range is None:
            return chain.copy()
        start = pd.to_datetime(time_range[0], utc=True)
        end = pd.to_datetime(time_range[1], utc=True)
        ts = pd.to_datetime(chain["ts_event"], utc=True)
        return chain[(ts >= start) & (ts <= end)].copy()

    return _read


def _make_chain_json_snapshots(
    underlyings: list[str],
    dates: list[str],
    snaps_per_day: int = 3,
    atm_strike: float = 100.0,
) -> pd.DataFrame:
    """Build ``option_chain_snapshot`` rows in the real ``chain_json`` schema.

    Emits ``snaps_per_day`` intraday snapshots per (underlying, date). Each
    snapshot's ATM straddle mid rises with the snapshot index, so the closing
    (last) snapshot is identifiable: at index ``s`` the ATM mid is ``5 + s``.
    """
    rows = []
    for u in underlyings:
        for d in dates:
            for s in range(snaps_per_day):
                ts = pd.Timestamp(d, tz="UTC") + pd.Timedelta(hours=10 + s)
                mid = 5.0 + s
                contracts = [
                    {
                        "strike": atm_strike,
                        "option_type": "call",
                        "delta": 0.5,
                        "bid": mid - 0.1,
                        "ask": mid + 0.1,
                        "contract_symbol": f"{u}_C_ATM",
                    },
                    {
                        "strike": atm_strike,
                        "option_type": "put",
                        "delta": -0.5,
                        "bid": mid - 0.1,
                        "ask": mid + 0.1,
                        "contract_symbol": f"{u}_P_ATM",
                    },
                    {
                        "strike": atm_strike + 10,
                        "option_type": "call",
                        "delta": 0.2,
                        "bid": 1.0,
                        "ask": 1.2,
                        "contract_symbol": f"{u}_C_OTM",
                    },
                    {
                        "strike": atm_strike - 10,
                        "option_type": "put",
                        "delta": -0.2,
                        "bid": 1.0,
                        "ask": 1.2,
                        "contract_symbol": f"{u}_P_OTM",
                    },
                ]
                rows.append(
                    {
                        "underlying": u,
                        "ts_event": ts,
                        "chain_json": json.dumps({"data": {"contracts": contracts}}),
                    }
                )
    return pd.DataFrame(rows)


# ===========================================================================
# _select_eod_snapshots (end-of-day chain reduction)
# ===========================================================================


class TestSelectEodSnapshots:
    """The straddle value is taken from each day's closing chain per underlying."""

    def test_keeps_one_latest_snapshot_per_underlying(self) -> None:
        snaps = _make_chain_json_snapshots(["AAPL", "MSFT"], ["2026-01-02"], snaps_per_day=3)
        eod = _select_eod_snapshots(snaps)

        assert len(eod) == 2  # one per underlying
        # Closing snapshot is the s=2 row → hour 12
        assert {pd.to_datetime(t, utc=True).hour for t in eod["ts_event"]} == {12}

    def test_eod_chain_drives_daily_straddle_value(self) -> None:
        # 3 intraday snapshots: ATM mid 5, 6, 7 → EOD straddle = 7 + 7 = 14.
        snaps = _make_chain_json_snapshots(["AAPL"], ["2026-01-02"], snaps_per_day=3)
        vals = select_atm_options(_expand_chain_json(_select_eod_snapshots(snaps)))

        assert len(vals) == 1
        assert vals.iloc[0]["straddle_value"] == pytest.approx(14.0)

    def test_passthrough_without_required_columns(self) -> None:
        df = pd.DataFrame({"foo": [1, 2]})
        pd.testing.assert_frame_equal(_select_eod_snapshots(df), df)


# ===========================================================================
# select_atm_options
# ===========================================================================


class TestSelectAtmOptions:
    """Tests for ATM option selection."""

    def test_basic_output_shape(self) -> None:
        chain = _make_option_chain(["AAPL"], periods=5)
        result = select_atm_options(chain)

        assert not result.empty
        assert "underlying" in result.columns
        assert "date" in result.columns
        assert "straddle_value" in result.columns
        assert "atm_call_mid" in result.columns
        assert "atm_put_mid" in result.columns

    def test_empty_input(self) -> None:
        result = select_atm_options(pd.DataFrame())
        assert result.empty
        assert "straddle_value" in result.columns

    def test_one_row_per_underlying_date(self) -> None:
        """Should have at most one ATM straddle per (underlying, date)."""
        chain = _make_option_chain(["AAPL", "MSFT"], periods=10)
        result = select_atm_options(chain)

        assert not result.empty
        # Check no duplicates
        deduped = result.drop_duplicates(subset=["underlying", "date"])
        assert len(deduped) == len(result)

    def test_straddle_value_positive(self) -> None:
        """Straddle value (call_mid + put_mid) must be positive."""
        chain = _make_option_chain(["AAPL"], periods=10)
        result = select_atm_options(chain)

        assert not result.empty
        assert (result["straddle_value"] > 0).all()

    def test_atm_selection_by_delta(self) -> None:
        """When delta is available, ATM should pick |delta| closest to 0.5."""
        # Create minimal chain with known deltas
        rows = [
            {
                "underlying": "TEST",
                "ts_event": pd.Timestamp("2025-01-02", tz="UTC"),
                "strike": 95,
                "put_call": "C",
                "delta": 0.8,
                "bid": 8.0,
                "ask": 8.5,
                "last": 8.25,
            },
            {
                "underlying": "TEST",
                "ts_event": pd.Timestamp("2025-01-02", tz="UTC"),
                "strike": 100,
                "put_call": "C",
                "delta": 0.52,
                "bid": 5.0,
                "ask": 5.5,
                "last": 5.25,
            },
            {
                "underlying": "TEST",
                "ts_event": pd.Timestamp("2025-01-02", tz="UTC"),
                "strike": 105,
                "put_call": "C",
                "delta": 0.2,
                "bid": 2.0,
                "ask": 2.5,
                "last": 2.25,
            },
            {
                "underlying": "TEST",
                "ts_event": pd.Timestamp("2025-01-02", tz="UTC"),
                "strike": 95,
                "put_call": "P",
                "delta": -0.2,
                "bid": 2.0,
                "ask": 2.5,
                "last": 2.25,
            },
            {
                "underlying": "TEST",
                "ts_event": pd.Timestamp("2025-01-02", tz="UTC"),
                "strike": 100,
                "put_call": "P",
                "delta": -0.48,
                "bid": 4.5,
                "ask": 5.0,
                "last": 4.75,
            },
            {
                "underlying": "TEST",
                "ts_event": pd.Timestamp("2025-01-02", tz="UTC"),
                "strike": 105,
                "put_call": "P",
                "delta": -0.8,
                "bid": 8.0,
                "ask": 8.5,
                "last": 8.25,
            },
        ]
        chain = pd.DataFrame(rows)
        result = select_atm_options(chain)

        assert len(result) == 1
        # ATM call should be strike=100 (delta=0.52, closest to 0.5)
        assert result.iloc[0]["atm_call_mid"] == pytest.approx(5.25)
        # ATM put should be strike=100 (|delta|=0.48, closest to 0.5)
        assert result.iloc[0]["atm_put_mid"] == pytest.approx(4.75)

    def test_no_puts_returns_empty(self) -> None:
        """If no puts exist, cannot form straddle."""
        rows = [
            {
                "underlying": "TEST",
                "ts_event": pd.Timestamp("2025-01-02", tz="UTC"),
                "strike": 100,
                "put_call": "C",
                "delta": 0.5,
                "bid": 5.0,
                "ask": 5.5,
                "last": 5.25,
            },
        ]
        chain = pd.DataFrame(rows)
        result = select_atm_options(chain)
        assert result.empty


# ===========================================================================
# compute_straddle_returns
# ===========================================================================


class TestComputeStraddleReturns:
    """Tests for straddle return computation."""

    def test_basic_output_shape(self) -> None:
        values = _make_straddle_values(["AAPL"], periods=70)
        result = compute_straddle_returns(values)

        assert not result.empty
        assert "instrument_key" in result.columns
        assert "ts_event" in result.columns
        assert "straddle_return_1m" in result.columns
        assert "straddle_return_3m" in result.columns

    def test_empty_input(self) -> None:
        result = compute_straddle_returns(pd.DataFrame())
        assert result.empty
        assert set(result.columns) == {"instrument_key", "ts_event", "straddle_return_1m", "straddle_return_3m"}

    def test_1m_return_available_after_20_days(self) -> None:
        """1-month return should only be available after 20 trading days."""
        values = _make_straddle_values(["AAPL"], periods=25)
        result = compute_straddle_returns(values, window_1m=20, window_3m=60)

        # Should have 1m returns starting at row 20
        valid_1m = result.dropna(subset=["straddle_return_1m"])
        assert len(valid_1m) > 0
        assert len(valid_1m) <= 5  # Only 25-20=5 rows can have 1m return

    def test_3m_return_requires_60_days(self) -> None:
        """3-month return should not be available with fewer than 60 trading days."""
        values = _make_straddle_values(["AAPL"], periods=50)
        result = compute_straddle_returns(values, window_1m=20, window_3m=60)

        # No 3m returns should exist
        assert result["straddle_return_3m"].isna().all()

    def test_return_calculation_correct(self) -> None:
        """Verify return = (current - past) / past."""
        dates = pd.bdate_range("2025-01-01", periods=25, tz="UTC")
        values = pd.DataFrame(
            {
                "underlying": "AAPL",
                "date": [d.date() for d in dates],
                "straddle_value": [10.0] * 20 + [12.0] * 5,  # Jump at day 20
            }
        )
        result = compute_straddle_returns(values, window_1m=20, window_3m=60)

        valid_1m = result.dropna(subset=["straddle_return_1m"])
        assert not valid_1m.empty
        # First valid 1m return: (12 - 10) / 10 = 0.2
        assert valid_1m.iloc[0]["straddle_return_1m"] == pytest.approx(0.2)

    def test_instrument_key_format(self) -> None:
        """instrument_key should use equity: prefix."""
        values = _make_straddle_values(["AAPL"], periods=25)
        result = compute_straddle_returns(values)

        assert not result.empty
        assert all(k.startswith("equity:") for k in result["instrument_key"])

    def test_multiple_underlyings_independent(self) -> None:
        """Each underlying should have independent straddle returns."""
        values = _make_straddle_values(["AAPL", "MSFT"], periods=30)
        result = compute_straddle_returns(values, window_1m=20, window_3m=60)

        tickers = result["instrument_key"].unique()
        assert len(tickers) == 2


# ===========================================================================
# StraddleMomentumPipeline (with mocked reader)
# ===========================================================================


class TestStraddleMomentumPipeline:
    """Tests for the pipeline class with mocked HeberReader."""

    def test_pipeline_no_data(self) -> None:
        """Pipeline should return no_data when Silver has no option chain data."""
        mock_reader = MagicMock()
        mock_reader.read_silver.return_value = pd.DataFrame()

        pipeline = StraddleMomentumPipeline(reader=mock_reader)
        stats = pipeline.run(start_date="2025-01-01", end_date="2025-03-31", dry_run=True)

        assert stats["status"] == "no_data"
        assert stats["rows"] == 0

    def test_pipeline_dry_run_does_not_write(self) -> None:
        """Pipeline dry run should compute but not call write_gold."""
        chain = _make_option_chain(["AAPL"], periods=70, start="2024-10-01")

        mock_reader = MagicMock()
        mock_reader.read_silver.side_effect = _per_day_reader(chain)

        pipeline = StraddleMomentumPipeline(reader=mock_reader)
        pipeline.run(start_date="2025-01-01", end_date="2025-03-31", dry_run=True)

        # May or may not have data depending on date filtering, but should not write
        mock_reader.write_gold.assert_not_called()

    def test_pipeline_loads_incrementally(self) -> None:
        """Pipeline must read option_chain_snapshot one day at a time to bound peak
        memory — never load the whole lookback window in a single read.

        Regression for the gold-poller OOM: loading the full ~71-day window of the
        12 GB option_chain_snapshot feed at once exhausted the Docker VM, killing the
        poller before the pipelines after straddle_momentum could run.
        """
        chain = _make_option_chain(["AAPL"], periods=70, start="2024-10-01")

        mock_reader = MagicMock()
        mock_reader.read_silver.side_effect = _per_day_reader(chain)

        pipeline = StraddleMomentumPipeline(reader=mock_reader)
        pipeline.run(start_date="2025-01-01", end_date="2025-03-31", dry_run=True)

        # Many per-day reads, not a single bulk read over the full span.
        assert mock_reader.read_silver.call_count > 30

        # Every read must be a bounded (<= ~1 day) window — never the multi-month span.
        for call in mock_reader.read_silver.call_args_list:
            time_range = call.kwargs.get("time_range")
            if time_range is None and len(call.args) > 1:
                time_range = call.args[1]
            span = pd.to_datetime(time_range[1], utc=True) - pd.to_datetime(time_range[0], utc=True)
            assert span <= pd.Timedelta(days=1, seconds=1)

    def test_pipeline_runs_chain_json_eod_path(self) -> None:
        """End-to-end through the real chain_json schema: per-day reads → EOD
        snapshot reduction → expand → reduce → trailing returns."""
        dates = pd.bdate_range("2024-10-01", periods=75, tz="UTC")
        snaps = _make_chain_json_snapshots(["AAPL"], [d.date().isoformat() for d in dates], snaps_per_day=3)

        mock_reader = MagicMock()
        mock_reader.read_silver.side_effect = _per_day_reader(snaps)

        pipeline = StraddleMomentumPipeline(reader=mock_reader)
        stats = pipeline.run(start_date="2025-01-01", end_date="2025-01-10", dry_run=True)

        assert stats["status"] == "success"
        assert stats["rows"] > 0
        assert mock_reader.read_silver.call_count > 30  # per-day reads, not one bulk read
