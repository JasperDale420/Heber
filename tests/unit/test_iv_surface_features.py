"""Unit tests for IV surface feature compute functions.

Tests the standalone compute_iv_surface_features function independently
with synthetic DataFrames.  Does NOT test the IVSurfacePipeline class
(that requires a real Heber volume).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from heber.features.pipelines.iv_surface_features import compute_iv_surface_features

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_iv_rows(
    symbol: str = "AAPL",
    date: str = "2026-01-05",
    expiries: list[dict] | None = None,
) -> pd.DataFrame:
    """Build a minimal iv_term_structure DataFrame for one symbol and date.

    Each dict in *expiries* must contain: dte, iv, call_iv, put_iv.
    The expiry column is derived from dte + date for simplicity.
    """
    if expiries is None:
        expiries = [
            {"dte": 7, "iv": 0.30, "call_iv": 0.28, "put_iv": 0.32},
            {"dte": 30, "iv": 0.25, "call_iv": 0.24, "put_iv": 0.26},
        ]
    rows = []
    base_date = pd.Timestamp(date, tz="UTC")
    for exp in expiries:
        expiry_date = (base_date + pd.Timedelta(days=exp["dte"])).date()
        rows.append(
            {
                "instrument_key": f"equity:{symbol}",
                "symbol": symbol,
                "ts_event": base_date,
                "ts_available": base_date,
                "expiry": expiry_date,
                "dte": exp["dte"],
                "iv": exp["iv"],
                "call_iv": exp["call_iv"],
                "put_iv": exp["put_iv"],
            }
        )
    return pd.DataFrame(rows)


def _make_multi_day(
    symbol: str = "AAPL",
    dates: list[str] | None = None,
    near_iv: list[float] | None = None,
    near_call_iv: list[float] | None = None,
    near_put_iv: list[float] | None = None,
) -> pd.DataFrame:
    """Build iv_term_structure rows across multiple days for iv_change_1d testing."""
    if dates is None:
        dates = ["2026-01-05", "2026-01-06"]
    if near_iv is None:
        near_iv = [0.30, 0.35]
    if near_call_iv is None:
        near_call_iv = [0.28, 0.33]
    if near_put_iv is None:
        near_put_iv = [0.32, 0.37]

    frames = []
    for i, date in enumerate(dates):
        frames.append(
            _make_iv_rows(
                symbol=symbol,
                date=date,
                expiries=[
                    {"dte": 7, "iv": near_iv[i], "call_iv": near_call_iv[i], "put_iv": near_put_iv[i]},
                    {"dte": 30, "iv": 0.25, "call_iv": 0.24, "put_iv": 0.26},
                ],
            )
        )
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestEmptyInput:
    def test_empty_input(self):
        """Empty DataFrame returns empty."""
        empty = pd.DataFrame()
        result = compute_iv_surface_features(empty)
        assert result.empty


class TestSingleExpiry:
    def test_single_expiry_no_slope(self):
        """One expiry produces put_call_iv_skew but term_structure_slope is NaN."""
        df = _make_iv_rows(
            expiries=[{"dte": 7, "iv": 0.30, "call_iv": 0.28, "put_iv": 0.32}],
        )
        result = compute_iv_surface_features(df)
        assert len(result) == 1
        assert not np.isnan(result["put_call_iv_skew"].iloc[0])
        assert np.isnan(result["term_structure_slope"].iloc[0])


class TestTwoExpiriesSlope:
    def test_two_expiries_slope(self):
        """Two expiries produce a valid slope calculation."""
        near_iv = 0.30
        far_iv = 0.25
        df = _make_iv_rows(
            expiries=[
                {"dte": 7, "iv": near_iv, "call_iv": 0.28, "put_iv": 0.32},
                {"dte": 30, "iv": far_iv, "call_iv": 0.24, "put_iv": 0.26},
            ],
        )
        result = compute_iv_surface_features(df)
        assert len(result) == 1
        expected_slope = (far_iv - near_iv) / near_iv
        assert pytest.approx(result["term_structure_slope"].iloc[0], rel=1e-6) == expected_slope


class TestPutCallSkew:
    def test_put_call_skew_positive(self):
        """put_iv > call_iv produces positive skew."""
        df = _make_iv_rows(
            expiries=[{"dte": 7, "iv": 0.30, "call_iv": 0.25, "put_iv": 0.35}],
        )
        result = compute_iv_surface_features(df)
        skew = result["put_call_iv_skew"].iloc[0]
        assert skew > 0
        expected = (0.35 - 0.25) / 0.25
        assert pytest.approx(skew, rel=1e-6) == expected

    def test_put_call_skew_negative(self):
        """call_iv > put_iv produces negative skew."""
        df = _make_iv_rows(
            expiries=[{"dte": 7, "iv": 0.30, "call_iv": 0.40, "put_iv": 0.20}],
        )
        result = compute_iv_surface_features(df)
        skew = result["put_call_iv_skew"].iloc[0]
        assert skew < 0
        expected = (0.20 - 0.40) / 0.40
        assert pytest.approx(skew, rel=1e-6) == expected


class TestIVChange1d:
    def test_iv_change_1d(self):
        """Two consecutive days produce correct day-over-day change."""
        df = _make_multi_day(
            dates=["2026-01-05", "2026-01-06"],
            near_iv=[0.30, 0.35],
        )
        result = compute_iv_surface_features(df)
        result = result.sort_values("ts_event").reset_index(drop=True)
        assert len(result) == 2
        # First day: NaN (no previous day)
        assert np.isnan(result["iv_change_1d"].iloc[0])
        # Second day: 0.35 - 0.30 = 0.05
        assert pytest.approx(result["iv_change_1d"].iloc[1], abs=1e-6) == 0.05

    def test_iv_change_first_day_nan(self):
        """First day for a ticker has iv_change_1d as NaN."""
        df = _make_iv_rows(date="2026-01-05")
        result = compute_iv_surface_features(df)
        assert len(result) == 1
        assert np.isnan(result["iv_change_1d"].iloc[0])


class TestMultiTicker:
    def test_multi_ticker(self):
        """Two tickers computed independently."""
        aapl = _make_multi_day(
            symbol="AAPL",
            dates=["2026-01-05", "2026-01-06"],
            near_iv=[0.30, 0.35],
            near_call_iv=[0.28, 0.33],
            near_put_iv=[0.32, 0.37],
        )
        tsla = _make_multi_day(
            symbol="TSLA",
            dates=["2026-01-05", "2026-01-06"],
            near_iv=[0.50, 0.60],
            near_call_iv=[0.48, 0.58],
            near_put_iv=[0.52, 0.62],
        )
        df = pd.concat([aapl, tsla], ignore_index=True)
        result = compute_iv_surface_features(df)

        # 2 tickers x 2 days = 4 rows
        assert len(result) == 4

        # Check AAPL day2 change
        aapl_rows = result[result["instrument_key"] == "equity:AAPL"].sort_values("ts_event")
        assert pytest.approx(aapl_rows["iv_change_1d"].iloc[1], abs=1e-6) == 0.05

        # Check TSLA day2 change
        tsla_rows = result[result["instrument_key"] == "equity:TSLA"].sort_values("ts_event")
        assert pytest.approx(tsla_rows["iv_change_1d"].iloc[1], abs=1e-6) == 0.10


class TestOutputColumns:
    def test_output_columns(self):
        """Output has instrument_key, ts_event, ts_available, and all 3 features."""
        df = _make_iv_rows()
        result = compute_iv_surface_features(df)
        required_cols = {
            "instrument_key",
            "ts_event",
            "ts_available",
            "put_call_iv_skew",
            "term_structure_slope",
            "iv_change_1d",
        }
        assert required_cols.issubset(set(result.columns))


class TestZeroCallIVSafeDivision:
    def test_zero_call_iv_safe_division(self):
        """call_iv=0 produces NaN skew (no division by zero)."""
        df = _make_iv_rows(
            expiries=[{"dte": 7, "iv": 0.30, "call_iv": 0.0, "put_iv": 0.32}],
        )
        result = compute_iv_surface_features(df)
        assert len(result) == 1
        assert np.isnan(result["put_call_iv_skew"].iloc[0])

    def test_zero_near_iv_safe_slope(self):
        """Near-term iv=0 produces NaN slope (no division by zero)."""
        df = _make_iv_rows(
            expiries=[
                {"dte": 7, "iv": 0.0, "call_iv": 0.0, "put_iv": 0.0},
                {"dte": 30, "iv": 0.25, "call_iv": 0.24, "put_iv": 0.26},
            ],
        )
        result = compute_iv_surface_features(df)
        assert len(result) == 1
        assert np.isnan(result["term_structure_slope"].iloc[0])
