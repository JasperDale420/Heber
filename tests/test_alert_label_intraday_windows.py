"""Regression tests for intraday alert-label window durations."""

from __future__ import annotations

from datetime import timedelta

import pandas as pd
import pytest

from heber.features.templates.alert_labels import (
    MARKET_PROXY,
    BarrierConfig,
    VixRegime,
    _compute_beta_neutral_return,
    _compute_spy_relative_return,
    _get_entry_data,
    _get_vix_at_alert,
    _process_single_alert,
    _window_delta_for_config,
    classify_vix_regime,
)


def test_window_delta_for_intraday_uses_minutes() -> None:
    config = BarrierConfig.intraday()
    assert _window_delta_for_config(config) == timedelta(hours=2)


def test_window_delta_for_swing_uses_days() -> None:
    config = BarrierConfig.swing()
    assert _window_delta_for_config(config) == timedelta(days=5)


def test_spy_relative_return_uses_intraday_end_time() -> None:
    ts_alert = pd.Timestamp("2026-01-02T14:30:00Z")
    config = BarrierConfig.intraday()
    spy_bars = pd.DataFrame(
        {
            "instrument_key": [MARKET_PROXY, MARKET_PROXY, MARKET_PROXY],
            "bar_start_ts": [
                ts_alert,
                ts_alert + pd.Timedelta(hours=2),
                ts_alert + pd.Timedelta(days=1),
            ],
            "close": [100.0, 110.0, 120.0],
        }
    )

    beta_neutral = _compute_spy_relative_return(spy_bars, ts_alert, config, raw_return=0.10)

    # Uses +2h spy return (10%), not +1d return (20%).
    assert beta_neutral == pytest.approx(0.0)


def test_process_single_alert_sets_intraday_ts_available_in_minutes() -> None:
    ts_alert = pd.Timestamp("2026-01-02T14:30:00Z")
    alert = pd.Series(
        {
            "event_id": "evt-1",
            "underlying": "equity:AAPL",
            "ts_event": ts_alert,
            "put_call": "C",
            "expiry": (ts_alert + pd.Timedelta(days=1)).date(),
            "spot_px": 100.0,
        }
    )
    bars = pd.DataFrame(
        {
            "instrument_key": ["equity:AAPL", "equity:AAPL", "equity:AAPL"],
            "bar_start_ts": [
                ts_alert - pd.Timedelta(minutes=5),
                ts_alert + pd.Timedelta(minutes=5),
                ts_alert + pd.Timedelta(minutes=10),
            ],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.0, 101.0, 102.0],
            "atr": [1.0, 1.0, 1.0],
        }
    )

    result = _process_single_alert(
        alert=alert,
        bars=bars,
        spy_bars=None,
        vix_data=None,
        config=BarrierConfig.intraday(),
        slippage_model=None,
    )

    assert result["ts_available"] == ts_alert + pd.Timedelta(hours=2)


def test_get_entry_data_uses_bar_close_when_alert_spot_is_nan() -> None:
    ts_alert = pd.Timestamp("2026-01-02T14:30:00Z")
    underlying_bars = pd.DataFrame(
        {
            "instrument_key": ["equity:AAPL", "equity:AAPL"],
            "bar_start_ts": [ts_alert - pd.Timedelta(minutes=5), ts_alert],
            "close": [99.0, 100.0],
            "atr": [1.0, 1.25],
        }
    )
    alert = pd.Series({"spot_px": float("nan")})

    result = _get_entry_data(underlying_bars, ts_alert, alert)

    assert result is not None
    atr_at_alert, spot_at_alert = result
    assert atr_at_alert == pytest.approx(1.25)
    assert spot_at_alert == pytest.approx(100.0)


def test_spy_relative_return_returns_none_for_infinite_spy_move() -> None:
    ts_alert = pd.Timestamp("2026-01-02T14:30:00Z")
    config = BarrierConfig.intraday()
    spy_bars = pd.DataFrame(
        {
            "instrument_key": [MARKET_PROXY, MARKET_PROXY],
            "bar_start_ts": [
                ts_alert,
                ts_alert + pd.Timedelta(hours=2),
            ],
            "close": [100.0, float("inf")],
        }
    )

    beta_neutral = _compute_spy_relative_return(spy_bars, ts_alert, config, raw_return=0.10)

    assert beta_neutral is None


def test_classify_vix_regime_returns_normal_for_non_finite_vix() -> None:
    assert classify_vix_regime(float("nan")) == VixRegime.NORMAL.value
    assert classify_vix_regime(float("inf")) == VixRegime.NORMAL.value
    assert classify_vix_regime(float("-inf")) == VixRegime.NORMAL.value


def test_get_vix_at_alert_returns_none_for_non_finite_close() -> None:
    ts_alert = pd.Timestamp("2026-01-02T14:30:00Z")
    vix_data = pd.DataFrame(
        {
            "bar_start_ts": [ts_alert - pd.Timedelta(minutes=5), ts_alert],
            "close": [20.0, float("inf")],
        }
    )

    assert _get_vix_at_alert(vix_data, ts_alert) is None


def test_compute_beta_neutral_return_returns_none_for_non_finite_inputs() -> None:
    assert _compute_beta_neutral_return(float("inf"), 0.02) is None
    assert _compute_beta_neutral_return(0.05, float("inf")) is None
    assert _compute_beta_neutral_return(0.05, 0.02, beta=float("nan")) is None
