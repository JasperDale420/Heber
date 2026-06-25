"""Zero-leakage regression tests for forward-return label availability.

compute_return_labels stamps ts_available from the bar that RESOLVES the
forward return (horizon trading-day bars ahead), not horizon calendar days
ahead. Calendar-day arithmetic leaked every label spanning a weekend/holiday:
HeberReader.read_asof pushes ts_available <= asof into the scan, so a backtest
would have received the label before the resolving close was observable.
"""

from __future__ import annotations

import pandas as pd
import pytest

from heber.features.pipelines.equity_features import compute_return_labels

# Consecutive XNYS trading days spanning the 2026-06-05 Fri -> 2026-06-08 Mon weekend.
_TRADING_DAYS = ["2026-06-03", "2026-06-04", "2026-06-05", "2026-06-08", "2026-06-09", "2026-06-10"]


def _daily_bars() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "instrument_key": ["equity:AAPL"] * len(_TRADING_DAYS),
            "ts_event": [pd.Timestamp(d, tz="UTC") for d in _TRADING_DAYS],
            "close": [100.0 + i for i in range(len(_TRADING_DAYS))],
        }
    )


@pytest.mark.unit
@pytest.mark.parametrize("horizon", [1, 5])
def test_ts_available_is_tz_aware_utc(horizon: int) -> None:
    out = compute_return_labels(_daily_bars(), horizon=horizon)
    assert not out.empty
    assert str(out["ts_available"].dtype) == "datetime64[ns, UTC]"
    assert out["ts_available"].dt.tz is not None


@pytest.mark.unit
@pytest.mark.parametrize("horizon", [1, 5])
def test_ts_available_after_ts_event_invariant(horizon: int) -> None:
    """write_gold enforces ts_available >= ts_event; never violate it."""
    out = compute_return_labels(_daily_bars(), horizon=horizon)
    assert (out["ts_available"] > pd.to_datetime(out["ts_event"], utc=True)).all()


@pytest.mark.unit
def test_friday_label_does_not_leak_across_weekend() -> None:
    """The 1d return for Friday resolves at Monday's close; ts_available must be
    after Monday's bar, NOT Saturday (the old calendar-day +1 result)."""
    out = compute_return_labels(_daily_bars(), horizon=1)
    fri = out[out["ts_event"] == pd.Timestamp("2026-06-05", tz="UTC")].iloc[0]
    monday_close_observable = pd.Timestamp("2026-06-09", tz="UTC")  # day after Mon 06-08
    assert fri["ts_available"] >= monday_close_observable
    # The pre-fix calendar-day bug produced 2026-06-06 (Saturday) — a leak.
    assert fri["ts_available"] != pd.Timestamp("2026-06-06", tz="UTC")


@pytest.mark.unit
def test_ts_available_uses_trading_days_not_calendar_days() -> None:
    """For horizon=5, the label at the first bar resolves at the 5th trading bar
    ahead (2026-06-10), so availability is the day after that — well past the
    5 *calendar* days (2026-06-08) the old code used."""
    out = compute_return_labels(_daily_bars(), horizon=5)
    first = out[out["ts_event"] == pd.Timestamp("2026-06-03", tz="UTC")].iloc[0]
    assert first["ts_available"] == pd.Timestamp("2026-06-11", tz="UTC")  # 06-10 resolving + 1d
    assert first["ts_available"] > pd.Timestamp("2026-06-08", tz="UTC")  # old calendar-day result
