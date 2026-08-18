"""`ftd_days_outstanding` is not a real feature and must not be produced.

The column was `nunique(ftd_date)` within each `(instrument_key, date(ts_event))`
group, labelled in the code as a "proxy" for how long a fail had been open. It
never measured that. SEC fails-to-deliver records are aggregate outstanding
balances for a settlement date; how long any individual fail has been open is
not derivable from them, so there is no correct implementation to write.

What it actually produced was a second copy of the row count: with one FTD
record per settlement date in a group, `nunique(ftd_date)` and
`count(quantity)` are the same number. Measured over a 13,616-row sample of
`gold/dataset=ftd_features`, 82.6% of rows had `ftd_days_outstanding` exactly
equal to `ftd_trade_count`, and none had the value 1.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd
import pytest

from heber.features.pipelines.market_intel_features import compute_ftd_features


def _ftd_frame() -> pd.DataFrame:
    """Two settlement dates for one symbol on one event date — the shape that
    used to make the column equal the row count.
    """
    return pd.DataFrame(
        {
            "instrument_key": ["equity:AAPL", "equity:AAPL", "equity:MSFT"],
            "ts_event": [datetime(2026, 3, 9, tzinfo=UTC)] * 3,
            "ftd_date": ["2026-03-02", "2026-03-03", "2026-03-02"],
            "quantity": [100, 200, 50],
            "price": [1.0, 2.0, 3.0],
            "value": [100.0, 400.0, 150.0],
        }
    )


@pytest.mark.unit
def test_ftd_features_do_not_carry_days_outstanding() -> None:
    out = compute_ftd_features(_ftd_frame())
    assert "ftd_days_outstanding" not in out.columns


@pytest.mark.unit
def test_ftd_features_still_carry_the_real_aggregates() -> None:
    """Removing the bogus column must not disturb the ones that mean something."""
    out = compute_ftd_features(_ftd_frame())
    for col in ("ftd_quantity", "ftd_value", "ftd_trade_count", "ftd_avg_price"):
        assert col in out.columns
    aapl = out[out["instrument_key"] == "equity:AAPL"].iloc[0]
    assert aapl["ftd_quantity"] == 300
    assert aapl["ftd_trade_count"] == 2


@pytest.mark.unit
def test_ftd_features_survive_missing_ftd_date() -> None:
    """The old code had an else-branch that hardcoded the column to 1 when
    ftd_date was absent. Neither branch should exist now.
    """
    frame = _ftd_frame().drop(columns=["ftd_date"])
    out = compute_ftd_features(frame)
    assert "ftd_days_outstanding" not in out.columns
    assert len(out) == 2
