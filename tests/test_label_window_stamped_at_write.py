"""The truncation verdict must be on the row when it is written.

`MetaLabelDatasetBuilder` recomputes it, but it is not the only consumer:
`heber/features/pipelines/ticker_base_rates.py` reads the raw Gold labels
through `HeberReader` and aggregates them without ever passing through the
builder. A label written with a short window therefore has to carry its own
flag, not depend on a downstream recomputation or on a migration having run.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from heber.ml.datasets import QUALITY_FLAG_LABEL_WINDOW_TRUNCATED
from heber.watch.checker import outcome_to_label_row
from heber.watch.models import (
    POLL_CONFIG,
    WatchHorizon,
    WatchOutcome,
    WatchStatus,
    nominal_window_hours,
)
from heber.watch.models import (
    QUALITY_FLAG_LABEL_WINDOW_TRUNCATED as WATCH_SIDE_FLAG,
)

pytestmark = pytest.mark.unit


def test_the_two_copies_of_the_flag_name_agree() -> None:
    """`heber.ml.datasets` repeats the literal to avoid an import cycle."""
    assert QUALITY_FLAG_LABEL_WINDOW_TRUNCATED == WATCH_SIDE_FLAG


def _outcome(horizon: WatchHorizon, window_hours: float, status: WatchStatus) -> WatchOutcome:
    now = datetime(2026, 3, 11, 15, 0, tzinfo=UTC)
    return WatchOutcome(
        watch_id="w1",
        alert_id="a1",
        occ_symbol="AAPL260116C00200000",
        underlying="AAPL",
        put_call="C",
        horizon=horizon,
        status=status,
        outcome_time=now,
        outcome_return=0.1,
        bars_to_hit=3,
        mfe=0.2,
        mae=-0.05,
        hit_tp_first=1 if status == WatchStatus.HIT_TP else 0,
        entry_price=1.0,
        spot_at_alert=200.0,
        alert_time=now,
        window_duration_hours=window_hours,
    )


def test_nominal_hours_are_the_poll_config_values() -> None:
    for horizon in WatchHorizon:
        assert nominal_window_hours(horizon) == float(POLL_CONFIG[horizon]["max_duration_hours"])


def test_horizons_are_whole_trading_days() -> None:
    """6.5 hours per regular session — the unit add_trading_hours spends."""
    assert nominal_window_hours(WatchHorizon.SWING) == 5 * 6.5
    assert nominal_window_hours(WatchHorizon.LEAP) == 30 * 6.5


def test_truncated_window_is_flagged_on_the_written_row() -> None:
    row = outcome_to_label_row(_outcome(WatchHorizon.SWING, 3.01, WatchStatus.EXPIRED))

    assert row["quality_flags"] == [QUALITY_FLAG_LABEL_WINDOW_TRUNCATED]


def test_a_post_window_barrier_hit_is_flagged_too() -> None:
    """The 51 legacy watches can resolve hit_tp from a snapshot past their
    stale window_end; the verdict follows the window, not the outcome."""
    row = outcome_to_label_row(_outcome(WatchHorizon.LEAP, 5.01, WatchStatus.HIT_TP))

    assert row["quality_flags"] == [QUALITY_FLAG_LABEL_WINDOW_TRUNCATED]


def test_a_full_window_is_not_flagged() -> None:
    row = outcome_to_label_row(_outcome(WatchHorizon.SWING, 200.0, WatchStatus.HIT_TP))

    assert row["quality_flags"] == []


def test_a_window_exactly_at_nominal_is_not_flagged() -> None:
    exact = nominal_window_hours(WatchHorizon.LEAP)
    row = outcome_to_label_row(_outcome(WatchHorizon.LEAP, exact, WatchStatus.EXPIRED))

    assert row["quality_flags"] == []
