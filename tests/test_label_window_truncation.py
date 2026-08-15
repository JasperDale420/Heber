"""Detection of alert-label rows whose observation window closed early.

``MarketCalendar.add_trading_hours`` returned a window_end far short of the
requested span until b71d63e2, so every SWING and LEAP watch was resolved after
a few hours of the alert day instead of its nominal horizon. The recorded
``window_duration_hours`` on each Gold row is the evidence: a correctly computed
window always spans at least its nominal count of *wall-clock* hours, because
the calendar only ever skips forward over non-trading time.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from heber.ml.datasets import (
    LABEL_WINDOW_TRUNCATED_COLUMN,
    QUALITY_FLAG_LABEL_WINDOW_TRUNCATED,
    add_label_window_flags,
    label_window_truncated_mask,
    quality_flag_series,
)

pytestmark = pytest.mark.unit


def _rows(*specs: tuple[str, float]) -> pd.DataFrame:
    return pd.DataFrame(
        [{"alert_id": f"a{i}", "horizon": h, "window_duration_hours": w} for i, (h, w) in enumerate(specs)]
    )


def test_truncated_swing_and_leap_windows_are_detected() -> None:
    df = _rows(("swing", 3.0087), ("leap", 5.0086))
    assert list(label_window_truncated_mask(df)) == [True, True]


def test_full_windows_are_not_detected() -> None:
    # A 120 trading-hour SWING window spans ~18.5 sessions of wall clock; a
    # 4 trading-hour INTRADAY alert fired after the close lands the next day.
    df = _rows(("swing", 448.0), ("leap", 2688.0), ("intraday", 21.4))
    assert not label_window_truncated_mask(df).any()


def test_window_ending_before_it_opened_is_detected() -> None:
    """The tz relabel made window_end earlier than alert_time on some rows."""
    df = _rows(("intraday", -2.48), ("leap", -1.49))
    assert list(label_window_truncated_mask(df)) == [True, True]


def test_window_exactly_at_nominal_span_is_not_detected() -> None:
    df = _rows(("intraday", 4.0), ("swing", 120.0), ("leap", 720.0))
    assert not label_window_truncated_mask(df).any()


def test_unverifiable_rows_are_detected() -> None:
    """A null horizon or duration cannot be shown correct, so it is not assumed correct."""
    df = _rows(("swing", float("nan")), ("nonsense", 5.0))
    assert list(label_window_truncated_mask(df)) == [True, True]


def test_horizon_case_and_enum_prefix_are_tolerated() -> None:
    df = _rows(("SWING", 3.0), ("WatchHorizon.LEAP", 5.0))
    assert list(label_window_truncated_mask(df)) == [True, True]


def test_frame_without_the_columns_is_left_alone() -> None:
    df = pd.DataFrame({"alert_id": ["a"], "outcome": ["hit_tp"]})
    assert not label_window_truncated_mask(df).any()
    assert add_label_window_flags(df).equals(df)


def test_flag_is_appended_to_quality_flags() -> None:
    df = _rows(("swing", 3.0), ("swing", 448.0))
    out = add_label_window_flags(df)
    flags = quality_flag_series(out)
    assert QUALITY_FLAG_LABEL_WINDOW_TRUNCATED in flags[0]
    assert flags[1] == []


def test_flagging_a_legacy_partition_without_quality_flags_works() -> None:
    """labels_alert_barriers has never had a quality_flags column."""
    df = _rows(("leap", 5.0))
    assert "quality_flags" not in df.columns
    out = add_label_window_flags(df)
    assert out.at[0, "quality_flags"] == [QUALITY_FLAG_LABEL_WINDOW_TRUNCATED]


def test_existing_flags_are_preserved_and_rerun_is_a_noop() -> None:
    df = _rows(("swing", 3.0))
    df["quality_flags"] = pd.Series([np.array(["some_other_flag"], dtype=object)], dtype=object)

    once = add_label_window_flags(df)
    assert sorted(once.at[0, "quality_flags"]) == sorted(["some_other_flag", QUALITY_FLAG_LABEL_WINDOW_TRUNCATED])

    twice = add_label_window_flags(once)
    assert twice.at[0, "quality_flags"].count(QUALITY_FLAG_LABEL_WINDOW_TRUNCATED) == 1


def test_input_frame_is_not_mutated() -> None:
    df = _rows(("swing", 3.0))
    add_label_window_flags(df)
    assert "quality_flags" not in df.columns


def test_duplicate_index_is_handled() -> None:
    df = _rows(("swing", 3.0), ("swing", 448.0))
    df.index = pd.Index([7, 7])
    out = add_label_window_flags(df)
    assert list(quality_flag_series(out))[0] == [QUALITY_FLAG_LABEL_WINDOW_TRUNCATED]
    assert list(quality_flag_series(out))[1] == []


def test_mask_column_name_matches_the_flag() -> None:
    assert LABEL_WINDOW_TRUNCATED_COLUMN == QUALITY_FLAG_LABEL_WINDOW_TRUNCATED
