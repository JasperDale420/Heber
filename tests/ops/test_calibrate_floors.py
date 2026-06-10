"""Tests for floor calibration."""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from heber.ops.calibrate_floors import calibrate, suggest_floor_from_counts


@pytest.mark.unit
def test_suggest_floor_is_ratio_of_median() -> None:
    # Buckets of 100, 200, 300 -> median 200 -> 30% -> 60.
    counts = [100, 200, 300]
    assert suggest_floor_from_counts(counts, ratio=0.3) == 60


@pytest.mark.unit
def test_suggest_floor_minimum_one() -> None:
    assert suggest_floor_from_counts([0, 1, 0], ratio=0.3) == 1


@pytest.mark.unit
def test_suggest_floor_empty_returns_one() -> None:
    assert suggest_floor_from_counts([], ratio=0.3) == 1


@pytest.mark.unit
def test_calibrate_skips_disabled_feeds() -> None:
    """Feeds disabled via floor override (floor 0) are not read — keeps calibration fast."""
    reader = MagicMock()
    reader.read_silver = MagicMock(return_value=pd.DataFrame({"ts_event": pd.to_datetime([], utc=True)}))
    calibrate(reader=reader, floor_overrides={"bars": 0, "trades": 0})
    read_feeds = {call.args[0] for call in reader.read_silver.call_args_list}
    assert read_feeds == {"flow_alerts", "darkpool"}


@pytest.mark.unit
def test_calibrate_reads_prune_by_dt() -> None:
    """Calibration reads prune to the day's partition (no full-history scan)."""
    reader = MagicMock()
    reader.read_silver = MagicMock(return_value=pd.DataFrame({"ts_event": pd.to_datetime([], utc=True)}))
    calibrate(reader=reader, floor_overrides={"bars": 0, "trades": 0})
    assert reader.read_silver.call_args_list
    for call in reader.read_silver.call_args_list:
        assert call.kwargs.get("prune_by_dt") is True
