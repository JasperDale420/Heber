"""Tests for floor calibration."""

from __future__ import annotations

import pytest

from heber.ops.calibrate_floors import suggest_floor_from_counts


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
