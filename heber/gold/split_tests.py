"""Tests for Train/Test Split Utilities (PRD §30)."""

from datetime import datetime, timedelta

import pytest

from heber.gold.splits import (
    DateRange,
    HoldoutSet,
    check_holdout_access,
    expanding_window_splits,
    parse_period,
    purge_window,
    walk_forward_splits,
)


class TestParsePeriod:
    """Test period string parsing."""

    def test_months(self):
        assert parse_period("12M") == timedelta(days=360)
        assert parse_period("3M") == timedelta(days=90)
        assert parse_period("1M") == timedelta(days=30)

    def test_days(self):
        assert parse_period("5d") == timedelta(days=5)
        assert parse_period("30d") == timedelta(days=30)

    def test_weeks(self):
        assert parse_period("2w") == timedelta(weeks=2)

    def test_invalid_format(self):
        with pytest.raises(ValueError):
            parse_period("invalid")


class TestDateRange:
    """Test DateRange dataclass."""

    def test_valid_range(self):
        dr = DateRange(
            start=datetime(2024, 1, 1),
            end=datetime(2024, 6, 1),
        )
        assert dr.start == datetime(2024, 1, 1)
        assert dr.end == datetime(2024, 6, 1)

    def test_invalid_range_raises(self):
        with pytest.raises(ValueError):
            DateRange(
                start=datetime(2024, 6, 1),
                end=datetime(2024, 1, 1),
            )

    def test_duration(self):
        dr = DateRange(
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 11),
        )
        assert dr.duration() == timedelta(days=10)

    def test_unpacking(self):
        dr = DateRange(
            start=datetime(2024, 1, 1),
            end=datetime(2024, 6, 1),
        )
        start, _end = dr
        assert start == datetime(2024, 1, 1)


class TestWalkForwardSplits:
    """Test walk-forward split generation (PRD §30.1)."""

    def test_basic_splits(self):
        splits = walk_forward_splits(
            start="2020-01-01",
            end="2022-01-01",
            train_period="12M",
            test_period="3M",
            step="3M",
            embargo="0d",
        )

        assert len(splits) >= 1

        first = splits[0]
        assert first.train.start == datetime(2020, 1, 1)
        assert first.train.duration() == timedelta(days=360)

    def test_with_embargo(self):
        splits = walk_forward_splits(
            start="2020-01-01",
            end="2022-01-01",
            train_period="12M",
            test_period="3M",
            step="3M",
            embargo="5d",
        )

        first = splits[0]
        embargo_gap = first.test.start - first.train.end
        assert embargo_gap >= timedelta(days=5)
        assert first.embargo_days == 5

    def test_datetime_inputs(self):
        splits = walk_forward_splits(
            start=datetime(2020, 1, 1),
            end=datetime(2022, 1, 1),
            train_period="12M",
            test_period="3M",
            step="3M",
        )

        assert len(splits) >= 1

    def test_no_overlap(self):
        splits = walk_forward_splits(
            start="2020-01-01",
            end="2023-01-01",
            train_period="12M",
            test_period="3M",
            step="3M",
            embargo="5d",
        )

        for split in splits:
            assert split.train.end < split.test.start


class TestExpandingWindowSplits:
    """Test expanding window splits (PRD §30.3)."""

    def test_training_window_grows(self):
        splits = expanding_window_splits(
            start="2020-01-01",
            end="2023-01-01",
            min_train_period="12M",
            test_period="3M",
            embargo="0d",
        )

        assert len(splits) >= 2

        assert splits[0].train.start == splits[1].train.start

        assert splits[1].train.duration() > splits[0].train.duration()

    def test_with_embargo(self):
        splits = expanding_window_splits(
            start="2020-01-01",
            end="2023-01-01",
            min_train_period="12M",
            test_period="3M",
            embargo="5d",
        )

        for split in splits:
            assert split.train.end < split.test.start


class TestHoldoutSet:
    """Test holdout set functionality (PRD §30.4)."""

    def test_contains(self):
        holdout = HoldoutSet(
            start=datetime(2024, 7, 1),
            end=datetime(2025, 1, 1),
            purpose="final_validation",
        )

        assert holdout.contains(datetime(2024, 9, 1))
        assert not holdout.contains(datetime(2024, 1, 1))

    def test_overlaps(self):
        holdout = HoldoutSet(
            start=datetime(2024, 7, 1),
            end=datetime(2025, 1, 1),
        )

        overlapping = DateRange(
            start=datetime(2024, 6, 1),
            end=datetime(2024, 8, 1),
        )
        assert holdout.overlaps(overlapping)

        non_overlapping = DateRange(
            start=datetime(2024, 1, 1),
            end=datetime(2024, 6, 1),
        )
        assert not holdout.overlaps(non_overlapping)


class TestPurgeWindow:
    """Test purge window calculation."""

    def test_purge_equals_forward_window(self):
        result = purge_window(
            datetime(2024, 1, 1),  # train_end (reserved for future use)
            forward_window="5d",
        )
        assert result == timedelta(days=5)


class TestCheckHoldoutAccess:
    """Test holdout access warning."""

    def test_warns_on_holdout_access(self):
        holdout = HoldoutSet(
            start=datetime(2024, 7, 1),
            end=datetime(2025, 1, 1),
        )

        date_range = DateRange(
            start=datetime(2024, 8, 1),
            end=datetime(2024, 9, 1),
        )

        with pytest.warns(UserWarning, match="holdout"):
            check_holdout_access(date_range, holdout, allow_final_eval=False)

    def test_no_warning_with_allow_flag(self):
        holdout = HoldoutSet(
            start=datetime(2024, 7, 1),
            end=datetime(2025, 1, 1),
        )

        date_range = DateRange(
            start=datetime(2024, 8, 1),
            end=datetime(2024, 9, 1),
        )

        check_holdout_access(date_range, holdout, allow_final_eval=True)


def run_all_split_tests() -> dict[str, bool]:
    """Run all split utility tests.

    Returns:
        Dict mapping test name to pass/fail
    """
    results = {}

    test_classes = [
        TestParsePeriod,
        TestDateRange,
        TestWalkForwardSplits,
        TestExpandingWindowSplits,
        TestHoldoutSet,
        TestPurgeWindow,
    ]

    for test_class in test_classes:
        instance = test_class()
        for method_name in dir(instance):
            if method_name.startswith("test_"):
                try:
                    getattr(instance, method_name)()
                    results[f"{test_class.__name__}.{method_name}"] = True
                except Exception as e:
                    results[f"{test_class.__name__}.{method_name}"] = False
                    print(f"FAILED: {test_class.__name__}.{method_name}: {e}")

    passed = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"\nSplit Utility Tests: {passed}/{total} passed")

    return results


if __name__ == "__main__":
    run_all_split_tests()
