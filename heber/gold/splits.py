"""Train/Test Split Utilities (PRD §30).

Provides walk-forward and expanding window splits for time-series
ML experiments with proper embargo periods to prevent leakage.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import structlog

from heber.gold.duration import parse_duration

logger = structlog.get_logger(__name__)

# Backwards-compatible alias
parse_period = parse_duration


@dataclass
class DateRange:
    """A date range for train or test periods."""

    start: datetime
    end: datetime

    def __post_init__(self):
        if self.start >= self.end:
            raise ValueError(f"start must be before end: {self.start} >= {self.end}")

    def __iter__(self):
        return iter((self.start, self.end))

    def duration(self) -> timedelta:
        return self.end - self.start


@dataclass
class TrainTestSplit:
    """A single train/test split with embargo gap."""

    train: DateRange
    test: DateRange
    embargo_days: int = 0

    def __iter__(self):
        return iter((self.train, self.test))


@dataclass
class HoldoutSet:
    """Holdout set for final validation (PRD §30.4).

    The SDK can warn when accessing holdout data outside final eval.
    """

    start: datetime
    end: datetime
    purpose: str = "final_validation"

    def contains(self, dt: datetime) -> bool:
        """Check if a datetime falls within the holdout period."""
        return self.start <= dt <= self.end

    def overlaps(self, date_range: DateRange) -> bool:
        """Check if a date range overlaps with the holdout period."""
        return not (date_range.end < self.start or date_range.start > self.end)


def walk_forward_splits(
    start: str | datetime,
    end: str | datetime,
    train_period: str,
    test_period: str,
    step: str,
    embargo: str = "0d",
) -> list[TrainTestSplit]:
    """Generate walk-forward cross-validation splits (PRD §30.1).

    Creates rolling train/test windows with fixed-size training periods.

    Args:
        start: Start date (YYYY-MM-DD or datetime)
        end: End date (YYYY-MM-DD or datetime)
        train_period: Training window duration (e.g., "12M")
        test_period: Testing window duration (e.g., "3M")
        step: Step between splits (e.g., "3M")
        embargo: Gap between train and test to prevent leakage (e.g., "5d")

    Returns:
        List of TrainTestSplit objects

    Example:
        splits = walk_forward_splits(
            start="2020-01-01",
            end="2025-01-01",
            train_period="12M",
            test_period="3M",
            step="3M",
            embargo="5d"
        )
    """
    if isinstance(start, str):
        start = datetime.fromisoformat(start)
    if isinstance(end, str):
        end = datetime.fromisoformat(end)

    train_delta = parse_duration(train_period)
    test_delta = parse_duration(test_period)
    step_delta = parse_duration(step)
    embargo_delta = parse_duration(embargo)
    embargo_days = embargo_delta.days

    splits: list[TrainTestSplit] = []
    current_start = start

    while True:
        train_end = current_start + train_delta

        test_start = train_end + embargo_delta
        test_end = test_start + test_delta

        if test_end > end:
            break

        splits.append(
            TrainTestSplit(
                train=DateRange(start=current_start, end=train_end),
                test=DateRange(start=test_start, end=test_end),
                embargo_days=embargo_days,
            )
        )

        current_start += step_delta

    logger.info(
        "Generated walk-forward splits",
        num_splits=len(splits),
        train_period=train_period,
        test_period=test_period,
        embargo=embargo,
    )

    return splits


def expanding_window_splits(
    start: str | datetime,
    end: str | datetime,
    min_train_period: str,
    test_period: str,
    embargo: str = "0d",
) -> list[TrainTestSplit]:
    """Generate expanding window splits (PRD §30.3).

    Training window grows with each split while test window stays fixed.

    Args:
        start: Start date (YYYY-MM-DD or datetime)
        end: End date (YYYY-MM-DD or datetime)
        min_train_period: Minimum training window (e.g., "12M")
        test_period: Test window duration (e.g., "3M")
        embargo: Gap between train and test (e.g., "5d")

    Returns:
        List of TrainTestSplit objects
    """
    if isinstance(start, str):
        start = datetime.fromisoformat(start)
    if isinstance(end, str):
        end = datetime.fromisoformat(end)

    min_train_delta = parse_duration(min_train_period)
    test_delta = parse_duration(test_period)
    embargo_delta = parse_duration(embargo)
    embargo_days = embargo_delta.days

    splits: list[TrainTestSplit] = []

    train_end = start + min_train_delta

    while True:
        test_start = train_end + embargo_delta
        test_end = test_start + test_delta

        if test_end > end:
            break

        splits.append(
            TrainTestSplit(
                train=DateRange(start=start, end=train_end),
                test=DateRange(start=test_start, end=test_end),
                embargo_days=embargo_days,
            )
        )

        train_end = test_end

    logger.info(
        "Generated expanding window splits",
        num_splits=len(splits),
        min_train_period=min_train_period,
        test_period=test_period,
    )

    return splits


def purge_window(
    _train_end: datetime,  # Reserved for future purge calculations
    forward_window: str,
) -> timedelta:
    """Calculate purge window for label leakage prevention.

    When labels have forward-looking windows, we need to purge
    observations near the train/test boundary.

    Args:
        _train_end: End of training period (reserved for future use)
        forward_window: Label's forward window (e.g., "5d")

    Returns:
        Purge duration (typically equals forward_window)
    """
    return parse_duration(forward_window)


def check_holdout_access(
    date_range: DateRange,
    holdout: HoldoutSet,
    allow_final_eval: bool = False,
) -> None:
    """Warn if accessing holdout data outside final evaluation.

    Args:
        date_range: Range being accessed
        holdout: Holdout set definition
        allow_final_eval: If True, suppress warning

    Raises:
        UserWarning if accessing holdout outside final eval
    """
    if holdout.overlaps(date_range) and not allow_final_eval:
        import warnings

        warnings.warn(
            f"Accessing holdout period data ({holdout.start} to {holdout.end}). "
            "Are you sure this is for final evaluation?",
            UserWarning,
            stacklevel=2,
        )
