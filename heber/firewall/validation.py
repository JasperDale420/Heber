"""Validation utilities for Zero-Leakage enforcement per PRD §10.9-10.12.

Provides:
- LeakageError exception for leakage violations
- Validation functions for as-of reads
- Gold build gates that fail loudly on violations
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class LeakageError(Exception):
    """Raised when a potential data leakage is detected.

    This is a HARD FAILURE that must be fixed before proceeding.
    """

    pass


@dataclass
class GoldBuildMetadata:
    """Metadata required for every Gold dataset build (PRD §10.9).

    Every Gold build must record this information for audit trail.
    """

    feature_time: datetime
    max_ts_event_used: datetime
    max_ts_available_used: datetime
    dataset_version: str
    code_version: str  # git SHA
    input_datasets: list[str]  # names + schema versions

    def to_dict(self) -> dict[str, Any]:
        return {
            "feature_time": self.feature_time.isoformat(),
            "max_ts_event_used": self.max_ts_event_used.isoformat(),
            "max_ts_available_used": self.max_ts_available_used.isoformat(),
            "dataset_version": self.dataset_version,
            "code_version": self.code_version,
            "input_datasets": self.input_datasets,
        }


def validate_asof_read(
    df_has_ts_available: bool,
    asof_time: datetime | None,
    context: str = "training",
) -> None:
    """Validate that an as-of read is being performed correctly (PRD §10.11).

    In training/backtest context, reads without asof_time should fail.

    Args:
        df_has_ts_available: Whether the dataframe has ts_available column
        asof_time: The as-of time provided (None if not provided)
        context: "training", "backtest", "research", or "production"

    Raises:
        LeakageError: If read violates anti-leakage rules
    """
    # In training/backtest/research context, asof_time is required
    training_contexts = {"training", "backtest", "research"}

    if context in training_contexts:
        if asof_time is None:
            raise LeakageError(
                f"As-of time is required for {context} reads. "
                "Provide asof_time parameter to ensure point-in-time correctness."
            )

        if not df_has_ts_available:
            raise LeakageError("Dataset is missing ts_available column. Cannot perform point-in-time correct read.")

    logger.debug("validate_asof_read", context=context, has_asof=asof_time is not None)


def validate_gold_build(
    metadata: GoldBuildMetadata,
    strict: bool = True,
) -> list[str]:
    """Validate Gold dataset build gates (PRD §10.9).

    These are HARD GATES that should fail the build.

    Args:
        metadata: Build metadata to validate
        strict: If True, raise LeakageError on violations

    Returns:
        List of warning messages (empty if valid)

    Raises:
        LeakageError: If strict=True and any gate fails
    """
    violations = []

    # Gate 1: max_ts_available_used must not exceed feature_time
    if metadata.max_ts_available_used > metadata.feature_time:
        msg = (
            f"LEAKAGE: max_ts_available_used ({metadata.max_ts_available_used}) "
            f"> feature_time ({metadata.feature_time}). "
            "This means future data is being used."
        )
        violations.append(msg)
        logger.error("gold_build_leakage", violation="future_data", **metadata.to_dict())

    # Gate 2: max_ts_event_used should typically not exceed max_ts_available
    # (This is a warning, not a hard failure)
    if metadata.max_ts_event_used > metadata.max_ts_available_used:
        msg = (
            f"WARNING: max_ts_event_used ({metadata.max_ts_event_used}) "
            f"> max_ts_available_used ({metadata.max_ts_available_used}). "
            "Check timestamp semantics."
        )
        violations.append(msg)
        logger.warning("gold_build_warning", **metadata.to_dict())

    # Log successful validation
    if not violations:
        logger.info("gold_build_validated", **metadata.to_dict())

    # Raise on violations in strict mode
    if strict and violations:
        raise LeakageError("\n".join(violations))

    return violations


def validate_train_test_split(
    train_end: datetime,
    test_start: datetime,
    purge_window: int,
    embargo_window: int,
) -> None:
    """Validate train/test split configuration (PRD §10.10).

    Args:
        train_end: End of training period
        test_start: Start of test period
        purge_window: Purge period in seconds (remove overlapping windows)
        embargo_window: Embargo period in seconds (hold out after split)

    Raises:
        LeakageError: If split configuration is invalid
    """
    gap_seconds = (test_start - train_end).total_seconds()
    required_gap = purge_window + embargo_window

    if gap_seconds < required_gap:
        raise LeakageError(
            f"Train/test split too close. "
            f"Gap: {gap_seconds}s, Required: {required_gap}s "
            f"(purge: {purge_window}s + embargo: {embargo_window}s)"
        )

    logger.debug(
        "validate_train_test_split",
        train_end=train_end.isoformat(),
        test_start=test_start.isoformat(),
        gap_seconds=gap_seconds,
    )
