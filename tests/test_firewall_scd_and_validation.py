from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import pytest

from heber.firewall.scd import join_with_reference_asof
from heber.firewall.validation import GoldBuildMetadata, LeakageError, validate_gold_build


def test_join_with_reference_asof_handles_unsuffixed_validity_columns() -> None:
    left = pl.DataFrame(
        {
            "instrument_key": ["equity:AAPL", "equity:AAPL"],
            "ts_event": [
                datetime(2025, 1, 5, 12, tzinfo=UTC),
                datetime(2025, 1, 15, 12, tzinfo=UTC),
            ],
        }
    )
    reference = pl.DataFrame(
        {
            "instrument_key": ["equity:AAPL", "equity:AAPL"],
            "valid_from": [
                datetime(2025, 1, 1, 0, tzinfo=UTC),
                datetime(2025, 1, 10, 0, tzinfo=UTC),
            ],
            "valid_to": [
                datetime(2025, 1, 10, 0, tzinfo=UTC),
                None,
            ],
            "tick_size": [0.01, 0.05],
        }
    )

    result = join_with_reference_asof(
        left=left,
        reference=reference,
        left_key="instrument_key",
        ref_key="instrument_key",
    ).collect()

    assert len(result) == 2
    assert result["tick_size"].to_list() == [0.01, 0.05]


def test_join_with_reference_asof_handles_suffixed_validity_columns() -> None:
    left = pl.DataFrame(
        {
            "instrument_key": ["equity:AAPL", "equity:AAPL"],
            "ts_event": [
                datetime(2025, 1, 5, 12, tzinfo=UTC),
                datetime(2025, 1, 15, 12, tzinfo=UTC),
            ],
            "valid_from": [
                datetime(2024, 1, 1, 0, tzinfo=UTC),
                datetime(2024, 1, 1, 0, tzinfo=UTC),
            ],
            "valid_to": [None, None],
        }
    )
    reference = pl.DataFrame(
        {
            "instrument_key": ["equity:AAPL", "equity:AAPL"],
            "valid_from": [
                datetime(2025, 1, 1, 0, tzinfo=UTC),
                datetime(2025, 1, 10, 0, tzinfo=UTC),
            ],
            "valid_to": [
                datetime(2025, 1, 10, 0, tzinfo=UTC),
                None,
            ],
            "tick_size": [0.01, 0.05],
        }
    )

    result = join_with_reference_asof(
        left=left,
        reference=reference,
        left_key="instrument_key",
        ref_key="instrument_key",
    ).collect()

    assert len(result) == 2
    assert result["tick_size"].to_list() == [0.01, 0.05]


def _base_metadata() -> GoldBuildMetadata:
    return GoldBuildMetadata(
        feature_time=datetime(2025, 1, 10, 16, tzinfo=UTC),
        max_ts_event_used=datetime(2025, 1, 10, 15, tzinfo=UTC),
        max_ts_available_used=datetime(2025, 1, 10, 15, tzinfo=UTC),
        dataset_version="v1.0.0",
        code_version="abc123",
        input_datasets=["silver:bars:v1"],
    )


def test_validate_gold_build_strict_does_not_raise_on_warning_only() -> None:
    metadata = _base_metadata()
    metadata.max_ts_event_used = datetime(2025, 1, 10, 15, 30, tzinfo=UTC)
    metadata.max_ts_available_used = datetime(2025, 1, 10, 15, 0, tzinfo=UTC)

    warnings = validate_gold_build(metadata, strict=True)

    assert len(warnings) == 1
    assert warnings[0].startswith("WARNING:")


def test_validate_gold_build_strict_raises_on_hard_violation() -> None:
    metadata = _base_metadata()
    metadata.max_ts_available_used = datetime(2025, 1, 10, 16, 5, tzinfo=UTC)

    with pytest.raises(LeakageError, match="LEAKAGE:"):
        validate_gold_build(metadata, strict=True)
