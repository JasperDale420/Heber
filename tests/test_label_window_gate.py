"""The meta-label training build must not silently absorb truncated-window labels.

The join only carried a fixed list of outcome columns, so a ``quality_flags``
marker written onto the Gold label rows never reached the joined frame — the
existing flag filter runs against the *features* flags. The window check is
therefore evaluated on the outcomes frame and joined as its own column.
"""

from __future__ import annotations

import pandas as pd
import pytest

from heber.ml.datasets import (
    LABEL_WINDOW_TRUNCATED_COLUMN,
    QUALITY_FLAG_LABEL_WINDOW_TRUNCATED,
    DatasetConfig,
    MetaLabelDatasetBuilder,
)

pytestmark = pytest.mark.unit


@pytest.fixture
def builder() -> MetaLabelDatasetBuilder:
    return MetaLabelDatasetBuilder(config=DatasetConfig())


def _features() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "alert_id": ["truncated", "intact"],
            "premium": [1000.0, 2000.0],
            "quality_flags": [[], []],
        }
    )


def _outcomes() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "alert_id": ["truncated", "intact"],
            "outcome": ["expired", "hit_tp"],
            "hit_tp_first": [0, 1],
            "horizon": ["swing", "swing"],
            "window_duration_hours": [3.0087, 448.0],
        }
    )


def test_join_carries_the_window_check_onto_training_rows(builder: MetaLabelDatasetBuilder) -> None:
    joined = builder._join_features_outcomes(_features(), _outcomes())
    assert LABEL_WINDOW_TRUNCATED_COLUMN in joined.columns
    by_alert = joined.set_index("alert_id")[LABEL_WINDOW_TRUNCATED_COLUMN]
    assert bool(by_alert["truncated"]) is True
    assert bool(by_alert["intact"]) is False


def test_truncated_rows_are_excluded_by_default(builder: MetaLabelDatasetBuilder) -> None:
    joined = builder._join_features_outcomes(_features(), _outcomes())
    filtered = builder._apply_filters(joined)
    assert list(filtered["alert_id"]) == ["intact"]


def test_truncated_rows_can_be_opted_into() -> None:
    builder = MetaLabelDatasetBuilder(
        config=DatasetConfig(include_truncated_label_windows=True, min_outcomes_per_symbol=1)
    )
    joined = builder._join_features_outcomes(_features(), _outcomes())
    filtered = builder._apply_filters(joined)
    assert sorted(filtered["alert_id"]) == ["intact", "truncated"]


@pytest.mark.parametrize("missing", ["horizon", "window_duration_hours"])
def test_outcomes_that_cannot_be_checked_are_rejected(builder: MetaLabelDatasetBuilder, missing: str) -> None:
    """Silently training on unverifiable labels is the failure this guards against."""
    outcomes = _outcomes().drop(columns=[missing])
    with pytest.raises(ValueError, match=missing):
        builder._join_features_outcomes(_features(), outcomes)


def test_persisted_flag_is_honoured_when_the_recomputed_check_passes() -> None:
    """A row flagged by the migration stays excluded even if its columns look sound."""
    builder = MetaLabelDatasetBuilder(config=DatasetConfig(min_outcomes_per_symbol=1))
    outcomes = _outcomes()
    outcomes["quality_flags"] = [[QUALITY_FLAG_LABEL_WINDOW_TRUNCATED], []]
    outcomes["window_duration_hours"] = [448.0, 448.0]

    filtered = builder._apply_filters(builder._join_features_outcomes(_features(), outcomes))

    assert list(filtered["alert_id"]) == ["intact"]


def test_a_features_side_column_of_the_same_name_cannot_shadow_the_check() -> None:
    builder = MetaLabelDatasetBuilder(config=DatasetConfig(min_outcomes_per_symbol=1))
    features = _features()
    features[LABEL_WINDOW_TRUNCATED_COLUMN] = [False, False]

    joined = builder._join_features_outcomes(features, _outcomes())
    filtered = builder._apply_filters(joined)

    assert f"{LABEL_WINDOW_TRUNCATED_COLUMN}_x" not in joined.columns
    assert list(filtered["alert_id"]) == ["intact"]


def test_empty_outcomes_frame_is_passed_through(builder: MetaLabelDatasetBuilder) -> None:
    assert builder._join_features_outcomes(_features(), _outcomes().iloc[0:0]).empty


def test_window_check_is_not_offered_to_the_model(builder: MetaLabelDatasetBuilder) -> None:
    joined = builder._join_features_outcomes(_features(), _outcomes())
    assert LABEL_WINDOW_TRUNCATED_COLUMN not in builder.get_feature_columns(joined)
