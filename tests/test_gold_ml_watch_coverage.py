"""Coverage improvement tests for heber.gold, heber.ml, and heber.watch modules.

Targets missing lines identified by coverage analysis:
- gold/labels.py: lines 74, 155->159, 198, 263-264, 267, 271-272, 281-282, 290-292, 301
- ml/datasets.py: lines 56-57, 132-159, 173-187, 192-215, 233-243, 255-260, 287-330, 349-425, 443-507
- ml/trainer.py: lines 92-147, 165-188, 196-234, 246, 265-275, 317-340, 360-385
- ml/inference.py: lines 69-83, 95-96, 101-103, 113, 123, 134-141, 155-165, 179-184, etc.
- watch/checker.py: lines 54-62, 108, 177, 208, 238-239
- watch/features.py: various enrichment paths, edge cases
- watch/consumer.py: _classify_alert_results, _normalize_process_result, etc.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from heber.gold.labels import (
    LabelMetadata,
    compute_availability_time,
    read_label,
    write_label,
)
from heber.ml.datasets import (
    DatasetConfig,
    MetaLabelDatasetBuilder,
    _atomic_write_parquet,
    _read_existing_partition_or_quarantine,
    persist_features_to_gold,
)
from heber.ml.inference import AlertGate, MetaLabelScorer
from heber.reader.core import HeberReadError
from heber.watch.checker import BarrierChecker, outcome_to_label_row
from heber.watch.consumer import AlertWatchConsumer
from heber.watch.features import (
    AlertFeatureExtractor,
    AlertFeatures,
    _classify_direction,
    _coalesce_first,
    _coalesce_split_or_fallback,
    _unwrap_data_payload,
    get_features,
    store_features,
)
from heber.watch.models import (
    AlertWatch,
    WatchHorizon,
    WatchOutcome,
    WatchSnapshot,
    WatchStatus,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_watch(
    watch_id: str = "w1",
    alert_id: str = "a1",
    entry_price: float = 5.0,
    tp: float = 0.25,
    sl: float = 0.15,
    alert_time: datetime | None = None,
    window_end: datetime | None = None,
) -> AlertWatch:
    if alert_time is None:
        alert_time = datetime(2025, 6, 10, 10, 0, 0, tzinfo=UTC)
    if window_end is None:
        window_end = alert_time + timedelta(hours=4)
    return AlertWatch(
        watch_id=watch_id,
        alert_id=alert_id,
        occ_symbol="AAPL250620C00200000",
        underlying="AAPL",
        put_call="C",
        expiry="2025-06-20",
        strike=200.0,
        entry_price=entry_price,
        spot_at_alert=198.0,
        alert_time=alert_time,
        window_end=window_end,
        horizon=WatchHorizon.INTRADAY,
        tp_threshold=tp,
        sl_threshold=sl,
        status=WatchStatus.WATCHING,
    )


def _make_snapshot(
    watch_id: str = "w1",
    mid_px: float | None = None,
    return_pct: float | None = None,
    timestamp: datetime | None = None,
) -> WatchSnapshot:
    if timestamp is None:
        timestamp = datetime(2025, 6, 10, 10, 30, 0, tzinfo=UTC)
    return WatchSnapshot(
        watch_id=watch_id,
        occ_symbol="AAPL250620C00200000",
        timestamp=timestamp,
        mid_px=mid_px,
        return_pct=return_pct,
    )


def _make_outcome(status: WatchStatus = WatchStatus.HIT_TP) -> WatchOutcome:
    now = datetime(2025, 6, 10, 12, 0, 0, tzinfo=UTC)
    return WatchOutcome(
        watch_id="w1",
        alert_id="a1",
        occ_symbol="AAPL250620C00200000",
        underlying="AAPL",
        put_call="C",
        horizon="intraday",
        status=status,
        outcome_time=now,
        outcome_return=0.30,
        bars_to_hit=5,
        mfe=0.35,
        mae=-0.08,
        mfe_adj=0.31,
        mae_adj=-0.10,
        hit_tp_first=1 if status == WatchStatus.HIT_TP else 0,
        entry_price=5.0,
        spot_at_alert=198.0,
        alert_time=now - timedelta(hours=2),
        window_duration_hours=4.0,
        trading_minutes_to_hit=60,
    )


def _make_flow_alert_record():
    """Create a mock FlowAlertRecord for testing."""
    record = MagicMock()
    record.event_id = "alert-123"
    record.ts_event = datetime(2025, 6, 10, 10, 0, 0, tzinfo=UTC)
    record.underlying = "AAPL"
    record.occ_symbol = "AAPL250620C00200000"
    record.strike = 200.0
    record.expiry = date(2025, 6, 20)
    record.put_call = "C"
    record.premium = 50000.0
    record.volume = 500.0
    record.open_interest = 1000.0
    record.spot_px = 198.0
    record.contract_px = 5.0
    record.alert_type = "SWEEP"
    record.side = "ask"
    record.aggressor = "buyer"
    record.tags = ["bullish", "unusual"]
    return record


# ===========================================================================
# GOLD / LABELS
# ===========================================================================


class TestLabelMetadataAvailabilityLag:
    """Cover LabelMetadata.get_availability_lag_delta (line 74)."""

    def test_get_availability_lag_delta(self):
        meta = LabelMetadata(availability_lag="30m")
        assert meta.get_availability_lag_delta() == timedelta(minutes=30)

    def test_get_availability_lag_delta_zero(self):
        meta = LabelMetadata(availability_lag="0s")
        assert meta.get_availability_lag_delta() == timedelta(seconds=0)


class TestComputeAvailabilityTimeHourly:
    """Cover the hourly forward_window path (line 155->159)."""

    def test_hourly_forward_window_no_market_close_align(self):
        """With 'h' forward_window, availability should NOT snap to market close."""
        label_time = datetime(2025, 1, 10, 9, 30, tzinfo=UTC)
        result = compute_availability_time(label_time, "4h")
        # 4h forward from 9:30 -> 13:30, no market close alignment
        assert result.hour == 13
        assert result.minute == 30

    def test_minute_forward_window(self):
        label_time = datetime(2025, 1, 10, 9, 30, tzinfo=UTC)
        result = compute_availability_time(label_time, "30m")
        assert result.hour == 10
        assert result.minute == 0


class TestWriteLabelMissingColumns:
    """Cover missing required columns error path (line 198)."""

    def test_write_label_missing_columns_raises(self, tmp_path: Path):
        df = pd.DataFrame({"some_col": [1, 2]})
        with pytest.raises(ValueError, match="Missing required columns"):
            write_label(
                gold_root=tmp_path,
                dataset="test",
                df=df,
                forward_window="5d",
            )


class TestReadLabelEdgeCases:
    """Cover read_label edge cases: missing dataset, no versions, missing file, read error."""

    def test_read_label_nonexistent_dataset(self, tmp_path: Path):
        """Line 263-264: dataset path doesn't exist."""
        result = read_label(tmp_path, "nonexistent", datetime(2025, 1, 15, tzinfo=UTC))
        assert result.empty

    def test_read_label_no_versions(self, tmp_path: Path):
        """Line 271-272: dataset path exists but no version subdirs."""
        dataset_path = tmp_path / "dataset=test" / "type=label"
        dataset_path.mkdir(parents=True)
        result = read_label(tmp_path, "test", datetime(2025, 1, 15, tzinfo=UTC))
        assert result.empty

    def test_read_label_missing_data_file(self, tmp_path: Path):
        """Line 281-282: version dir exists but no data.parquet."""
        version_dir = tmp_path / "dataset=test" / "type=label" / "version=v1.0.0"
        version_dir.mkdir(parents=True)
        result = read_label(tmp_path, "test", datetime(2025, 1, 15, tzinfo=UTC))
        assert result.empty

    def test_read_label_specific_version_missing_data(self, tmp_path: Path):
        """Line 267: explicit version specified but data file missing."""
        version_dir = tmp_path / "dataset=test" / "type=label" / "version=v2.0.0"
        version_dir.mkdir(parents=True)
        result = read_label(tmp_path, "test", datetime(2025, 1, 15, tzinfo=UTC), version="v2.0.0")
        assert result.empty

    def test_read_label_parquet_read_error(self, tmp_path: Path):
        """An unparseable label file raises rather than reading back as zero labels.

        The three cases above — no dataset, no version, no data file — are
        genuine absences and still answer empty. Bytes that exist but cannot be
        parsed are an unknown label count, and returning empty for that fed
        silently incomplete labels into training with nothing to distinguish it
        from a day that produced none.
        """
        version_dir = tmp_path / "dataset=test" / "type=label" / "version=v1.0.0"
        version_dir.mkdir(parents=True)
        bad_file = version_dir / "data.parquet"
        bad_file.write_text("not a parquet file")
        with pytest.raises(HeberReadError):
            read_label(tmp_path, "test", datetime(2025, 1, 15, tzinfo=UTC))

    def test_read_label_missing_ts_available_warn_only(self, tmp_path: Path):
        """Line 301: warn but continue when fail_on_missing_ts_available=False."""
        version_dir = tmp_path / "dataset=test" / "type=label" / "version=v1.0.0"
        version_dir.mkdir(parents=True)
        df = pd.DataFrame(
            {
                "instrument_key": ["AAPL"],
                "ts_label": [datetime(2025, 1, 5, 9, 30, tzinfo=UTC)],
                "label": [0.02],
            }
        )
        df.to_parquet(version_dir / "data.parquet")
        result = read_label(
            tmp_path,
            "test",
            datetime(2025, 1, 20, tzinfo=UTC),
            fail_on_missing_ts_available=False,
        )
        # Should return data (or empty depending on reader), not raise
        assert isinstance(result, pd.DataFrame)


# ===========================================================================
# ML / DATASETS
# ===========================================================================


class TestAtomicWriteParquet:
    """Cover _atomic_write_parquet and _read_existing_partition_or_quarantine."""

    def test_atomic_write_creates_file(self, tmp_path: Path):
        df = pd.DataFrame({"a": [1, 2, 3]})
        out = tmp_path / "test.parquet"
        _atomic_write_parquet(df, out)
        assert out.exists()
        result = pd.read_parquet(out)
        assert len(result) == 3

    def test_read_existing_partition_none_when_missing(self, tmp_path: Path):
        result = _read_existing_partition_or_quarantine(tmp_path / "nope.parquet")
        assert result is None

    def test_read_existing_partition_returns_data(self, tmp_path: Path):
        out = tmp_path / "data.parquet"
        pd.DataFrame({"x": [10]}).to_parquet(out)
        result = _read_existing_partition_or_quarantine(out)
        assert result is not None
        assert len(result) == 1

    def test_read_existing_partition_quarantines_corrupt(self, tmp_path: Path):
        """Lines 56-57: corrupt file quarantined."""
        out = tmp_path / "data.parquet"
        out.write_text("corrupt data")
        result = _read_existing_partition_or_quarantine(out)
        assert result is None
        # Original file should be gone, quarantine file present
        quarantine_files = list(tmp_path.glob("data.parquet.corrupt-*"))
        assert len(quarantine_files) == 1


class TestMetaLabelDatasetBuilderBuildFromParquet:
    """Cover build_from_parquet paths (lines 132-159)."""

    def test_build_from_parquet_empty_outcomes(self, tmp_path: Path):
        """Lines 133-135: no outcomes in range."""
        config = DatasetConfig(
            outcomes_path=tmp_path / "outcomes",
            features_path=tmp_path / "features",
        )
        builder = MetaLabelDatasetBuilder(config=config)
        result = builder.build_from_parquet(date(2025, 1, 1), date(2025, 1, 31))
        assert result.empty

    def test_build_from_parquet_empty_features(self, tmp_path: Path):
        """Lines 138-141: outcomes exist but features empty."""
        # Set up outcomes
        outcomes_path = tmp_path / "outcomes" / "dt=2025-01-15"
        outcomes_path.mkdir(parents=True)
        pd.DataFrame(
            {
                "alert_id": ["a1"],
                "outcome": ["hit_tp"],
                "outcome_return": [0.25],
            }
        ).to_parquet(outcomes_path / "data.parquet")

        config = DatasetConfig(
            outcomes_path=tmp_path / "outcomes",
            features_path=tmp_path / "features",
        )
        builder = MetaLabelDatasetBuilder(config=config)
        result = builder.build_from_parquet(date(2025, 1, 1), date(2025, 1, 31))
        assert result.empty

    def test_build_from_parquet_full_pipeline(self, tmp_path: Path):
        """Lines 143-159: full join, add_meta_label, apply_filters.

        We bypass the HeberReader by directly testing _join, _add_meta_label, _apply_filters
        since _load_outcomes / _load_features depend on HeberReader internals.
        """
        builder = MetaLabelDatasetBuilder(config=DatasetConfig(min_outcomes_per_symbol=1))

        features = pd.DataFrame(
            {
                "alert_id": ["a1", "a2", "a3"],
                "symbol": ["AAPL", "AAPL", "MSFT"],
                "feature_x": [1.0, 2.0, 3.0],
            }
        )

        outcomes = pd.DataFrame(
            {
                "alert_id": ["a1", "a2", "a3"],
                "outcome": ["hit_tp", "hit_sl", "expired"],
                "outcome_return": [0.25, -0.15, 0.0],
                "mfe": [0.30, 0.05, 0.02],
                "mae": [-0.05, -0.15, -0.08],
                "bars_to_hit": [3, 5, None],
                "trading_minutes_to_hit": [30, 60, None],
                "hit_tp_first": [1, 0, 0],
            }
        )

        joined = builder._join_features_outcomes(features, outcomes)
        labeled = builder._add_meta_label(joined)
        filtered = builder._apply_filters(labeled)
        assert not filtered.empty
        assert "meta_label" in filtered.columns
        assert filtered["meta_label"].iloc[0] == 1  # hit_tp -> 1


class TestBuildFromRedis:
    """Cover build_from_redis (lines 173-187)."""

    @pytest.mark.asyncio
    async def test_build_from_redis_no_redis_raises(self):
        """Line 173-174: raises ValueError without Redis."""
        builder = MetaLabelDatasetBuilder()
        with pytest.raises(ValueError, match="Redis client required"):
            await builder.build_from_redis(["a1"])

    @pytest.mark.asyncio
    async def test_build_from_redis_no_features_found(self):
        """Lines 184-185: returns empty when no features found."""
        mock_redis = AsyncMock()
        builder = MetaLabelDatasetBuilder(redis=mock_redis)
        with patch("heber.watch.features.get_features", new_callable=AsyncMock, return_value=None):
            result = await builder.build_from_redis(["a1"])
        assert result.empty


class TestLoadOutcomesAndFeatures:
    """Cover _load_outcomes and _load_features error paths (lines 192-243)."""

    def test_load_outcomes_absent_path_is_empty(self, tmp_path: Path):
        """A path nobody has written is a genuine absence and reads as empty."""
        config = DatasetConfig(
            outcomes_path=tmp_path / "nonexistent",
            features_path=tmp_path / "features",
        )
        builder = MetaLabelDatasetBuilder(config=config)
        result = builder._load_outcomes(date(2025, 1, 1), date(2025, 1, 31))
        assert result.empty

    def test_load_features_absent_path_is_empty(self, tmp_path: Path):
        """A path nobody has written is a genuine absence and reads as empty."""
        config = DatasetConfig(
            features_path=tmp_path / "nonexistent",
            outcomes_path=tmp_path / "outcomes",
        )
        builder = MetaLabelDatasetBuilder(config=config)
        result = builder._load_features(date(2025, 1, 1), date(2025, 1, 31))
        assert result.empty

    def test_load_outcomes_unreadable_raises(self, tmp_path: Path):
        """A corrupt outcomes file must not build an empty training set.

        Both loaders used to catch every exception and return an empty frame,
        so ``build_from_parquet`` logged "No outcomes found in date range" and
        stopped — the same message it logs for a date range that genuinely holds
        none. Training data going missing must not look like a quiet no-op.
        """
        outcomes = tmp_path / "outcomes"
        outcomes.mkdir()
        (outcomes / "corrupt.parquet").write_bytes(b"not parquet")
        config = DatasetConfig(outcomes_path=outcomes, features_path=tmp_path / "features")
        builder = MetaLabelDatasetBuilder(config=config)
        with pytest.raises(HeberReadError):
            builder._load_outcomes(date(2025, 1, 1), date(2025, 1, 31))

    def test_load_features_unreadable_raises(self, tmp_path: Path):
        """A corrupt features file must not build an empty training set."""
        features = tmp_path / "features"
        features.mkdir()
        (features / "corrupt.parquet").write_bytes(b"not parquet")
        config = DatasetConfig(features_path=features, outcomes_path=tmp_path / "outcomes")
        builder = MetaLabelDatasetBuilder(config=config)
        with pytest.raises(HeberReadError):
            builder._load_features(date(2025, 1, 1), date(2025, 1, 31))


class TestJoinFeaturesOutcomes:
    """Cover _join_features_outcomes edge cases (lines 255-260)."""

    def test_features_missing_alert_id(self):
        """Line 255-256: features lacks alert_id."""
        builder = MetaLabelDatasetBuilder()
        features = pd.DataFrame({"x": [1.0]})
        outcomes = pd.DataFrame({"alert_id": ["a1"], "outcome": ["hit_tp"]})
        result = builder._join_features_outcomes(features, outcomes)
        assert result.empty

    def test_outcomes_missing_alert_id(self):
        """Line 258-260: outcomes lacks alert_id."""
        builder = MetaLabelDatasetBuilder()
        features = pd.DataFrame({"alert_id": ["a1"], "x": [1.0]})
        outcomes = pd.DataFrame({"outcome": ["hit_tp"]})
        result = builder._join_features_outcomes(features, outcomes)
        assert result.empty


class TestNormalizeOutcomes:
    """Cover _normalize_outcomes backward compat (lines 287-302)."""

    def test_all_legacy_columns(self):
        builder = MetaLabelDatasetBuilder()
        df = pd.DataFrame(
            {
                "alert_id": ["a1"],
                "outcome_reason": ["hit_tp"],
                "contract_hit_tp_first": [1],
                "contract_mfe": [0.30],
                "contract_mae": [-0.10],
                "contract_bars_to_hit": [3],
            }
        )
        result = builder._normalize_outcomes(df)
        assert "outcome" in result.columns
        assert "hit_tp_first" in result.columns
        assert "mfe" in result.columns
        assert "mae" in result.columns
        assert "bars_to_hit" in result.columns


class TestAddMetaLabel:
    """Cover _add_meta_label (lines 304-313)."""

    def test_no_outcome_column(self):
        builder = MetaLabelDatasetBuilder()
        df = pd.DataFrame({"x": [1.0, 2.0]})
        result = builder._add_meta_label(df)
        assert "meta_label" not in result.columns

    def test_meta_label_values(self):
        builder = MetaLabelDatasetBuilder()
        df = pd.DataFrame({"outcome": ["hit_tp", "hit_sl", "expired"]})
        result = builder._add_meta_label(df)
        assert list(result["meta_label"]) == [1, 0, 0]


class TestApplyFilters:
    """Cover _apply_filters (lines 315-330)."""

    def test_empty_dataframe(self):
        builder = MetaLabelDatasetBuilder()
        result = builder._apply_filters(pd.DataFrame())
        assert result.empty

    def test_exclude_expired(self):
        config = DatasetConfig(exclude_expired=True, min_outcomes_per_symbol=1)
        builder = MetaLabelDatasetBuilder(config=config)
        df = pd.DataFrame(
            {
                "outcome": ["hit_tp", "expired", "hit_sl"],
                "symbol": ["AAPL", "AAPL", "AAPL"],
            }
        )
        result = builder._apply_filters(df)
        assert len(result) == 2
        assert "expired" not in result["outcome"].values

    def test_min_outcomes_per_symbol(self):
        config = DatasetConfig(min_outcomes_per_symbol=3)
        builder = MetaLabelDatasetBuilder(config=config)
        df = pd.DataFrame(
            {
                "symbol": ["AAPL"] * 5 + ["MSFT"] * 2,
                "outcome": ["hit_tp"] * 7,
            }
        )
        result = builder._apply_filters(df)
        assert set(result["symbol"].unique()) == {"AAPL"}


class TestTrainTestSplit:
    """Cover train_test_split (lines 349-387)."""

    def test_empty_df(self):
        builder = MetaLabelDatasetBuilder()
        train, test = builder.train_test_split(pd.DataFrame())
        assert train.empty
        assert test.empty

    def test_missing_alert_time(self):
        builder = MetaLabelDatasetBuilder()
        df = pd.DataFrame({"x": [1.0]})
        train, test = builder.train_test_split(df)
        # Train = entire df, test = empty
        assert len(train) == 1
        assert test.empty

    def test_split_with_purge_embargo(self):
        builder = MetaLabelDatasetBuilder(config=DatasetConfig(purge_days=2, embargo_days=1, train_ratio=0.5))
        base = datetime(2025, 1, 1)
        df = pd.DataFrame(
            {
                "alert_time": [base + timedelta(days=i) for i in range(20)],
                "x": range(20),
            }
        )
        train, test = builder.train_test_split(df)
        # Both should have data, with purge gap between them
        assert len(train) >= 1
        assert len(test) >= 1

    def test_split_with_explicit_date(self):
        builder = MetaLabelDatasetBuilder(config=DatasetConfig(purge_days=1, embargo_days=1))
        base = datetime(2025, 1, 1)
        df = pd.DataFrame(
            {
                "alert_time": [base + timedelta(days=i) for i in range(30)],
                "x": range(30),
            }
        )
        train, test = builder.train_test_split(df, split_date=date(2025, 1, 15))
        assert len(train) >= 1
        assert len(test) >= 1


class TestGetFeatureColumnsAndToXY:
    """Cover get_feature_columns and to_xy (lines 398-449)."""

    def test_get_feature_columns_excludes_identifiers(self):
        builder = MetaLabelDatasetBuilder()
        df = pd.DataFrame(
            {
                "alert_id": ["a1"],
                "symbol": ["AAPL"],
                "meta_label": [1],
                "feature_x": [1.0],
                "feature_y": [2.0],
            }
        )
        cols = builder.get_feature_columns(df)
        assert "feature_x" in cols
        assert "feature_y" in cols
        assert "alert_id" not in cols
        assert "symbol" not in cols
        assert "meta_label" not in cols

    def test_to_xy(self):
        builder = MetaLabelDatasetBuilder()
        df = pd.DataFrame(
            {
                "feature_x": [1.0, 2.0],
                "feature_y": [3.0, 4.0],
                "meta_label": [1, 0],
            }
        )
        X, y = builder.to_xy(df, feature_cols=["feature_x", "feature_y"])
        assert X.shape == (2, 2)
        assert len(y) == 2

    def test_to_xy_auto_detect(self):
        builder = MetaLabelDatasetBuilder()
        df = pd.DataFrame(
            {
                "feature_x": [1.0, 2.0],
                "meta_label": [1, 0],
            }
        )
        X, y = builder.to_xy(df)
        assert "feature_x" in X.columns


class TestPersistFeaturesToGold:
    """Cover persist_features_to_gold (lines 452-518)."""

    def test_empty_dataframe_noop(self, tmp_path: Path):
        """Line 464-465: empty df returns early."""
        persist_features_to_gold(pd.DataFrame(), tmp_path)
        assert not list(tmp_path.iterdir())

    def test_persist_multiple_dates(self, tmp_path: Path):
        """Lines 472-518: partitioned write."""
        df = pd.DataFrame(
            {
                "alert_id": ["a1", "a2"],
                "alert_time": [
                    datetime(2025, 1, 10, 10, 0, tzinfo=UTC),
                    datetime(2025, 1, 11, 10, 0, tzinfo=UTC),
                ],
                "feature_x": [1.0, 2.0],
            }
        )
        persist_features_to_gold(df, tmp_path)
        assert (tmp_path / "dt=2025-01-10" / "data.parquet").exists()
        assert (tmp_path / "dt=2025-01-11" / "data.parquet").exists()

    def test_one_bad_date_does_not_abort_the_rest_of_the_batch(self, tmp_path: Path, monkeypatch):
        """A durability failure writing one date's partition must not discard
        every other date already queued in the same call — only filelock.Timeout
        used to be isolated per-date; a new OSError from the write's own
        validation (empty/truncated staged file, row-count mismatch) escaped
        the loop entirely and aborted dates that hadn't failed at all."""
        df = pd.DataFrame(
            {
                "alert_id": ["a1", "a2", "a3"],
                "alert_time": [
                    datetime(2025, 1, 10, 10, 0, tzinfo=UTC),
                    datetime(2025, 1, 11, 10, 0, tzinfo=UTC),
                    datetime(2025, 1, 12, 10, 0, tzinfo=UTC),
                ],
                "feature_x": [1.0, 2.0, 3.0],
            }
        )

        real_atomic_write = _atomic_write_parquet

        def failing_on_one_date(partition_df, out_file):
            if "2025-01-11" in str(out_file):
                raise OSError("simulated truncated write")
            real_atomic_write(partition_df, out_file)

        monkeypatch.setattr("heber.ml.datasets._atomic_write_parquet", failing_on_one_date)

        # Every date is still attempted, but the caller must not be told this
        # succeeded — an aggregate failure is raised only after the loop, so a
        # bad date can't silently look identical to a fully successful batch.
        with pytest.raises(OSError, match="2025-01-11"):
            persist_features_to_gold(df, tmp_path)

        assert (tmp_path / "dt=2025-01-10" / "data.parquet").exists()
        assert not (tmp_path / "dt=2025-01-11" / "data.parquet").exists()
        assert (tmp_path / "dt=2025-01-12" / "data.parquet").exists()


# ===========================================================================
# ML / TRAINER
# ===========================================================================


_HAS_LIGHTGBM = pytest.importorskip is not None  # placeholder

try:
    import lightgbm  # noqa: F401

    _HAS_LIGHTGBM = True
except ModuleNotFoundError:
    _HAS_LIGHTGBM = False

_skip_no_lgb = pytest.mark.skipif(not _HAS_LIGHTGBM, reason="LightGBM not installed")


@_skip_no_lgb
class TestMetaModelTrainerTrain:
    """Cover MetaModelTrainer.train (lines 92-147)."""

    def test_train_basic(self):
        """Lines 92-147: basic train flow with validation."""
        from heber.ml.trainer import MetaModelTrainer, TrainingConfig

        config = TrainingConfig(n_estimators=5, max_depth=2)
        trainer = MetaModelTrainer(config=config)

        np.random.seed(42)
        X_train = np.random.rand(50, 3)
        y_train = (np.random.rand(50) > 0.5).astype(int)
        X_val = np.random.rand(10, 3)
        y_val = (np.random.rand(10) > 0.5).astype(int)

        metrics = trainer.train(X_train, y_train, X_val, y_val, feature_names=["f1", "f2", "f3"])
        assert "train_accuracy" in metrics
        assert "val_accuracy" in metrics
        assert trainer.config.feature_names == ["f1", "f2", "f3"]

    def test_train_without_validation(self):
        """Train without validation set."""
        from heber.ml.trainer import MetaModelTrainer, TrainingConfig

        config = TrainingConfig(n_estimators=5, max_depth=2)
        trainer = MetaModelTrainer(config=config)

        X_train = np.random.rand(30, 2)
        y_train = (np.random.rand(30) > 0.5).astype(int)

        metrics = trainer.train(X_train, y_train)
        assert "train_accuracy" in metrics
        assert "val_accuracy" not in metrics

    def test_train_with_pandas(self):
        """Train with pandas DataFrame/Series."""
        from heber.ml.trainer import MetaModelTrainer, TrainingConfig

        config = TrainingConfig(n_estimators=5, max_depth=2)
        trainer = MetaModelTrainer(config=config)

        X_train = pd.DataFrame({"f1": np.random.rand(30), "f2": np.random.rand(30)})
        y_train = pd.Series((np.random.rand(30) > 0.5).astype(int))

        metrics = trainer.train(X_train, y_train)
        assert "train_accuracy" in metrics


class TestMetaModelTrainerNoModel:
    """Cover error paths that do not need LightGBM."""

    def test_predict_proba_no_model_raises(self):
        """Line 246: predict_proba without training."""
        from heber.ml.trainer import MetaModelTrainer

        trainer = MetaModelTrainer()
        with pytest.raises(ValueError, match="Model not trained"):
            trainer.predict_proba(np.array([[1.0, 2.0]]))

    def test_save_no_model_raises(self, tmp_path: Path):
        """Line 275: save without training."""
        from heber.ml.trainer import MetaModelTrainer

        trainer = MetaModelTrainer()
        with pytest.raises(ValueError, match="No model to save"):
            trainer.save(tmp_path / "model")

    def test_get_feature_importance_no_model(self):
        """Line 337-338: get_feature_importance without training."""
        from heber.ml.trainer import MetaModelTrainer

        trainer = MetaModelTrainer()
        with pytest.raises(ValueError, match="Model not trained"):
            trainer.get_feature_importance(["f1", "f2"])


@_skip_no_lgb
class TestMetaModelTrainerWithLightGBM:
    """Cover predict_proba, predict, save, load, get_feature_importance (needs LightGBM)."""

    def _trained_trainer(self):
        from heber.ml.trainer import MetaModelTrainer, TrainingConfig

        config = TrainingConfig(n_estimators=5, max_depth=2)
        trainer = MetaModelTrainer(config=config)
        np.random.seed(42)
        X = np.random.rand(30, 2)
        y = (np.random.rand(30) > 0.5).astype(int)
        trainer.train(X, y, feature_names=["f1", "f2"])
        return trainer

    def test_predict_proba(self):
        trainer = self._trained_trainer()
        proba = trainer.predict_proba(np.array([[0.5, 0.5]]))
        assert 0 <= proba[0] <= 1

    def test_predict_with_threshold(self):
        """Lines 265-266: predict with custom threshold."""
        trainer = self._trained_trainer()
        preds = trainer.predict(np.array([[0.5, 0.5]]), threshold=0.9)
        assert preds[0] in (0, 1)

    def test_save_and_load(self, tmp_path: Path):
        """Lines 268-323: save model + config, then load."""
        from heber.ml.trainer import MetaModelTrainer

        trainer = self._trained_trainer()
        model_path = tmp_path / "my_model"
        trainer.save(model_path)

        assert (tmp_path / "my_model.joblib").exists()
        assert (tmp_path / "my_model.json").exists()

        loaded = MetaModelTrainer.load(model_path)
        assert loaded._model is not None
        proba = loaded.predict_proba(np.array([[0.5, 0.5]]))
        assert 0 <= proba[0] <= 1

    def test_get_feature_importance(self):
        """Lines 337-340: get importance scores."""
        trainer = self._trained_trainer()
        importance = trainer.get_feature_importance(["f1", "f2"])
        assert "f1" in importance
        assert "f2" in importance


class TestTrainMetaModelConvenience:
    """Cover train_meta_model convenience function (lines 360-385)."""

    def test_train_meta_model_empty_data_raises(self, tmp_path: Path):
        """Line 367: raises ValueError on empty data."""
        from heber.ml.trainer import train_meta_model

        with patch("heber.ml.datasets.MetaLabelDatasetBuilder") as MockBuilder:
            instance = MockBuilder.return_value
            instance.build_from_parquet.return_value = pd.DataFrame()
            with pytest.raises(ValueError, match="No data found"):
                train_meta_model(date(2025, 1, 1), date(2025, 1, 31))


# ===========================================================================
# ML / INFERENCE
# ===========================================================================


class TestMetaLabelScorerInit:
    """Cover MetaLabelScorer initialization and classify."""

    def test_default_config(self):
        scorer = MetaLabelScorer()
        assert scorer.config.high_confidence_threshold == 0.7
        assert scorer.config.low_confidence_threshold == 0.3

    def test_classify_high(self):
        scorer = MetaLabelScorer()
        assert scorer.classify(0.8) == "high"

    def test_classify_medium(self):
        scorer = MetaLabelScorer()
        assert scorer.classify(0.5) == "medium"

    def test_classify_low(self):
        scorer = MetaLabelScorer()
        assert scorer.classify(0.2) == "low"


class TestMetaLabelScorerScore:
    """Cover score method paths (lines 94-141)."""

    @pytest.mark.asyncio
    async def test_score_no_model(self):
        """Lines 94-96: returns None when model not loaded."""
        scorer = MetaLabelScorer()
        alert = _make_flow_alert_record()
        result = await scorer.score(alert)
        assert result is None

    @pytest.mark.asyncio
    async def test_score_with_cached_result(self):
        """Lines 100-103: cached score returned."""
        mock_redis = AsyncMock()
        mock_redis.get.return_value = b"0.75"
        mock_model = MagicMock()
        scorer = MetaLabelScorer(model=mock_model, redis=mock_redis)
        scorer._feature_extractor = MagicMock()
        alert = _make_flow_alert_record()
        result = await scorer.score(alert)
        assert result == 0.75

    @pytest.mark.asyncio
    async def test_score_exception_returns_none(self):
        """Lines 134-141: exception returns None."""
        mock_model = MagicMock()
        mock_model.predict_proba.side_effect = RuntimeError("boom")
        scorer = MetaLabelScorer(model=mock_model)
        mock_extractor = AsyncMock()
        mock_features = MagicMock()
        mock_features.to_feature_array.return_value = [1.0, 2.0]
        mock_extractor.extract.return_value = mock_features
        scorer._feature_extractor = mock_extractor

        alert = _make_flow_alert_record()
        result = await scorer.score(alert)
        assert result is None


class TestMetaLabelScorerBatchAndFilter:
    """Cover score_batch and score_and_filter (lines 155-218)."""

    @pytest.mark.asyncio
    async def test_score_batch_with_exception(self):
        """Lines 155-165: batch scoring with exception handling."""
        scorer = MetaLabelScorer()
        alert1 = _make_flow_alert_record()
        alert1.event_id = "a1"
        alert2 = _make_flow_alert_record()
        alert2.event_id = "a2"
        results = await scorer.score_batch([alert1, alert2])
        # Both should be None since no model loaded
        assert results["a1"] is None
        assert results["a2"] is None

    @pytest.mark.asyncio
    async def test_score_and_filter(self):
        """Lines 186-218: filter by min_score."""
        scorer = MetaLabelScorer()
        # Patch score to return specific values
        alert1 = _make_flow_alert_record()
        alert1.event_id = "a1"
        alert2 = _make_flow_alert_record()
        alert2.event_id = "a2"

        async def mock_score(alert):
            if alert.event_id == "a1":
                return 0.8
            return 0.3

        scorer.score = mock_score
        results = await scorer.score_and_filter([alert1, alert2], min_score=0.5)
        assert len(results) == 1
        assert results[0][0].event_id == "a1"


class TestMetaLabelScorerCache:
    """Cover _get_cached_score and _cache_score (lines 220-238)."""

    @pytest.mark.asyncio
    async def test_get_cached_no_redis(self):
        scorer = MetaLabelScorer()
        result = await scorer._get_cached_score("a1")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_cached_miss(self):
        mock_redis = AsyncMock()
        mock_redis.get.return_value = None
        scorer = MetaLabelScorer(redis=mock_redis)
        result = await scorer._get_cached_score("a1")
        assert result is None

    @pytest.mark.asyncio
    async def test_cache_score_no_redis(self):
        scorer = MetaLabelScorer()
        await scorer._cache_score("a1", 0.75)  # should not raise

    @pytest.mark.asyncio
    async def test_cache_score_with_redis(self):
        mock_redis = AsyncMock()
        scorer = MetaLabelScorer(redis=mock_redis)
        await scorer._cache_score("a1", 0.75)
        mock_redis.set.assert_called_once()


class TestAlertGate:
    """Cover AlertGate (lines 241-296)."""

    @pytest.mark.asyncio
    async def test_gate_disabled_allows_all(self):
        """Line 274: disabled gate passes everything."""
        scorer = MetaLabelScorer()
        gate = AlertGate(scorer=scorer, enabled=False)
        alert = _make_flow_alert_record()
        assert await gate.should_create_watch(alert) is True

    @pytest.mark.asyncio
    async def test_gate_score_none_allows(self):
        """Lines 279-285: None score fails open."""
        scorer = MetaLabelScorer()
        gate = AlertGate(scorer=scorer, min_score=0.5)
        alert = _make_flow_alert_record()
        # No model loaded, score returns None
        assert await gate.should_create_watch(alert) is True

    @pytest.mark.asyncio
    async def test_gate_score_below_threshold(self):
        """Lines 287-296: score below threshold."""
        scorer = MetaLabelScorer()

        async def low_score(alert):
            return 0.3

        scorer.score = low_score
        gate = AlertGate(scorer=scorer, min_score=0.5)
        alert = _make_flow_alert_record()
        assert await gate.should_create_watch(alert) is False

    @pytest.mark.asyncio
    async def test_gate_score_above_threshold(self):
        scorer = MetaLabelScorer()

        async def high_score(alert):
            return 0.8

        scorer.score = high_score
        gate = AlertGate(scorer=scorer, min_score=0.5)
        alert = _make_flow_alert_record()
        assert await gate.should_create_watch(alert) is True


# ===========================================================================
# WATCH / CHECKER
# ===========================================================================


class TestBarrierCheckerCheckAll:
    """Cover check_all (lines 54-62)."""

    def test_check_all_no_active_watches(self):
        """Lines 54-62: no active watches returns empty."""
        manager = MagicMock()
        manager.get_active_watches.return_value = []
        calendar = MagicMock()
        checker = BarrierChecker(watch_manager=manager, calendar=calendar)
        outcomes = checker.check_all()
        assert outcomes == []

    def test_check_all_returns_completed(self):
        """check_all returns outcomes for completed watches."""
        watch = _make_watch(
            alert_time=datetime(2025, 6, 10, 10, 0, tzinfo=UTC),
            window_end=datetime(2025, 6, 10, 10, 1, tzinfo=UTC),  # Already expired
        )
        snap = _make_snapshot(
            mid_px=6.5,  # 30% return from entry_price=5.0
            timestamp=datetime(2025, 6, 10, 10, 5, tzinfo=UTC),
        )
        manager = MagicMock()
        manager.get_active_watches.return_value = [watch]
        manager.get_snapshots.return_value = [snap]
        calendar = MagicMock()
        calendar.trading_minutes_until.return_value = 5
        checker = BarrierChecker(watch_manager=manager, calendar=calendar)
        outcomes = checker.check_all()
        assert len(outcomes) == 1


class TestBarrierCheckerCheckWatch:
    """Cover check_watch edge cases (lines 64-145)."""

    def _make_checker(self, watch, snapshots):
        manager = MagicMock()
        manager.get_snapshots.return_value = snapshots
        calendar = MagicMock()
        calendar.trading_minutes_until.return_value = 30
        checker = BarrierChecker(watch_manager=manager, calendar=calendar)
        return checker

    def test_hit_tp(self):
        """Watch hits TP."""
        watch = _make_watch(entry_price=5.0, tp=0.25, sl=0.15)
        snap = _make_snapshot(mid_px=7.0, timestamp=datetime(2025, 6, 10, 10, 30, tzinfo=UTC))
        checker = self._make_checker(watch, [snap])
        outcome = checker.check_watch(watch)
        assert outcome is not None
        assert outcome.status == WatchStatus.HIT_TP
        assert outcome.hit_tp_first == 1

    def test_hit_sl(self):
        """Watch hits SL."""
        watch = _make_watch(entry_price=5.0, tp=0.25, sl=0.15)
        snap = _make_snapshot(mid_px=4.0, timestamp=datetime(2025, 6, 10, 10, 30, tzinfo=UTC))
        checker = self._make_checker(watch, [snap])
        outcome = checker.check_watch(watch)
        assert outcome is not None
        assert outcome.status == WatchStatus.HIT_SL
        assert outcome.hit_tp_first == 0

    def test_still_watching(self):
        """Returns None when still within barriers and window."""
        watch = _make_watch(
            entry_price=5.0,
            tp=0.25,
            sl=0.15,
            window_end=datetime(2099, 6, 10, 14, 0, tzinfo=UTC),
        )
        snap = _make_snapshot(mid_px=5.2, timestamp=datetime(2025, 6, 10, 10, 30, tzinfo=UTC))
        checker = self._make_checker(watch, [snap])
        outcome = checker.check_watch(watch)
        assert outcome is None

    def test_expired_with_snapshots(self):
        """Lines 102-104: window expires."""
        watch = _make_watch(
            entry_price=5.0,
            tp=0.50,
            sl=0.50,
            alert_time=datetime(2025, 6, 10, 10, 0, tzinfo=UTC),
            window_end=datetime(2025, 6, 10, 10, 1, tzinfo=UTC),  # Already expired
        )
        snap = _make_snapshot(mid_px=5.2, timestamp=datetime(2025, 6, 10, 10, 5, tzinfo=UTC))
        checker = self._make_checker(watch, [snap])
        outcome = checker.check_watch(watch)
        assert outcome is not None
        assert outcome.status == WatchStatus.EXPIRED


class TestHandleNoSnapshots:
    """Cover _handle_no_snapshots (line 208)."""

    def test_no_snapshots_before_window_end(self):
        """Returns None when window hasn't ended yet."""
        watch = _make_watch(
            window_end=datetime(2099, 6, 10, 14, 0, tzinfo=UTC),
        )
        manager = MagicMock()
        manager.get_snapshots.return_value = []
        calendar = MagicMock()
        calendar.trading_minutes_until.return_value = 0
        checker = BarrierChecker(watch_manager=manager, calendar=calendar)
        outcome = checker.check_watch(watch)
        assert outcome is None

    def test_no_snapshots_after_window_end(self):
        """Expires watch after window_end with no snapshots."""
        watch = _make_watch(
            alert_time=datetime(2025, 6, 10, 10, 0, tzinfo=UTC),
            window_end=datetime(2025, 6, 10, 10, 1, tzinfo=UTC),
        )
        manager = MagicMock()
        manager.get_snapshots.return_value = []
        calendar = MagicMock()
        calendar.trading_minutes_until.return_value = 0
        checker = BarrierChecker(watch_manager=manager, calendar=calendar)
        outcome = checker.check_watch(watch)
        assert outcome is not None
        assert outcome.status == WatchStatus.EXPIRED
        assert outcome.bars_to_hit == 0


class TestHandleNoReturns:
    """Cover _handle_no_returns (line 177)."""

    def test_no_valid_returns_before_expiry(self):
        """Returns None when no valid returns and window still open."""
        watch = _make_watch(
            window_end=datetime(2099, 6, 10, 14, 0, tzinfo=UTC),
        )
        # Snapshot with no mid_px and no return_pct
        snap = WatchSnapshot(
            watch_id="w1",
            occ_symbol="AAPL250620C00200000",
            timestamp=datetime(2025, 6, 10, 10, 30, tzinfo=UTC),
            mid_px=None,
            return_pct=None,
        )
        manager = MagicMock()
        manager.get_snapshots.return_value = [snap]
        calendar = MagicMock()
        calendar.trading_minutes_until.return_value = 0
        checker = BarrierChecker(watch_manager=manager, calendar=calendar)
        outcome = checker.check_watch(watch)
        assert outcome is None

    def test_no_valid_returns_after_expiry(self):
        """Expires when no valid returns and window ended."""
        watch = _make_watch(
            alert_time=datetime(2025, 6, 10, 10, 0, tzinfo=UTC),
            window_end=datetime(2025, 6, 10, 10, 1, tzinfo=UTC),
        )
        snap = WatchSnapshot(
            watch_id="w1",
            occ_symbol="AAPL250620C00200000",
            timestamp=datetime(2025, 6, 10, 10, 5, tzinfo=UTC),
            mid_px=None,
            return_pct=None,
        )
        manager = MagicMock()
        manager.get_snapshots.return_value = [snap]
        calendar = MagicMock()
        calendar.trading_minutes_until.return_value = 0
        checker = BarrierChecker(watch_manager=manager, calendar=calendar)
        outcome = checker.check_watch(watch)
        assert outcome is not None
        assert outcome.status == WatchStatus.EXPIRED


class TestBuildReturnPath:
    """Cover _build_return_path edge cases."""

    def test_uses_mid_px(self):
        snap = _make_snapshot(mid_px=6.0)
        returns, timestamps = BarrierChecker._build_return_path([snap], 5.0)
        assert len(returns) == 1
        assert returns[0] == pytest.approx(0.2)

    def test_falls_back_to_return_pct(self):
        snap = _make_snapshot(mid_px=None, return_pct=0.15)
        returns, timestamps = BarrierChecker._build_return_path([snap], 5.0)
        assert len(returns) == 1
        assert returns[0] == pytest.approx(0.15)

    def test_skips_nan_returns(self):
        snap = _make_snapshot(mid_px=float("inf"))
        returns, _ = BarrierChecker._build_return_path([snap], 5.0)
        assert len(returns) == 0

    def test_zero_entry_price_uses_return_pct(self):
        snap = _make_snapshot(mid_px=6.0, return_pct=0.10)
        returns, _ = BarrierChecker._build_return_path([snap], 0.0)
        assert len(returns) == 1
        assert returns[0] == pytest.approx(0.10)


class TestResolveOutcomeReturn:
    """Cover _resolve_outcome_return (lines 238-239)."""

    def test_bars_within_range(self):
        returns = [0.01, 0.05, 0.10]
        timestamps = [datetime(2025, 6, 10, 10, i * 5, tzinfo=UTC) for i in range(3)]
        ret, ts = BarrierChecker._resolve_outcome_return(returns, timestamps, 2)
        assert ret == pytest.approx(0.05)
        assert ts == timestamps[1]

    def test_bars_beyond_range(self):
        returns = [0.01, 0.05]
        timestamps = [datetime(2025, 6, 10, 10, i * 5, tzinfo=UTC) for i in range(2)]
        ret, ts = BarrierChecker._resolve_outcome_return(returns, timestamps, 10)
        assert ret == pytest.approx(0.05)
        assert ts == timestamps[-1]

    def test_bars_none(self):
        returns = [0.01, 0.05, 0.10]
        timestamps = [datetime(2025, 6, 10, 10, i * 5, tzinfo=UTC) for i in range(3)]
        ret, ts = BarrierChecker._resolve_outcome_return(returns, timestamps, None)
        assert ret == pytest.approx(0.10)


class TestOutcomeToLabelRow:
    """Cover outcome_to_label_row for different statuses."""

    def test_hit_tp(self):
        outcome = _make_outcome(WatchStatus.HIT_TP)
        row = outcome_to_label_row(outcome)
        assert row["outcome"] == "hit_tp"
        assert row["hit_tp_first"] == 1
        assert row["contract_hit_tp_first"] == 1

    def test_hit_sl(self):
        outcome = _make_outcome(WatchStatus.HIT_SL)
        row = outcome_to_label_row(outcome)
        assert row["outcome"] == "hit_sl"
        assert row["hit_tp_first"] == 0

    def test_expired(self):
        outcome = _make_outcome(WatchStatus.EXPIRED)
        row = outcome_to_label_row(outcome)
        assert row["outcome"] == "expired"
        assert row["hit_tp_first"] == 0


# ===========================================================================
# WATCH / FEATURES
# ===========================================================================


class TestFeatureHelpers:
    """Cover helper functions in features.py."""

    def test_unwrap_data_payload_dict(self):
        result = _unwrap_data_payload({"data": {"foo": "bar"}})
        assert result == {"foo": "bar"}

    def test_unwrap_data_payload_list(self):
        """Unwraps single-element list."""
        result = _unwrap_data_payload({"data": [{"foo": "bar"}]})
        assert result == {"foo": "bar"}

    def test_unwrap_data_payload_empty(self):
        result = _unwrap_data_payload({})
        assert result == {}

    def test_coalesce_first_hit(self):
        data = {"a": None, "b": "5.0"}
        assert _coalesce_first(data, "a", "b") == 5.0

    def test_coalesce_first_miss(self):
        assert _coalesce_first({}, "a", "b") is None

    def test_coalesce_split_or_fallback_split(self):
        data = {"call_gamma": "100", "put_gamma": "50"}
        result = _coalesce_split_or_fallback(data, call_key="call_gamma", put_key="put_gamma", fallbacks=("gex",))
        assert result == 150.0

    def test_coalesce_split_or_fallback_partial(self):
        data = {"call_gamma": "100"}
        result = _coalesce_split_or_fallback(data, call_key="call_gamma", put_key="put_gamma", fallbacks=("gex",))
        assert result == 100.0

    def test_coalesce_split_or_fallback_uses_fallback(self):
        data = {"gex": "200"}
        result = _coalesce_split_or_fallback(data, call_key="call_gamma", put_key="put_gamma", fallbacks=("gex",))
        assert result == 200.0

    def test_classify_direction(self):
        assert _classify_direction(100) == "bullish"
        assert _classify_direction(-50) == "bearish"
        assert _classify_direction(0) == "neutral"


class TestAlertFeatures:
    """Cover AlertFeatures serialization and feature array conversion."""

    def _make_features(self) -> AlertFeatures:
        return AlertFeatures(
            alert_id="test-1",
            alert_time=datetime(2025, 6, 10, 10, 0, tzinfo=UTC),
            symbol="AAPL",
            occ_symbol="AAPL250620C00200000",
            underlying="AAPL",
            strike=200.0,
            expiry=date(2025, 6, 20),
            put_call="C",
            days_to_expiry=10,
            premium=50000.0,
            volume=500.0,
            open_interest=1000.0,
            volume_oi_ratio=0.5,
            alert_type="SWEEP",
            side="ask",
            aggressor="buyer",
            spot_price=198.0,
            contract_price=5.0,
            moneyness=1.01,
            delta=0.45,
            iv=0.30,
        )

    def test_to_dict_and_from_dict_roundtrip(self):
        features = self._make_features()
        d = features.to_dict()
        assert isinstance(d["alert_time"], str)
        assert isinstance(d["expiry"], str)
        restored = AlertFeatures.from_dict(d)
        assert restored.alert_id == "test-1"
        assert restored.strike == 200.0

    def test_to_feature_array_default(self):
        features = self._make_features()
        arr = features.to_feature_array()
        assert len(arr) == len(AlertFeatures.numeric_feature_names())
        assert all(isinstance(v, float) for v in arr)

    def test_to_feature_array_specific_names(self):
        features = self._make_features()
        arr = features.to_feature_array(feature_names=["delta", "iv", "volume"])
        assert len(arr) == 3
        assert arr[0] == 0.45
        assert arr[1] == 0.30
        assert arr[2] == 500.0

    def test_to_feature_array_missing_attr(self):
        """Non-existent attribute returns 0.0."""
        features = self._make_features()
        arr = features.to_feature_array(feature_names=["nonexistent_feature"])
        assert arr == [0.0]

    def test_to_feature_array_non_numeric_field(self):
        """String attribute returns 0.0."""
        features = self._make_features()
        arr = features.to_feature_array(feature_names=["alert_type"])
        assert arr == [0.0]

    def test_from_dict_naive_alert_time(self):
        """Naive alert_time gets UTC timezone."""
        d = self._make_features().to_dict()
        d["alert_time"] = "2025-06-10T10:00:00"
        restored = AlertFeatures.from_dict(d)
        assert restored.alert_time.tzinfo is not None


class TestAlertFeatureExtractorEdgeCases:
    """Cover feature extractor methods and edge cases."""

    def test_to_market_time_naive(self):
        extractor = AlertFeatureExtractor()
        dt = datetime(2025, 6, 10, 14, 0, 0)  # naive
        result = extractor._to_market_time(dt)
        assert result.tzinfo is not None

    def test_cache_operations(self):
        extractor = AlertFeatureExtractor(cache_ttl_seconds=60.0)
        extractor._cache_set("key1", {"data": "value"})
        assert extractor._cache_get("key1") == {"data": "value"}

    def test_cache_disabled(self):
        extractor = AlertFeatureExtractor(cache_ttl_seconds=0.0)
        extractor._cache_set("key1", {"data": "value"})
        assert extractor._cache_get("key1") is None

    def test_record_auth_failure(self):
        extractor = AlertFeatureExtractor(
            auth_failure_threshold=3,
            auth_failure_window_seconds=60.0,
        )
        count1 = extractor._record_auth_failure()
        count2 = extractor._record_auth_failure()
        assert count1 == 1
        assert count2 == 2

    def test_compute_returns(self):
        features = AlertFeatures(
            alert_id="t1",
            alert_time=datetime(2025, 6, 10, 10, 0, tzinfo=UTC),
            symbol="AAPL",
            occ_symbol=None,
            underlying="AAPL",
            strike=200.0,
            expiry=date(2025, 6, 20),
            put_call="C",
            days_to_expiry=10,
            premium=1000.0,
            volume=100.0,
            open_interest=None,
            volume_oi_ratio=None,
            alert_type="SWEEP",
            side=None,
            aggressor=None,
            spot_price=200.0,
            contract_price=5.0,
        )
        closes = [105.0, 100.0, 99.0, 98.0, 97.0, 90.0] + [80.0] * 25
        AlertFeatureExtractor._compute_returns(features, closes)
        assert features.underlying_1d_return == pytest.approx(0.05)
        assert features.underlying_5d_return == pytest.approx(105.0 / 90.0 - 1)

    def test_compute_returns_insufficient_closes(self):
        features = AlertFeatures(
            alert_id="t1",
            alert_time=datetime(2025, 6, 10, 10, 0, tzinfo=UTC),
            symbol="AAPL",
            occ_symbol=None,
            underlying="AAPL",
            strike=200.0,
            expiry=date(2025, 6, 20),
            put_call="C",
            days_to_expiry=10,
            premium=1000.0,
            volume=100.0,
            open_interest=None,
            volume_oi_ratio=None,
            alert_type="SWEEP",
            side=None,
            aggressor=None,
            spot_price=200.0,
            contract_price=5.0,
        )
        closes = [105.0]
        AlertFeatureExtractor._compute_returns(features, closes)
        assert features.underlying_1d_return is None

    def test_compute_realized_vol_20d(self):
        features = AlertFeatures(
            alert_id="t1",
            alert_time=datetime(2025, 6, 10, 10, 0, tzinfo=UTC),
            symbol="AAPL",
            occ_symbol=None,
            underlying="AAPL",
            strike=200.0,
            expiry=date(2025, 6, 20),
            put_call="C",
            days_to_expiry=10,
            premium=1000.0,
            volume=100.0,
            open_interest=None,
            volume_oi_ratio=None,
            alert_type="SWEEP",
            side=None,
            aggressor=None,
            spot_price=200.0,
            contract_price=5.0,
        )
        # 21 closes needed
        closes = [100.0 + i * 0.5 for i in range(21)]
        AlertFeatureExtractor._compute_realized_vol_20d(features, closes)
        assert features.realized_vol_20d is not None
        assert features.realized_vol_20d > 0

    def test_compute_realized_vol_insufficient(self):
        features = AlertFeatures(
            alert_id="t1",
            alert_time=datetime(2025, 6, 10, 10, 0, tzinfo=UTC),
            symbol="AAPL",
            occ_symbol=None,
            underlying="AAPL",
            strike=200.0,
            expiry=date(2025, 6, 20),
            put_call="C",
            days_to_expiry=10,
            premium=1000.0,
            volume=100.0,
            open_interest=None,
            volume_oi_ratio=None,
            alert_type="SWEEP",
            side=None,
            aggressor=None,
            spot_price=200.0,
            contract_price=5.0,
        )
        closes = [100.0] * 10  # Not enough
        AlertFeatureExtractor._compute_realized_vol_20d(features, closes)
        assert features.realized_vol_20d is None

    def test_find_matching_contract_exact(self):
        extractor = AlertFeatureExtractor()
        contracts = [
            {"strike_price": 200.0, "delta": 0.5},
            {"strike_price": 210.0, "delta": 0.4},
        ]
        result = extractor._find_matching_contract(contracts, 200.0)
        assert result["delta"] == 0.5

    def test_find_matching_contract_fallback(self):
        extractor = AlertFeatureExtractor()
        contracts = [
            {"strike_price": 210.0, "delta": 0.4},
        ]
        result = extractor._find_matching_contract(contracts, 200.0)
        assert result["delta"] == 0.4

    def test_find_matching_contract_none(self):
        extractor = AlertFeatureExtractor()
        result = extractor._find_matching_contract([], 200.0)
        assert result is None


class TestStoreAndGetFeatures:
    """Cover store_features and get_features (lines 1089-1118)."""

    @pytest.mark.asyncio
    async def test_store_and_get_roundtrip(self):
        mock_redis = AsyncMock()
        stored_data = {}

        async def mock_set(key, value, ex=None):
            stored_data[key] = value

        async def mock_get(key):
            return stored_data.get(key)

        mock_redis.set = mock_set
        mock_redis.get = mock_get

        features = AlertFeatures(
            alert_id="test-store",
            alert_time=datetime(2025, 6, 10, 10, 0, tzinfo=UTC),
            symbol="AAPL",
            occ_symbol=None,
            underlying="AAPL",
            strike=200.0,
            expiry=date(2025, 6, 20),
            put_call="C",
            days_to_expiry=10,
            premium=1000.0,
            volume=100.0,
            open_interest=None,
            volume_oi_ratio=None,
            alert_type="SWEEP",
            side=None,
            aggressor=None,
            spot_price=198.0,
            contract_price=5.0,
        )

        await store_features(mock_redis, features)
        retrieved = await get_features(mock_redis, "test-store")
        assert retrieved is not None
        assert retrieved.alert_id == "test-store"
        assert retrieved.symbol == "AAPL"

    @pytest.mark.asyncio
    async def test_get_features_not_found(self):
        mock_redis = AsyncMock()
        mock_redis.get.return_value = None
        result = await get_features(mock_redis, "nonexistent")
        assert result is None


# ===========================================================================
# WATCH / CONSUMER
# ===========================================================================


class TestClassifyAlertResults:
    """Cover _classify_alert_results static method."""

    def test_created_success(self):
        s, r, reason = AlertWatchConsumer._classify_alert_results(1, 0, 0, 0)
        assert s is True
        assert reason == "watch_created"

    def test_stale_window_only(self):
        s, r, reason = AlertWatchConsumer._classify_alert_results(0, 0, 1, 0)
        assert s is True
        assert reason == "stale_alert_window"

    def test_missing_occ_only(self):
        s, r, reason = AlertWatchConsumer._classify_alert_results(0, 1, 0, 0)
        assert s is True
        assert reason == "missing_occ_symbol"

    def test_stale_and_missing_occ(self):
        """When both stale_window and missing_occ are set, missing_occ branch wins (line 370)."""
        s, r, reason = AlertWatchConsumer._classify_alert_results(0, 1, 1, 0)
        assert s is True
        # Line 370 catches this first: missing_occ > 0 and exceptions == 0
        assert reason == "missing_occ_symbol"

    def test_exceptions(self):
        s, r, reason = AlertWatchConsumer._classify_alert_results(0, 0, 0, 1)
        assert s is False
        assert r is True
        assert reason == "processing_exception"

    def test_all_zero(self):
        s, r, reason = AlertWatchConsumer._classify_alert_results(0, 0, 0, 0)
        assert s is False
        assert r is False
        assert reason == "alert_parse_failed"


class TestNormalizeProcessResult:
    """Cover _normalize_process_result static method."""

    def test_bool_true(self):
        s, r, reason = AlertWatchConsumer._normalize_process_result(True)
        assert s is True
        assert r is False

    def test_bool_false(self):
        s, r, reason = AlertWatchConsumer._normalize_process_result(False)
        assert s is False
        assert r is True

    def test_tuple_three(self):
        s, r, reason = AlertWatchConsumer._normalize_process_result((True, False, "custom"))
        assert s is True
        assert r is False
        assert reason == "custom"

    def test_tuple_two(self):
        s, r, reason = AlertWatchConsumer._normalize_process_result((True, False))
        assert s is True
        assert r is False

    def test_tuple_one(self):
        s, r, reason = AlertWatchConsumer._normalize_process_result((True,))
        assert s is True

    def test_tuple_three_none_reason(self):
        s, r, reason = AlertWatchConsumer._normalize_process_result((False, True, None))
        assert s is False
        assert r is True
        assert reason == "processing_failed"


class TestConsumerIsFlowAlert:
    """Cover _is_flow_alert (lines 331-356)."""

    def _make_consumer(self):
        return AlertWatchConsumer.__new__(AlertWatchConsumer)

    def test_no_data_key(self):
        consumer = self._make_consumer()
        assert consumer._is_flow_alert({}) is False

    def test_data_bytes_flow_alerts(self):
        consumer = self._make_consumer()
        payload = json.dumps({"feed": "flow_alerts"}).encode()
        assert consumer._is_flow_alert({b"data": payload}) is True

    def test_data_string_flow_alerts(self):
        consumer = self._make_consumer()
        payload = json.dumps({"feed": "flow_alerts"})
        assert consumer._is_flow_alert({"data": payload}) is True

    def test_data_dict_flow_alerts(self):
        consumer = self._make_consumer()
        assert consumer._is_flow_alert({"data": {"feed": "flow_alerts"}}) is True

    def test_data_wrong_feed(self):
        consumer = self._make_consumer()
        payload = json.dumps({"feed": "darkpool"})
        assert consumer._is_flow_alert({"data": payload}) is False

    def test_data_invalid_json(self):
        consumer = self._make_consumer()
        assert consumer._is_flow_alert({b"data": b"not json{"}) is False


class TestConsumerNormalizePutCall:
    """Cover _normalize_put_call."""

    def test_call_variants(self):
        assert AlertWatchConsumer._normalize_put_call("call") == "C"
        assert AlertWatchConsumer._normalize_put_call("C") == "C"
        assert AlertWatchConsumer._normalize_put_call("CALL") == "C"

    def test_put_variants(self):
        assert AlertWatchConsumer._normalize_put_call("put") == "P"
        assert AlertWatchConsumer._normalize_put_call("P") == "P"
        assert AlertWatchConsumer._normalize_put_call("PUT") == "P"

    def test_default(self):
        assert AlertWatchConsumer._normalize_put_call(None) is None
        assert AlertWatchConsumer._normalize_put_call(123) is None


class TestConsumerNormalizeTags:
    """Cover _normalize_tags."""

    def test_none_input(self):
        assert AlertWatchConsumer._normalize_tags(None) is None

    def test_json_list_string(self):
        result = AlertWatchConsumer._normalize_tags('["bullish", "sweep"]')
        assert result == ["bullish", "sweep"]

    def test_comma_separated(self):
        result = AlertWatchConsumer._normalize_tags("bullish,bearish")
        assert result == ["bullish", "bearish"]

    def test_pipe_separated(self):
        result = AlertWatchConsumer._normalize_tags("bullish|bearish")
        assert result == ["bullish", "bearish"]

    def test_list_input(self):
        result = AlertWatchConsumer._normalize_tags(["bullish", "sweep"])
        assert result == ["bullish", "sweep"]

    def test_empty_string(self):
        assert AlertWatchConsumer._normalize_tags("") is None

    def test_empty_list(self):
        assert AlertWatchConsumer._normalize_tags([]) is None


class TestConsumerCalculateDte:
    """Cover _calculate_dte."""

    def _make_consumer(self):
        return AlertWatchConsumer.__new__(AlertWatchConsumer)

    def test_valid_expiry(self):
        consumer = self._make_consumer()
        future_date = (datetime.now() + timedelta(days=10)).strftime("%Y-%m-%d")
        dte = consumer._calculate_dte(future_date)
        assert 9 <= dte <= 11

    def test_none_expiry(self):
        consumer = self._make_consumer()
        assert consumer._calculate_dte(None) == 5

    def test_invalid_expiry(self):
        consumer = self._make_consumer()
        assert consumer._calculate_dte("not-a-date") == 5
