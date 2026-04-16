"""Comprehensive tests for HeberReader.

Covers Silver reads, Gold reads/writes, asof_join, read_parquet_dataset,
list_gold_versions, and module-level helpers.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from heber.reader.core import (
    HeberReader,
    _build_scan_filter,
    _detect_time_col,
    _to_utc,
    _tuple_filters_to_exprs,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _ts(offset_hours: int = 0) -> datetime:
    """UTC timestamp helper (base = 2026-01-15T10:00:00Z)."""
    return datetime(2026, 1, 15, 10, 0, 0, tzinfo=UTC) + timedelta(hours=offset_hours)


def _write_silver_parquet(
    base: Path,
    feed: str,
    instrument_type: str,
    rows: list[dict],
) -> Path:
    """Write a Silver-like parquet dataset and return the feed path."""
    df = pd.DataFrame(rows)
    table = pa.Table.from_pandas(df)
    feed_path = base / "silver" / f"feed={feed}" / f"instrument_type={instrument_type}"
    dt_value = pd.to_datetime(rows[0]["ts_event"]).strftime("%Y-%m-%d")
    partition_dir = feed_path / f"dt={dt_value}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, str(partition_dir / "data.parquet"))
    return feed_path


def _write_gold_parquet(
    base: Path,
    dataset: str,
    project: str,
    version: str,
    rows: list[dict],
) -> Path:
    """Write a Gold-like parquet dataset and return the dataset path."""
    df = pd.DataFrame(rows)
    table = pa.Table.from_pandas(df)
    dt_value = pd.to_datetime(rows[0]["ts_event"]).strftime("%Y-%m-%d")
    partition_dir = (
        base / "gold" / f"dataset={dataset}" / f"project={project}" / f"version={version}" / f"dt={dt_value}"
    )
    partition_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, str(partition_dir / "data.parquet"))
    return partition_dir


@pytest.fixture()
def silver_data(tmp_path: Path) -> tuple[Path, pd.DataFrame]:
    """Create a Silver dataset with 4 rows and return (data_root, expected_df)."""
    rows = [
        {
            "ts_event": _ts(0),
            "ts_available": _ts(1),
            "instrument_key": "equity:AAPL",
            "close": 150.0,
        },
        {
            "ts_event": _ts(1),
            "ts_available": _ts(2),
            "instrument_key": "equity:AAPL",
            "close": 151.0,
        },
        {
            "ts_event": _ts(2),
            "ts_available": _ts(3),
            "instrument_key": "equity:TSLA",
            "close": 250.0,
        },
        {
            "ts_event": _ts(3),
            "ts_available": _ts(4),
            "instrument_key": "equity:TSLA",
            "close": 252.0,
        },
    ]
    _write_silver_parquet(tmp_path, "bars", "equity", rows)
    return tmp_path, pd.DataFrame(rows)


@pytest.fixture()
def gold_data(tmp_path: Path) -> tuple[Path, pd.DataFrame]:
    """Create Gold dataset with two versions — v1 and v2."""
    rows_v1 = [
        {
            "ts_event": _ts(0),
            "ts_available": _ts(1),
            "instrument_key": "equity:AAPL",
            "feature_a": 1.0,
        },
    ]
    rows_v2 = [
        {
            "ts_event": _ts(0),
            "ts_available": _ts(1),
            "instrument_key": "equity:AAPL",
            "feature_a": 2.0,
        },
        {
            "ts_event": _ts(2),
            "ts_available": _ts(3),
            "instrument_key": "equity:TSLA",
            "feature_a": 3.0,
        },
    ]
    _write_gold_parquet(tmp_path, "features", "kairos", "v1", rows_v1)
    _write_gold_parquet(tmp_path, "features", "kairos", "v2", rows_v2)
    return tmp_path, pd.DataFrame(rows_v2)


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


class TestToUtc:
    def test_naive_string(self) -> None:
        result = _to_utc("2026-01-15T10:00:00")
        assert result.as_py().tzinfo is not None

    def test_aware_datetime(self) -> None:
        result = _to_utc(_ts(0))
        assert result.as_py() == _ts(0)

    def test_date_string(self) -> None:
        result = _to_utc("2026-01-15")
        assert result.as_py().year == 2026


class TestBuildScanFilter:
    def test_empty_list(self) -> None:
        assert _build_scan_filter([]) is None

    def test_single_expression(self) -> None:
        expr = ds.field("x") > pa.scalar(1)
        assert _build_scan_filter([expr]) is not None

    def test_multiple_expressions(self) -> None:
        e1 = ds.field("x") > pa.scalar(1)
        e2 = ds.field("y") < pa.scalar(10)
        result = _build_scan_filter([e1, e2])
        assert result is not None


class TestDetectTimeCol:
    def test_ts_event_preferred(self) -> None:
        assert _detect_time_col({"ts_event", "ts_label", "foo"}) == "ts_event"

    def test_fallback_ts_label(self) -> None:
        assert _detect_time_col({"ts_label", "bar"}) == "ts_label"

    def test_none_when_missing(self) -> None:
        assert _detect_time_col({"foo", "bar"}) is None


class TestTupleFiltersToExprs:
    def test_basic_eq(self) -> None:
        exprs = _tuple_filters_to_exprs([("col", "==", 42)], {"col"})
        assert len(exprs) == 1

    def test_skips_missing_column(self) -> None:
        exprs = _tuple_filters_to_exprs([("missing", "==", 1)], {"col"})
        assert len(exprs) == 0

    def test_isin(self) -> None:
        exprs = _tuple_filters_to_exprs([("col", "in", [1, 2])], {"col"})
        assert len(exprs) == 1

    def test_unknown_op_ignored(self) -> None:
        exprs = _tuple_filters_to_exprs([("col", "???", 1)], {"col"})
        assert len(exprs) == 0


# ---------------------------------------------------------------------------
# Silver reads
# ---------------------------------------------------------------------------


class TestReadSilver:
    def test_full_read(self, silver_data: tuple[Path, pd.DataFrame]) -> None:
        root, expected = silver_data
        reader = HeberReader(root)
        df = reader.read_silver("bars")
        assert len(df) == len(expected)

    def test_instrument_key_filter(self, silver_data: tuple[Path, pd.DataFrame]) -> None:
        root, _ = silver_data
        reader = HeberReader(root)
        df = reader.read_silver("bars", instrument_keys=["equity:AAPL"])
        assert len(df) == 2
        assert (df["instrument_key"] == "equity:AAPL").all()

    def test_time_range_filter(self, silver_data: tuple[Path, pd.DataFrame]) -> None:
        root, _ = silver_data
        reader = HeberReader(root)
        df = reader.read_silver("bars", time_range=(_ts(0), _ts(1)))
        assert len(df) == 2

    def test_asof_time_filter(self, silver_data: tuple[Path, pd.DataFrame]) -> None:
        root, _ = silver_data
        reader = HeberReader(root)
        # Only rows with ts_available <= _ts(2) should appear
        df = reader.read_silver("bars", asof_time=_ts(2))
        assert len(df) == 2

    def test_column_projection(self, silver_data: tuple[Path, pd.DataFrame]) -> None:
        root, _ = silver_data
        reader = HeberReader(root)
        df = reader.read_silver("bars", columns=["close"])
        # Essential columns are always included when present
        assert "close" in df.columns
        assert "ts_event" in df.columns

    def test_instrument_type_partition(self, silver_data: tuple[Path, pd.DataFrame]) -> None:
        root, _ = silver_data
        reader = HeberReader(root)
        df = reader.read_silver("bars", instrument_type="equity")
        assert len(df) == 4

    def test_missing_feed_returns_empty(self, tmp_path: Path) -> None:
        reader = HeberReader(tmp_path)
        df = reader.read_silver("nonexistent")
        assert df.empty


class TestReadAsof:
    def test_delegates_to_read_silver(self, silver_data: tuple[Path, pd.DataFrame]) -> None:
        root, _ = silver_data
        reader = HeberReader(root)
        df = reader.read_asof("bars", asof_time=_ts(2))
        assert len(df) == 2


# ---------------------------------------------------------------------------
# Gold reads
# ---------------------------------------------------------------------------


class TestReadGold:
    def test_specific_version(self, gold_data: tuple[Path, pd.DataFrame]) -> None:
        root, _ = gold_data
        reader = HeberReader(root)
        df = reader.read_gold("features", project="kairos", version="v1")
        assert len(df) == 1

    def test_latest_version(self, gold_data: tuple[Path, pd.DataFrame]) -> None:
        root, _ = gold_data
        reader = HeberReader(root)
        df = reader.read_gold("features", project="kairos")
        # Latest version=v2 has 2 rows
        assert len(df) == 2

    def test_instrument_key_filter(self, gold_data: tuple[Path, pd.DataFrame]) -> None:
        root, _ = gold_data
        reader = HeberReader(root)
        df = reader.read_gold(
            "features",
            project="kairos",
            version="v2",
            instrument_keys=["equity:AAPL"],
        )
        assert len(df) == 1

    def test_asof_time_filter(self, gold_data: tuple[Path, pd.DataFrame]) -> None:
        root, _ = gold_data
        reader = HeberReader(root)
        df = reader.read_gold("features", project="kairos", version="v2", asof_time=_ts(1))
        assert len(df) == 1

    def test_missing_dataset_returns_empty(self, tmp_path: Path) -> None:
        reader = HeberReader(tmp_path)
        df = reader.read_gold("nonexistent")
        assert df.empty

    def test_no_versions_returns_empty(self, tmp_path: Path) -> None:
        # Create dataset dir but no version= subdirectories
        (tmp_path / "gold" / "dataset=features" / "project=kairos").mkdir(parents=True)
        reader = HeberReader(tmp_path)
        df = reader.read_gold("features", project="kairos")
        assert df.empty


# ---------------------------------------------------------------------------
# Gold writes
# ---------------------------------------------------------------------------


class TestWriteGold:
    def _make_df(self, n: int = 3) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "instrument_key": [f"equity:SYM{i}" for i in range(n)],
                "ts_event": [_ts(i) for i in range(n)],
                "ts_available": [_ts(i + 1) for i in range(n)],
                "value": list(range(n)),
            }
        )

    def test_write_and_read_back(self, tmp_path: Path) -> None:
        reader = HeberReader(tmp_path)
        df = self._make_df()
        out = reader.write_gold("test_ds", df, project="test", version="v1")
        assert out is not None
        assert out.exists()

        read_back = reader.read_gold("test_ds", project="test", version="v1")
        assert len(read_back) == len(df)

    def test_empty_df_returns_none(self, tmp_path: Path) -> None:
        reader = HeberReader(tmp_path)
        empty = pd.DataFrame(columns=["instrument_key", "ts_event", "ts_available"])
        result = reader.write_gold("test_ds", empty, project="test", version="v1")
        assert result is None

    def test_missing_columns_raises(self, tmp_path: Path) -> None:
        reader = HeberReader(tmp_path)
        df = pd.DataFrame({"foo": [1]})
        with pytest.raises(ValueError, match="Missing required columns"):
            reader.write_gold("test_ds", df, project="test", version="v1")

    def test_lookahead_raises(self, tmp_path: Path) -> None:
        reader = HeberReader(tmp_path)
        df = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": [_ts(2)],
                "ts_available": [_ts(0)],  # before ts_event → lookahead
            }
        )
        with pytest.raises(ValueError, match="look-ahead"):
            reader.write_gold("test_ds", df, project="test", version="v1")

    def test_metadata_stored(self, tmp_path: Path) -> None:
        reader = HeberReader(tmp_path)
        df = self._make_df(1)
        out = reader.write_gold(
            "test_ds",
            df,
            project="test",
            version="v1",
            metadata={"pipeline": "unit_test"},
        )
        assert out is not None
        table = pq.read_table(str(out))
        assert b"pipeline" in table.schema.metadata


# ---------------------------------------------------------------------------
# asof_join
# ---------------------------------------------------------------------------


class TestAsofJoin:
    def test_basic_join(self) -> None:
        reader = HeberReader(Path("/tmp/unused"))
        left = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL", "equity:AAPL"],
                "ts_event": pd.to_datetime([_ts(2), _ts(4)]),
            }
        )
        right = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL", "equity:AAPL"],
                "ts_event": pd.to_datetime([_ts(0), _ts(3)]),
                "ts_available": pd.to_datetime([_ts(1), _ts(4)]),
                "feature": [10.0, 20.0],
            }
        )
        result = reader.asof_join(left, right)
        assert len(result) == 2
        assert "feature" in result.columns

    def test_tolerance(self) -> None:
        reader = HeberReader(Path("/tmp/unused"))
        left = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": pd.to_datetime([_ts(10)]),
            }
        )
        right = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": pd.to_datetime([_ts(0)]),
                "ts_available": pd.to_datetime([_ts(0)]),
                "feature": [99.0],
            }
        )
        result = reader.asof_join(left, right, tolerance="1h")
        # 10 hours gap > 1h tolerance so feature should be NaN
        assert pd.isna(result["feature"].iloc[0])

    def test_safe_time_column_dropped(self) -> None:
        reader = HeberReader(Path("/tmp/unused"))
        left = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": pd.to_datetime([_ts(2)]),
            }
        )
        right = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": pd.to_datetime([_ts(0)]),
                "ts_available": pd.to_datetime([_ts(1)]),
                "feature": [1.0],
            }
        )
        result = reader.asof_join(left, right)
        assert "_safe_time" not in result.columns


# ---------------------------------------------------------------------------
# read_parquet_dataset
# ---------------------------------------------------------------------------


class TestReadParquetDataset:
    def test_with_tuple_filters(self, silver_data: tuple[Path, pd.DataFrame]) -> None:
        root, _ = silver_data
        reader = HeberReader(root)
        path = root / "silver" / "feed=bars" / "instrument_type=equity"
        df = reader.read_parquet_dataset(
            path,
            filters=[("instrument_key", "==", "equity:AAPL")],
        )
        assert len(df) == 2

    def test_missing_path_returns_empty(self, tmp_path: Path) -> None:
        reader = HeberReader(tmp_path)
        df = reader.read_parquet_dataset(tmp_path / "nope")
        assert df.empty

    def test_time_range(self, silver_data: tuple[Path, pd.DataFrame]) -> None:
        root, _ = silver_data
        reader = HeberReader(root)
        path = root / "silver" / "feed=bars" / "instrument_type=equity"
        df = reader.read_parquet_dataset(path, time_range=(_ts(0), _ts(1)))
        assert len(df) == 2

    def test_asof_time(self, silver_data: tuple[Path, pd.DataFrame]) -> None:
        root, _ = silver_data
        reader = HeberReader(root)
        path = root / "silver" / "feed=bars" / "instrument_type=equity"
        df = reader.read_parquet_dataset(path, asof_time=_ts(2))
        assert len(df) == 2


# ---------------------------------------------------------------------------
# list_gold_versions
# ---------------------------------------------------------------------------


class TestListGoldVersions:
    def test_returns_sorted_versions(self, gold_data: tuple[Path, pd.DataFrame]) -> None:
        root, _ = gold_data
        reader = HeberReader(root)
        versions = reader.list_gold_versions("features", project="kairos")
        assert versions == ["v2", "v1"]

    def test_empty_when_no_dataset(self, tmp_path: Path) -> None:
        reader = HeberReader(tmp_path)
        assert reader.list_gold_versions("nope") == []


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------


class TestContextManager:
    def test_enter_exit(self, tmp_path: Path) -> None:
        with HeberReader(tmp_path) as reader:
            assert reader._root == tmp_path


# ---------------------------------------------------------------------------
# P2 coverage — semver ordering, multi-date write, combined filters, multi-instrument join
# ---------------------------------------------------------------------------


class TestSemverOrdering:
    def test_v10_after_v2(self, tmp_path: Path) -> None:
        """Versions v10 and v2 must sort by semantic version, not lexicographic."""
        for v in ("v1.0.0", "v2.0.0", "v10.0.0"):
            _write_gold_parquet(
                tmp_path,
                "feat",
                "proj",
                v,
                [
                    {
                        "ts_event": _ts(0),
                        "ts_available": _ts(1),
                        "instrument_key": "equity:AAPL",
                        "val": 1.0,
                    }
                ],
            )
        reader = HeberReader(tmp_path)
        versions = reader.list_gold_versions("feat", project="proj")
        assert versions[0] == "v10.0.0"
        assert versions[-1] == "v1.0.0"

    def test_latest_version_is_semver_latest(self, tmp_path: Path) -> None:
        """read_gold(version=None) should pick v10.0.0 over v2.0.0."""
        _write_gold_parquet(
            tmp_path,
            "feat",
            "proj",
            "v2.0.0",
            [
                {
                    "ts_event": _ts(0),
                    "ts_available": _ts(1),
                    "instrument_key": "equity:AAPL",
                    "val": 2.0,
                }
            ],
        )
        _write_gold_parquet(
            tmp_path,
            "feat",
            "proj",
            "v10.0.0",
            [
                {
                    "ts_event": _ts(0),
                    "ts_available": _ts(1),
                    "instrument_key": "equity:AAPL",
                    "val": 10.0,
                }
            ],
        )
        reader = HeberReader(tmp_path)
        df = reader.read_gold("feat", project="proj")
        assert df["val"].iloc[0] == pytest.approx(10.0)


class TestMultiDatePartitioning:
    def test_write_creates_per_date_dirs(self, tmp_path: Path) -> None:
        reader = HeberReader(tmp_path)
        df = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL", "equity:AAPL"],
                "ts_event": [_ts(0), _ts(24)],  # 24hrs apart → 2 dates
                "ts_available": [_ts(1), _ts(25)],
                "val": [1.0, 2.0],
            }
        )
        out = reader.write_gold("ds", df, project="p", version="v1")
        assert out is not None
        # Read back should return both rows
        read_back = reader.read_gold("ds", project="p", version="v1")
        assert len(read_back) == 2


class TestCombinedSilverFilters:
    def test_time_range_plus_keys_plus_asof(self, silver_data: tuple[Path, pd.DataFrame]) -> None:
        root, _ = silver_data
        reader = HeberReader(root)
        df = reader.read_silver(
            "bars",
            time_range=(_ts(0), _ts(3)),
            instrument_keys=["equity:AAPL"],
            asof_time=_ts(1),
        )
        # AAPL rows where ts_event ∈ [0,3] and ts_available ≤ _ts(1) → only the first row
        assert len(df) == 1
        assert (df["instrument_key"] == "equity:AAPL").all()


class TestBatchedReading:
    """Tests for the batch_size parameter on read_silver."""

    def test_batched_returns_same_as_unbatched(self, silver_data: tuple[Path, pd.DataFrame]) -> None:
        root, _ = silver_data
        reader = HeberReader(root)
        df_normal = reader.read_silver("bars")
        df_batched = reader.read_silver("bars", batch_size=2)
        assert len(df_batched) == len(df_normal)
        # Same columns
        assert set(df_batched.columns) == set(df_normal.columns)

    def test_batched_with_filters(self, silver_data: tuple[Path, pd.DataFrame]) -> None:
        root, _ = silver_data
        reader = HeberReader(root)
        df = reader.read_silver(
            "bars",
            instrument_keys=["equity:AAPL"],
            batch_size=1,
        )
        assert len(df) == 2
        assert (df["instrument_key"] == "equity:AAPL").all()

    def test_batched_with_column_projection(self, silver_data: tuple[Path, pd.DataFrame]) -> None:
        root, _ = silver_data
        reader = HeberReader(root)
        df = reader.read_silver("bars", columns=["close"], batch_size=2)
        assert "close" in df.columns
        assert "ts_event" in df.columns  # essential column kept
        assert len(df) == 4

    def test_batched_empty_result(self, silver_data: tuple[Path, pd.DataFrame]) -> None:
        root, _ = silver_data
        reader = HeberReader(root)
        df = reader.read_silver(
            "bars",
            instrument_keys=["equity:NONEXISTENT"],
            batch_size=2,
        )
        assert df.empty

    def test_read_asof_passes_batch_size(self, silver_data: tuple[Path, pd.DataFrame]) -> None:
        root, _ = silver_data
        reader = HeberReader(root)
        df = reader.read_asof("bars", asof_time=_ts(2), batch_size=2)
        assert len(df) == 2


class TestMultiInstrumentAsofJoin:
    def test_groups_by_instrument(self) -> None:
        reader = HeberReader(Path("/tmp/unused"))
        left = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL", "equity:TSLA"],
                "ts_event": pd.to_datetime([_ts(2), _ts(2)]),
            }
        )
        right = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL", "equity:TSLA"],
                "ts_event": pd.to_datetime([_ts(0), _ts(0)]),
                "ts_available": pd.to_datetime([_ts(1), _ts(1)]),
                "feature": [10.0, 20.0],
            }
        )
        result = reader.asof_join(left, right)
        assert len(result) == 2
        aapl_row = result[result["instrument_key"] == "equity:AAPL"]
        tsla_row = result[result["instrument_key"] == "equity:TSLA"]
        assert aapl_row["feature"].iloc[0] == pytest.approx(10.0)
        assert tsla_row["feature"].iloc[0] == pytest.approx(20.0)
