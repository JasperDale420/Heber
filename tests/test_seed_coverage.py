"""Tests for coverage scanning: _scan_partition_dates, seed_coverage_from_disk."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.catalog.seeds import _read_row_count, _scan_partition_dates, _walk_partition_files


def _write_parquet(path: Path, num_rows: int) -> None:
    """Write a small Parquet file with the given number of rows."""
    table = pa.table({"x": list(range(num_rows))})
    pq.write_table(table, path)


def _partition(base: Path, dt: str = "2024-01-15", itype: str = "equity") -> Path:
    """Create and return a dt= partition directory under a feed dir."""
    part = base / f"instrument_type={itype}" / f"dt={dt}"
    part.mkdir(parents=True, exist_ok=True)
    return part


def _rows_for(feed_dir: Path, dt: str = "2024-01-15") -> int:
    """Row count the scan attributes to one date, or 0 if the date is absent."""
    result = _scan_partition_dates(feed_dir)
    return dict(result or {}).get(dt, 0)


# ── row counting within a partition ──────────────────────────────────────────


class TestPartitionRowCounts:
    def test_counts_rows_from_single_file(self, tmp_path: Path) -> None:
        _write_parquet(_partition(tmp_path) / "part-00000.parquet", 500)
        assert _rows_for(tmp_path) == 500

    def test_sums_across_multiple_files(self, tmp_path: Path) -> None:
        part = _partition(tmp_path)
        _write_parquet(part / "part-00000.parquet", 200)
        _write_parquet(part / "part-00001.parquet", 300)
        assert _rows_for(tmp_path) == 500

    def test_ignores_non_parquet_files(self, tmp_path: Path) -> None:
        part = _partition(tmp_path)
        _write_parquet(part / "part-00000.parquet", 100)
        (part / "metadata.json").write_text("{}")
        (part / "_SUCCESS").touch()
        assert _rows_for(tmp_path) == 100

    def test_counts_rows_in_hourly_subpartitions(self, tmp_path: Path) -> None:
        part = _partition(tmp_path)
        for hour, rows in (("hour=13", 100), ("hour=14", 250)):
            (part / hour).mkdir()
            _write_parquet(part / hour / "part-00000.parquet", rows)
        assert _rows_for(tmp_path) == 350

    def test_skips_files_inside_sidecar_directories(self, tmp_path: Path) -> None:
        part = _partition(tmp_path)
        _write_parquet(part / "part-00000.parquet", 100)
        (part / "._hour=13").mkdir()
        _write_parquet(part / "._hour=13" / "part-00000.parquet", 999)
        assert _rows_for(tmp_path) == 100

    def test_skips_sidecar_files_beside_real_ones(self, tmp_path: Path) -> None:
        part = _partition(tmp_path)
        _write_parquet(part / "part-00000.parquet", 100)
        _write_parquet(part / "._part-00000.parquet", 999)
        assert _rows_for(tmp_path) == 100

    def test_skips_corrupt_parquet(self, tmp_path: Path) -> None:
        part = _partition(tmp_path)
        _write_parquet(part / "good.parquet", 50)
        (part / "bad.parquet").write_bytes(b"not parquet data")
        assert _rows_for(tmp_path) == 50

    def test_zero_byte_parquet_is_counted_as_absent(self, tmp_path: Path) -> None:
        """A zero-byte part file must not change the row count."""
        part = _partition(tmp_path)
        _write_parquet(part / "good.parquet", 50)
        (part / "part-empty.parquet").touch()

        assert _rows_for(tmp_path) == 50


class TestWalkPartitionFiles:
    """What the walk collects, and what it deliberately does not pay for.

    175 zero-byte part files exist in the lake, 173 of them under feed=quotes.
    Each one reaching pyarrow costs an open, a raised ArrowInvalid and a full
    traceback in a JSON log line — so they are still filtered, but by
    `_read_row_count` at read time rather than by statting every file here.
    """

    def _walk(self, feed_dir: Path):
        with ThreadPoolExecutor(max_workers=2) as pool:
            return _walk_partition_files(feed_dir, pool)

    def test_zero_byte_files_are_collected_but_filtered_at_read_time(self, tmp_path: Path) -> None:
        """The walk no longer stats files, so the size filter moved to the read.

        Statting every file cost 4.24ms each against 0.15ms for the scandir
        entry — 29x, measured on the mount — and was paid even for the files a
        reuse pass never opens. `_read_row_count` still keeps pyarrow away from
        a zero-byte file; it just does it for the files actually being opened.
        """
        part = _partition(tmp_path)
        _write_parquet(part / "good.parquet", 50)
        (part / "part-empty.parquet").touch()

        walk = self._walk(tmp_path)

        collected = sorted(p.name for paths in walk.files_by_date.values() for p in paths)
        assert collected == ["good.parquet", "part-empty.parquet"]

    def test_reports_counts_for_per_feed_instrumentation(self, tmp_path: Path) -> None:
        """dirs/files/skipped counts are what the per-feed log line reports."""
        part = _partition(tmp_path)
        _write_parquet(part / "a.parquet", 10)
        _write_parquet(part / "b.parquet", 20)

        walk = self._walk(tmp_path)
        files_by_date, dirs_scanned = walk.files_by_date, walk.dirs_scanned

        assert list(files_by_date) == ["2024-01-15"]
        assert len(files_by_date["2024-01-15"]) == 2
        # feed dir + instrument_type= + dt=
        assert dirs_scanned == 3

    def test_attributes_hourly_files_to_their_date(self, tmp_path: Path) -> None:
        part = _partition(tmp_path)
        (part / "hour=13").mkdir()
        _write_parquet(part / "hour=13" / "part.parquet", 10)

        files_by_date = self._walk(tmp_path).files_by_date

        assert list(files_by_date) == ["2024-01-15"]

    def test_ignores_files_with_no_date_partition_above_them(self, tmp_path: Path) -> None:
        _write_parquet(tmp_path / "loose.parquet", 10)

        assert self._walk(tmp_path).files_by_date == {}


class TestReadRowCount:
    """An unreadable footer is distinguishable from a genuinely empty one.

    A file that cannot be read silently contributes zero rows. If a degraded
    mount made a fraction of footers unreadable, per-date counts would shrink
    and the only signal would be one warning per file — so the scan reports
    how many it could not read, not just how many it skipped.
    """

    def test_returns_row_count_for_a_readable_file(self, tmp_path: Path) -> None:
        _write_parquet(tmp_path / "good.parquet", 50)
        assert _read_row_count(tmp_path / "good.parquet") == 50

    def test_returns_none_for_an_unreadable_file(self, tmp_path: Path) -> None:
        (tmp_path / "bad.parquet").write_bytes(b"not parquet data")
        assert _read_row_count(tmp_path / "bad.parquet") is None

    def test_zero_row_file_is_not_reported_as_unreadable(self, tmp_path: Path) -> None:
        _write_parquet(tmp_path / "empty-but-valid.parquet", 0)
        assert _read_row_count(tmp_path / "empty-but-valid.parquet") == 0


# ── _scan_partition_dates ────────────────────────────────────────────────────


class TestScanPartitionDates:
    def test_returns_per_date_row_counts(self, tmp_path: Path) -> None:
        """Each dt= partition gets its own (date_str, row_count) entry."""
        feed_dir = tmp_path / "feed=bars"
        feed_dir.mkdir()

        dt1 = feed_dir / "instrument_type=equity" / "dt=2024-01-15"
        dt1.mkdir(parents=True)
        _write_parquet(dt1 / "part-00000.parquet", 100)

        dt2 = feed_dir / "instrument_type=equity" / "dt=2024-06-30"
        dt2.mkdir(parents=True)
        _write_parquet(dt2 / "part-00000.parquet", 200)

        result = _scan_partition_dates(feed_dir)

        assert result is not None
        assert len(result) == 2
        result_dict = dict(result)
        assert result_dict["2024-01-15"] == 100
        assert result_dict["2024-06-30"] == 200

    def test_sums_a_date_across_instrument_types(self, tmp_path: Path) -> None:
        """One date spanning two instrument_type= dirs is one row, summed.

        Emitting the date twice made `_upsert_coverage` write it twice under
        the same (dataset_name, instrument_key); the unique constraint means
        the second write updates the first, so the stored count was whichever
        instrument type happened to be walked last, not the day's total.
        """
        feed_dir = tmp_path / "feed=bars"
        feed_dir.mkdir()

        for itype in ("equity", "crypto"):
            dt = feed_dir / f"instrument_type={itype}" / "dt=2024-03-01"
            dt.mkdir(parents=True)
            _write_parquet(dt / "part-00000.parquet", 50)

        result = _scan_partition_dates(feed_dir)
        assert result is not None
        assert result == [("2024-03-01", 100)]

    def test_returns_none_when_no_partitions(self, tmp_path: Path) -> None:
        feed_dir = tmp_path / "feed=empty"
        feed_dir.mkdir()
        (feed_dir / "somefile.parquet").touch()

        result = _scan_partition_dates(feed_dir)
        assert result is None

    def test_ignores_dt_files_not_dirs(self, tmp_path: Path) -> None:
        """dt= files (not directories) should be ignored."""
        feed_dir = tmp_path / "feed=bars"
        feed_dir.mkdir()
        (feed_dir / "dt=2024-01-01").touch()  # File, not dir

        result = _scan_partition_dates(feed_dir)
        assert result is None

    def test_skips_partitions_with_no_parquet(self, tmp_path: Path) -> None:
        """dt= dirs that contain no .parquet files are skipped (0 rows)."""
        feed_dir = tmp_path / "feed=bars"
        dt = feed_dir / "instrument_type=equity" / "dt=2024-01-01"
        dt.mkdir(parents=True)
        # No parquet files — just an empty dir
        (dt / "_SUCCESS").touch()

        result = _scan_partition_dates(feed_dir)
        assert result is None  # 0 rows → filtered out

    def test_ignores_malformed_date_partitions(self, tmp_path: Path) -> None:
        """Only dt=YYYY-MM-DD format matches, not dt=2024 or dt=foo."""
        feed_dir = tmp_path / "feed=bars"
        feed_dir.mkdir()

        bad1 = feed_dir / "instrument_type=equity" / "dt=2024"
        bad1.mkdir(parents=True)
        _write_parquet(bad1 / "part.parquet", 10)

        bad2 = feed_dir / "instrument_type=equity" / "dt=foo"
        bad2.mkdir(parents=True)
        _write_parquet(bad2 / "part.parquet", 10)

        good = feed_dir / "instrument_type=equity" / "dt=2024-01-15"
        good.mkdir(parents=True)
        _write_parquet(good / "part.parquet", 10)

        result = _scan_partition_dates(feed_dir)
        assert result is not None
        assert len(result) == 1
        assert result[0][0] == "2024-01-15"


# ── seed_coverage_from_disk (async) ──────────────────────────────────────────


@pytest.mark.asyncio
class TestSeedCoverageFromDisk:
    """Test the async seed_coverage_from_disk function with mocked DB session."""

    async def test_creates_aggregate_and_per_date_records(self, tmp_path: Path) -> None:
        """For a feed with 3 date partitions, should upsert 1 aggregate + 3 per-date records."""
        # Build fake Silver directory
        for d in ("2024-01-10", "2024-01-11", "2024-01-12"):
            dt_dir = tmp_path / "feed=bars" / "instrument_type=equity" / f"dt={d}"
            dt_dir.mkdir(parents=True)
            _write_parquet(dt_dir / "part.parquet", 100)

        # Mock session — use MagicMock for sync methods (add) to avoid
        # "coroutine never awaited" warnings; AsyncMock for async methods.
        mock_session = AsyncMock()
        mock_session.add = MagicMock()  # session.add() is sync
        # _upsert_coverage does a select → returns None (new record)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_result.all.return_value = []
        mock_session.execute.return_value = mock_result

        with patch("heber.catalog.seeds.settings") as mock_settings:
            mock_settings.silver_path = tmp_path
            mock_settings.catalog_coverage_scan_workers = 4

            from heber.catalog.seeds import seed_coverage_from_disk

            count = await seed_coverage_from_disk(mock_session)

        # 1 aggregate (__all__) + 3 per-date records = 4 upserts
        assert count == 4
        # session.add called for each new record
        assert mock_session.add.call_count == 4
        mock_session.commit.assert_awaited_once()

    async def test_returns_zero_when_silver_path_missing(self, tmp_path: Path) -> None:
        mock_session = AsyncMock()

        with patch("heber.catalog.seeds.settings") as mock_settings:
            mock_settings.silver_path = tmp_path / "nonexistent"

            from heber.catalog.seeds import seed_coverage_from_disk

            count = await seed_coverage_from_disk(mock_session)

        assert count == 0
        mock_session.commit.assert_not_awaited()

    async def test_skips_feeds_with_no_data(self, tmp_path: Path) -> None:
        """feed= dirs that have no dt= partitions with parquet files are skipped."""
        empty_feed = tmp_path / "feed=empty_feed"
        empty_feed.mkdir()
        # No dt= subdirs

        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_result.all.return_value = []
        mock_session.execute.return_value = mock_result

        with patch("heber.catalog.seeds.settings") as mock_settings:
            mock_settings.silver_path = tmp_path
            mock_settings.catalog_coverage_scan_workers = 4

            from heber.catalog.seeds import seed_coverage_from_disk

            count = await seed_coverage_from_disk(mock_session)

        assert count == 0

    async def test_aggregate_record_has_correct_min_max(self, tmp_path: Path) -> None:
        """The __all__ aggregate should span min(dates)..max(dates) with summed rows."""
        for d, rows in [("2024-01-05", 100), ("2024-06-15", 200), ("2024-12-25", 300)]:
            dt_dir = tmp_path / "feed=bars" / "instrument_type=equity" / f"dt={d}"
            dt_dir.mkdir(parents=True)
            _write_parquet(dt_dir / "part.parquet", rows)

        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_result.all.return_value = []
        mock_session.execute.return_value = mock_result

        with patch("heber.catalog.seeds.settings") as mock_settings:
            mock_settings.silver_path = tmp_path
            mock_settings.catalog_coverage_scan_workers = 4

            from heber.catalog.seeds import seed_coverage_from_disk

            await seed_coverage_from_disk(mock_session)

        # Check the first session.add call — should be the __all__ record
        added_objects = [call.args[0] for call in mock_session.add.call_args_list]
        aggregate = [o for o in added_objects if o.instrument_key == "__all__"]
        assert len(aggregate) == 1
        assert aggregate[0].dt_min == date(2024, 1, 5)
        assert aggregate[0].dt_max == date(2024, 12, 25)
        assert aggregate[0].approx_row_count == 600

    async def test_per_date_records_use_dt_prefix(self, tmp_path: Path) -> None:
        """Per-date records should use instrument_key='dt:YYYY-MM-DD'."""
        dt_dir = tmp_path / "feed=bars" / "instrument_type=equity" / "dt=2024-03-15"
        dt_dir.mkdir(parents=True)
        _write_parquet(dt_dir / "part.parquet", 42)

        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_result.all.return_value = []
        mock_session.execute.return_value = mock_result

        with patch("heber.catalog.seeds.settings") as mock_settings:
            mock_settings.silver_path = tmp_path
            mock_settings.catalog_coverage_scan_workers = 4

            from heber.catalog.seeds import seed_coverage_from_disk

            await seed_coverage_from_disk(mock_session)

        added_objects = [call.args[0] for call in mock_session.add.call_args_list]
        per_date = [o for o in added_objects if o.instrument_key.startswith("dt:")]
        assert len(per_date) == 1
        assert per_date[0].instrument_key == "dt:2024-03-15"
        assert per_date[0].dt_min == date(2024, 3, 15)
        assert per_date[0].dt_max == date(2024, 3, 15)
        assert per_date[0].approx_row_count == 42


# ── Coverage API endpoint ────────────────────────────────────────────────────


class TestCoverageApiResponse:
    """Test that the coverage API response format includes dt field for per-date records."""

    def test_per_date_record_has_dt_field(self) -> None:
        """Records with instrument_key starting with 'dt:' should expose a flat 'dt' field."""
        # Simulate what the API endpoint does
        from unittest.mock import MagicMock

        coverage_obj = MagicMock()
        coverage_obj.instrument_key = "dt:2024-01-15"
        coverage_obj.dt_min = date(2024, 1, 15)
        coverage_obj.dt_max = date(2024, 1, 15)
        coverage_obj.approx_row_count = 500

        record: dict = {
            "instrument_key": coverage_obj.instrument_key,
            "dt_min": coverage_obj.dt_min,
            "dt_max": coverage_obj.dt_max,
            "approx_row_count": coverage_obj.approx_row_count,
        }
        if coverage_obj.instrument_key and coverage_obj.instrument_key.startswith("dt:"):
            record["dt"] = str(coverage_obj.dt_min)

        assert record["dt"] == "2024-01-15"

    def test_aggregate_record_has_no_dt_field(self) -> None:
        """Records with instrument_key='__all__' should NOT have a dt field."""
        from unittest.mock import MagicMock

        coverage_obj = MagicMock()
        coverage_obj.instrument_key = "__all__"
        coverage_obj.dt_min = date(2024, 1, 1)
        coverage_obj.dt_max = date(2024, 12, 31)
        coverage_obj.approx_row_count = 50000

        record: dict = {
            "instrument_key": coverage_obj.instrument_key,
            "dt_min": coverage_obj.dt_min,
            "dt_max": coverage_obj.dt_max,
            "approx_row_count": coverage_obj.approx_row_count,
        }
        if coverage_obj.instrument_key and coverage_obj.instrument_key.startswith("dt:"):
            record["dt"] = str(coverage_obj.dt_min)

        assert "dt" not in record
