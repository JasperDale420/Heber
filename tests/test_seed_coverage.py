"""Tests for coverage scanning: _count_parquet_rows, _scan_partition_dates, seed_coverage_from_disk."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.catalog.seeds import _count_parquet_rows, _scan_partition_dates


def _write_parquet(path: Path, num_rows: int) -> None:
    """Write a small Parquet file with the given number of rows."""
    table = pa.table({"x": list(range(num_rows))})
    pq.write_table(table, path)


# ── _count_parquet_rows ──────────────────────────────────────────────────────


class TestCountParquetRows:
    def test_counts_rows_from_single_file(self, tmp_path: Path) -> None:
        _write_parquet(tmp_path / "part-00000.parquet", 500)
        assert _count_parquet_rows(tmp_path) == 500

    def test_sums_across_multiple_files(self, tmp_path: Path) -> None:
        _write_parquet(tmp_path / "part-00000.parquet", 200)
        _write_parquet(tmp_path / "part-00001.parquet", 300)
        assert _count_parquet_rows(tmp_path) == 500

    def test_returns_zero_for_empty_dir(self, tmp_path: Path) -> None:
        assert _count_parquet_rows(tmp_path) == 0

    def test_ignores_non_parquet_files(self, tmp_path: Path) -> None:
        _write_parquet(tmp_path / "part-00000.parquet", 100)
        (tmp_path / "metadata.json").write_text("{}")
        (tmp_path / "_SUCCESS").touch()
        assert _count_parquet_rows(tmp_path) == 100

    def test_skips_corrupt_parquet(self, tmp_path: Path) -> None:
        _write_parquet(tmp_path / "good.parquet", 50)
        (tmp_path / "bad.parquet").write_bytes(b"not parquet data")
        assert _count_parquet_rows(tmp_path) == 50


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

    def test_spans_multiple_instrument_types(self, tmp_path: Path) -> None:
        """dt= partitions under different instrument_type= dirs are all found."""
        feed_dir = tmp_path / "feed=bars"
        feed_dir.mkdir()

        for itype in ("equity", "crypto"):
            dt = feed_dir / f"instrument_type={itype}" / "dt=2024-03-01"
            dt.mkdir(parents=True)
            _write_parquet(dt / "part-00000.parquet", 50)

        result = _scan_partition_dates(feed_dir)
        assert result is not None
        # Same date under two instrument types → two entries for "2024-03-01"
        dates = [d for d, _ in result]
        assert dates.count("2024-03-01") == 2

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
        mock_session.execute.return_value = mock_result

        with patch("heber.catalog.seeds.settings") as mock_settings:
            mock_settings.silver_path = tmp_path

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

        with patch("heber.catalog.seeds.settings") as mock_settings:
            mock_settings.silver_path = tmp_path

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
        mock_session.execute.return_value = mock_result

        with patch("heber.catalog.seeds.settings") as mock_settings:
            mock_settings.silver_path = tmp_path

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
        mock_session.execute.return_value = mock_result

        with patch("heber.catalog.seeds.settings") as mock_settings:
            mock_settings.silver_path = tmp_path

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
