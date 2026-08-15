"""The coverage scan must cost what changed, not what exists.

`feed=quotes` holds 825,107 Parquet files across 4,445 directories, because the
compactor is occupied elsewhere and never reaches it. Reading every footer on
every pass made that one feed a projected 4.16h of a 4.33h pass — parallelism
cut it roughly in half but cannot fix re-reading 825k unchanged files every
five minutes.

A partition whose directory tree has not been modified since it was last
counted still holds the same files, so its recorded count is still correct.
Nothing in Heber rewrites a Parquet file in place: Silver writes new part
files, and the compactor writes a temp file and renames. Both change the
containing directory's mtime, so the directory is a sound change signal.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.catalog import seeds
from heber.catalog.seeds import _MTIME_SKEW_MARGIN_SECONDS


def _write_parquet(path: Path, num_rows: int) -> None:
    table = pa.table({"x": list(range(num_rows))})
    pq.write_table(table, path)


@pytest.fixture
def feed_dir(tmp_path: Path) -> Path:
    part = tmp_path / "feed=quotes" / "instrument_type=equity" / "dt=2026-08-01"
    part.mkdir(parents=True)
    _write_parquet(part / "part.parquet", 100)
    return tmp_path / "feed=quotes"


@pytest.fixture
def footer_reads(monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    """Record every file whose footer is actually opened."""
    seen: list[Path] = []
    real = seeds._read_row_count

    def _spy(path: Path):
        seen.append(path)
        return real(path)

    monkeypatch.setattr(seeds, "_read_row_count", _spy)
    return seen


def test_unchanged_partition_is_not_reread(feed_dir: Path, footer_reads: list[Path]) -> None:
    recorded = {"2026-08-01": (datetime.now(UTC) + timedelta(hours=1), 100)}

    result = seeds._scan_partition_dates(feed_dir, recorded)

    assert result == [("2026-08-01", 100)], "recorded count must be carried forward"
    assert footer_reads == [], f"unchanged partition was re-read: {footer_reads}"


def test_partition_modified_since_the_last_count_is_reread(feed_dir: Path, footer_reads: list[Path]) -> None:
    """A backfill writing into an old date must not be skipped."""
    recorded = {"2026-08-01": (datetime.now(UTC) - timedelta(days=30), 7)}

    result = seeds._scan_partition_dates(feed_dir, recorded)

    assert result == [("2026-08-01", 100)], "stale recorded count must be replaced by a fresh read"
    assert len(footer_reads) == 1


def test_a_date_never_recorded_is_always_read(feed_dir: Path, footer_reads: list[Path]) -> None:
    result = seeds._scan_partition_dates(feed_dir, {})

    assert result == [("2026-08-01", 100)]
    assert len(footer_reads) == 1


def test_new_file_in_an_otherwise_unchanged_date_is_picked_up(feed_dir: Path, footer_reads: list[Path]) -> None:
    """Adding a part file changes the directory mtime, so the date is re-read."""
    recorded_at = datetime.now(UTC) - timedelta(seconds=1)
    _write_parquet(feed_dir / "instrument_type=equity" / "dt=2026-08-01" / "part-new.parquet", 5)

    result = seeds._scan_partition_dates(feed_dir, {"2026-08-01": (recorded_at, 100)})

    assert result == [("2026-08-01", 105)]
    assert len(footer_reads) == 2


def test_a_partition_touched_around_the_recorded_time_is_reread(feed_dir: Path, footer_reads: list[Path]) -> None:
    """Within the skew margin, re-read rather than trust the record.

    Lakehouse mtimes were measured ~0.5s ahead of the container clock. A file
    landing in the same moment its directory was walked must not be able to
    read as older than the pass that missed it, because that would freeze the
    short count permanently.
    """
    just_after = datetime.now(UTC) + timedelta(seconds=_MTIME_SKEW_MARGIN_SECONDS - 1)

    result = seeds._scan_partition_dates(feed_dir, {"2026-08-01": (just_after, 7)})

    assert result == [("2026-08-01", 100)], "a partition inside the skew margin must be re-read"
    assert len(footer_reads) == 1


def test_change_inside_an_hour_subpartition_is_picked_up(tmp_path: Path, footer_reads: list[Path]) -> None:
    """quotes is hour-partitioned; a new file lands in hour=, not in dt=."""
    hour = tmp_path / "feed=quotes" / "instrument_type=equity" / "dt=2026-08-01" / "hour=14"
    hour.mkdir(parents=True)
    _write_parquet(hour / "part.parquet", 42)
    recorded_at = datetime.now(UTC) - timedelta(seconds=1)

    result = seeds._scan_partition_dates(tmp_path / "feed=quotes", {"2026-08-01": (recorded_at, 999)})

    assert result == [("2026-08-01", 42)], "an hour= subdirectory's mtime must count as the date changing"
    assert len(footer_reads) == 1


def test_naive_recorded_timestamp_is_treated_as_utc(feed_dir: Path, footer_reads: list[Path]) -> None:
    """Postgres returns tz-aware values, but a naive one must not crash the pass."""
    naive_future = (datetime.now(UTC) + timedelta(hours=1)).replace(tzinfo=None)

    result = seeds._scan_partition_dates(feed_dir, {"2026-08-01": (naive_future, 100)})

    assert result == [("2026-08-01", 100)]
    assert footer_reads == []


async def test_first_pass_of_a_process_recounts_everything(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, footer_reads: list[Path]
) -> None:
    """``reuse_recorded=False`` must ignore recorded counts entirely.

    Rows written before start-stamping existed were stamped when they were
    *written*, at the end of a pass that walked for hours — so that stamp sits
    long after the files it counted. Reusing one would permanently skip any
    partition whose last write landed mid-pass, an undercount nothing heals
    because no future write bumps the mtime. The first pass of a process
    therefore trusts nothing.
    """
    part = tmp_path / "silver" / "feed=alpha" / "instrument_type=equity" / "dt=2026-08-01"
    part.mkdir(parents=True)
    _write_parquet(part / "part.parquet", 10)
    monkeypatch.setattr(seeds.settings, "data_root", tmp_path)

    loaded: list[str] = []

    async def _never_called(_session):
        loaded.append("loaded")
        return {}

    monkeypatch.setattr(seeds, "_load_recorded_coverage", _never_called)

    session = MagicMock()
    session.add = MagicMock()

    async def _execute(*_a, **_k):
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        result.all.return_value = []
        return result

    session.execute = AsyncMock(side_effect=_execute)
    session.commit = AsyncMock()
    session.rollback = AsyncMock()

    await seeds.seed_coverage_from_disk(session, reuse_recorded=False)

    assert loaded == [], "the first pass must not consult recorded coverage at all"
    assert len(footer_reads) == 1, "every partition must be counted from its footers"


async def test_recorded_coverage_ignores_non_date_instrument_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    """Only ``dt:`` rows may seed the skip decision.

    Backfill and ``CatalogService.update_coverage`` write coverage rows keyed by
    real instrument keys, incrementally, still stamped at write time. Those must
    never reach the reuse path, or a write-time stamp could freeze a date short
    through a side door.
    """
    rows = [
        ("quotes", "dt:2026-08-01", datetime.now(UTC), 100),
        ("quotes", "__all__", datetime.now(UTC), 500),
        ("quotes", "equity:AAPL", datetime.now(UTC), 42),
    ]

    session = MagicMock()

    async def _execute(*_a, **_k):
        result = MagicMock()
        result.all.return_value = rows
        return result

    session.execute = AsyncMock(side_effect=_execute)
    session.rollback = AsyncMock()

    recorded = await seeds._load_recorded_coverage(session)

    assert recorded == {"quotes": {"2026-08-01": (rows[0][2], 100)}}


async def test_rows_are_stamped_with_the_scan_start_not_the_write_time(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The recorded timestamp must predate every footer this pass read.

    Coverage rows are written at the end of a pass that takes minutes. Stamping
    them with the write time would mark a partition as "counted at" a moment
    after files that landed mid-pass were already on disk, so the next pass
    would see an older mtime, skip the partition, and keep the short count
    permanently. Stamping with the pass's start time cannot go stale that way.
    """
    part = tmp_path / "silver" / "feed=alpha" / "instrument_type=equity" / "dt=2026-08-01"
    part.mkdir(parents=True)
    _write_parquet(part / "part.parquet", 10)
    monkeypatch.setattr(seeds.settings, "data_root", tmp_path)

    added: list[Any] = []
    session = MagicMock()
    session.add = MagicMock(side_effect=added.append)

    async def _execute(*_a, **_k):
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        result.all.return_value = []
        return result

    session.execute = AsyncMock(side_effect=_execute)
    session.commit = AsyncMock()
    session.rollback = AsyncMock()

    before = datetime.now(UTC)
    await seeds.seed_coverage_from_disk(session)
    after = datetime.now(UTC)

    stamps = [obj.last_updated_ts for obj in added]
    assert stamps, "no coverage rows were written"
    assert all(before <= s <= after for s in stamps), f"stamps outside the pass window: {stamps}"
    assert len(set(stamps)) == 1, "every row in a pass must share the pass's start time"
