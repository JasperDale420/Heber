"""Integration tests for the chunked fail-loud Massive ingestion driver."""

import asyncio
import gzip
from datetime import UTC, date, datetime

import pyarrow.parquet as pq
import pytest

from heber.backfill.massive.driver import MissingArchiveFileError, ingest
from heber.backfill.massive.parser import RejectCollector
from heber.backfill.massive.paths import archive_path

HEADER = "ticker,volume,open,close,high,low,window_start,transactions"
# 2024-01-02 00:00:00 UTC in nanoseconds since epoch.
WINDOW_START_2024_01_02 = 1704153600000000000


def _seed_day(archive_root, dataset, d: date, rows: list[str]):
    path = archive_path(archive_root, dataset, d)
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="") as fh:
        fh.write(HEADER + "\n")
        for row in rows:
            fh.write(row + "\n")


def test_ingest_single_trading_day_promotes_canonical(tmp_path):
    storage = str(tmp_path / "storage")
    archive = str(tmp_path / "archive")
    aapl_row = f"AAPL,1000,100.5,101.0,102.0,99.5,{WINDOW_START_2024_01_02},42"
    _seed_day(archive, "day_aggs_v1", date(2024, 1, 2), [aapl_row])

    result = asyncio.run(ingest("day_aggs_v1", date(2024, 1, 2), date(2024, 1, 2), storage, archive, RejectCollector()))

    canonical = tmp_path / "storage" / "silver" / "massive_bars" / "dt=2024-01-02" / "part-00000.parquet"
    assert canonical.exists()

    table = pq.ParquetFile(str(canonical)).read()
    assert table.num_rows == 1
    row = table.to_pylist()[0]
    assert row["instrument_key"] == "equity:AAPL"
    assert "backfill" in row["quality_flags"]
    assert row["ts_available"] == datetime(2024, 1, 3, 16, 0, tzinfo=UTC)

    # Temp backfill dir promoted away.
    part = tmp_path / "storage" / "silver" / "massive_bars" / "dt=2024-01-02"
    assert not list(part.glob("_backfill_*"))

    assert result.rows_written == 1


def test_ingest_missing_trading_day_file_raises(tmp_path):
    storage = str(tmp_path / "storage")
    archive = str(tmp_path / "archive")
    aapl_row = f"AAPL,1000,100.5,101.0,102.0,99.5,{WINDOW_START_2024_01_02},42"
    # Only 2024-01-02 present; 2024-01-03 is a trading day with no archived file.
    _seed_day(archive, "day_aggs_v1", date(2024, 1, 2), [aapl_row])

    with pytest.raises(MissingArchiveFileError):
        asyncio.run(ingest("day_aggs_v1", date(2024, 1, 2), date(2024, 1, 3), storage, archive, RejectCollector()))


def test_ingest_skips_non_trading_days(tmp_path):
    storage = str(tmp_path / "storage")
    archive = str(tmp_path / "archive")
    aapl_row = f"AAPL,1000,100.5,101.0,102.0,99.5,{WINDOW_START_2024_01_02},42"
    # Range Fri 2024-01-05 .. Mon 2024-01-08: only the two trading days have files.
    _seed_day(archive, "day_aggs_v1", date(2024, 1, 5), [aapl_row])
    _seed_day(archive, "day_aggs_v1", date(2024, 1, 8), [aapl_row])

    result = asyncio.run(ingest("day_aggs_v1", date(2024, 1, 5), date(2024, 1, 8), storage, archive, RejectCollector()))

    # Weekend 2024-01-06/07 require no files and must not raise.
    assert "2024-01-05" in result.progress_dates_completed
    assert "2024-01-08" in result.progress_dates_completed
    assert "2024-01-06" not in result.progress_dates_completed
    assert "2024-01-07" not in result.progress_dates_completed
