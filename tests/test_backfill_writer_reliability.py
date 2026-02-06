"""Regression tests for backfill write-path reliability."""

from __future__ import annotations

import builtins
import gzip
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from heber.backfill import BackfillCoordinator, BackfillJob, BackfillJobDefinition, BackfillStatus, BackfillWriter


def _new_job() -> BackfillJob:
    definition = BackfillJobDefinition(
        provider="alpaca",
        feed="bars",
        date_range_start=date(2026, 1, 2),
        date_range_end=date(2026, 1, 2),
    )
    return BackfillJob.from_definition(definition)


def test_write_batch_writes_bronze_and_silver_temp_partitions(tmp_path: Path) -> None:
    job = _new_job()

    def fake_parquet_writer(_records: list[dict], output_file: Path) -> None:
        output_file.write_text("parquet", encoding="utf-8")

    writer = BackfillWriter(storage_root=str(tmp_path), parquet_writer=fake_parquet_writer)

    rows = writer.write_batch(
        job,
        [
            {
                "instrument_key": "equity:AAPL",
                "ts_event": datetime(2026, 1, 2, 14, 30, tzinfo=UTC),
                "open": 100.0,
            }
        ],
        chunk_date=date(2026, 1, 2),
    )

    assert rows == 1

    bronze_files = list((tmp_path / "bronze").glob("provider=alpaca/feed=bars/dt=2026-01-02/hour=14/events-*.jsonl.gz"))
    assert len(bronze_files) == 1

    with gzip.open(bronze_files[0], "rt", encoding="utf-8") as handle:
        payload = handle.read()
    assert "backfill_id" in payload
    assert "quality_flags" in payload

    silver_files = list((tmp_path / "silver").glob("alpaca_bars/dt=2026-01-02/*/*.parquet"))
    assert len(silver_files) == 1


def test_write_parquet_raises_when_pyarrow_is_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    writer = BackfillWriter(storage_root=str(tmp_path))

    original_import = builtins.__import__

    def fake_import(name, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        if name.startswith("pyarrow"):
            raise ImportError("pyarrow unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(RuntimeError, match="pyarrow"):
        writer._write_parquet([{"a": 1}], tmp_path / "silver" / "test")


@pytest.mark.asyncio
async def test_run_job_invokes_catalog_metadata_updater(tmp_path: Path) -> None:
    calls: list[tuple[str, date, int]] = []

    async def fake_fetcher(**_kwargs):  # noqa: ANN003
        return [
            {
                "instrument_key": "equity:AAPL",
                "ts_event": datetime(2026, 1, 2, 14, 30, tzinfo=UTC),
                "close": 101.5,
            }
        ]

    def writer_factory(**kwargs) -> BackfillWriter:  # noqa: ANN003
        kwargs["parquet_writer"] = lambda _records, output_file: output_file.write_text("parquet", encoding="utf-8")
        return BackfillWriter(**kwargs)

    async def fake_catalog_updater(job: BackfillJob, records: list[dict], chunk_date: date, rows_written: int) -> None:
        calls.append((job.backfill_id, chunk_date, rows_written))
        assert len(records) == 1

    coordinator = BackfillCoordinator(
        storage_root=str(tmp_path),
        data_fetcher=fake_fetcher,
        writer_factory=writer_factory,
        catalog_metadata_updater=fake_catalog_updater,
    )

    definition = BackfillJobDefinition(
        provider="alpaca",
        feed="bars",
        date_range_start=date(2026, 1, 2),
        date_range_end=date(2026, 1, 2),
        rate_limit_per_second=1_000_000.0,
    )
    job = coordinator.create_job(definition)

    completed = await coordinator.run_job(job.backfill_id, definition)

    assert completed.status == BackfillStatus.COMPLETED
    assert completed.rows_written == 1
    assert len(calls) == 1
    assert calls[0][0] == job.backfill_id
    assert calls[0][1] == date(2026, 1, 2)
    assert calls[0][2] == 1
