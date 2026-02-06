"""Regression tests for persisted backfill jobs across process restarts."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from heber.backfill import (
    BackfillCoordinator,
    BackfillJob,
    BackfillJobDefinition,
    BackfillStatus,
)


def _definition(start: date, end: date) -> BackfillJobDefinition:
    return BackfillJobDefinition(
        provider="alpaca",
        feed="bars",
        date_range_start=start,
        date_range_end=end,
        rate_limit_per_second=1_000_000.0,
    )


async def _fake_fetcher(**kwargs):  # noqa: ANN003
    chunk_date = kwargs["date"]
    return [
        {
            "instrument_key": "equity:AAPL",
            "symbol": "AAPL",
            "ts_event": datetime(chunk_date.year, chunk_date.month, chunk_date.day, 14, 30, tzinfo=UTC),
            "close": 100.0,
        }
    ]


async def _noop_catalog_updater(_job, _records, _chunk_date, _rows):  # noqa: ANN001
    return None


class _FailSecondBatchWriter:
    def __init__(self, **_kwargs):  # noqa: ANN003
        self.calls = 0

    def write_batch(self, _job, records, _chunk_date):  # noqa: ANN001
        self.calls += 1
        if self.calls == 2:
            raise RuntimeError("boom on second chunk")
        return len(records)


class _SuccessWriter:
    def __init__(self, **_kwargs):  # noqa: ANN003
        return None

    def write_batch(self, _job, records, _chunk_date):  # noqa: ANN001
        return len(records)


def test_create_job_persists_and_reloads_from_disk(tmp_path: Path) -> None:
    coordinator = BackfillCoordinator(storage_root=str(tmp_path))
    definition = _definition(date(2026, 1, 2), date(2026, 1, 2))

    created = coordinator.create_job(definition)

    state_path = tmp_path / "backfill" / "jobs" / f"{created.backfill_id}.json"
    assert state_path.exists()

    reloaded = BackfillCoordinator(storage_root=str(tmp_path))
    loaded = reloaded.get_job(created.backfill_id)

    assert loaded is not None
    assert loaded.backfill_id == created.backfill_id
    assert loaded.status == BackfillStatus.PENDING
    assert loaded.progress_dates_completed == []


@pytest.mark.asyncio
async def test_failed_job_resume_continues_from_persisted_progress(tmp_path: Path) -> None:
    definition = _definition(date(2026, 1, 2), date(2026, 1, 3))

    failing = BackfillCoordinator(
        storage_root=str(tmp_path),
        data_fetcher=_fake_fetcher,
        writer_factory=_FailSecondBatchWriter,
        catalog_metadata_updater=_noop_catalog_updater,
    )
    job = failing.create_job(definition)

    with pytest.raises(RuntimeError, match="second chunk"):
        await failing.run_job(job.backfill_id, definition)

    after_failure = BackfillCoordinator(storage_root=str(tmp_path))
    failed_job = after_failure.get_job(job.backfill_id)
    assert failed_job is not None
    assert failed_job.status == BackfillStatus.FAILED
    assert failed_job.progress_dates_completed == ["2026-01-02"]
    assert failed_job.rows_written == 1

    resumed = BackfillCoordinator(
        storage_root=str(tmp_path),
        data_fetcher=_fake_fetcher,
        writer_factory=_SuccessWriter,
        catalog_metadata_updater=_noop_catalog_updater,
    )
    completed = await resumed.run_job(job.backfill_id, definition)

    assert completed.status == BackfillStatus.COMPLETED
    assert completed.progress_dates_completed == ["2026-01-02", "2026-01-03"]
    assert completed.rows_written == 2


def test_reloading_running_job_marks_it_pending_for_resume(tmp_path: Path) -> None:
    definition = _definition(date(2026, 1, 2), date(2026, 1, 2))
    job = BackfillJob.from_definition(definition)
    job.status = BackfillStatus.RUNNING
    job.started_at = datetime(2026, 1, 2, 12, 0, tzinfo=UTC)

    state_dir = tmp_path / "backfill" / "jobs"
    state_dir.mkdir(parents=True, exist_ok=True)
    state_path = state_dir / f"{job.backfill_id}.json"
    state_path.write_text(json.dumps(job.to_dict(), separators=(",", ":")), encoding="utf-8")

    reloaded = BackfillCoordinator(storage_root=str(tmp_path))
    recovered = reloaded.get_job(job.backfill_id)

    assert recovered is not None
    assert recovered.status == BackfillStatus.PENDING
    assert recovered.error_message == "Recovered after restart; resume required."
