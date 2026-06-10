"""Tests for BackfillCoordinator.run_job_chunked (lifecycle-aware chunked ingestion path)."""

from datetime import UTC, date, datetime

import pytest

from heber.backfill import (
    BackfillCoordinator,
    BackfillJobDefinition,
    BackfillStatus,
    TsAvailablePolicy,
)


@pytest.mark.asyncio
async def test_run_job_chunked_completes_with_post_date_hook(tmp_path):
    storage_root = str(tmp_path / "heber")

    # One in-range trading session: 2024-01-02 (Tuesday).
    start = date(2024, 1, 2)
    end = date(2024, 1, 2)

    chunk_a = [
        {
            "instrument_key": "equity:AAPL",
            "symbol": "AAPL",
            "ts_event": datetime(2024, 1, 2, 14, 30, tzinfo=UTC),
            "open": 1.0,
        },
        {
            "instrument_key": "equity:MSFT",
            "symbol": "MSFT",
            "ts_event": datetime(2024, 1, 2, 14, 31, tzinfo=UTC),
            "open": 2.0,
        },
    ]
    chunk_b = [
        {
            "instrument_key": "equity:TSLA",
            "symbol": "TSLA",
            "ts_event": datetime(2024, 1, 2, 14, 32, tzinfo=UTC),
            "open": 3.0,
        },
    ]
    total_rows = len(chunk_a) + len(chunk_b)

    async def chunk_fetcher(*, date, symbols):
        yield list(chunk_a)
        yield list(chunk_b)

    hook_calls: list[date] = []

    def post_date_hook(d):
        hook_calls.append(d)

    coord = BackfillCoordinator(storage_root=storage_root)
    definition = BackfillJobDefinition(
        provider="massive",
        feed="bars",
        date_range_start=start,
        date_range_end=end,
        ts_available_policy=TsAvailablePolicy.CUSTOM,
    )
    job = coord.create_job(definition)

    result = await coord.run_job_chunked(
        job.backfill_id,
        definition,
        chunk_fetcher,
        post_date_hook=post_date_hook,
    )

    assert result.status == BackfillStatus.COMPLETED
    assert result.rows_written == total_rows
    assert result.started_at is not None
    assert result.completed_at is not None
    assert hook_calls == [date(2024, 1, 2)]
    assert "2024-01-02" in result.progress_dates_completed
