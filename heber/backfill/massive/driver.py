"""Chunked, fail-loud ingestion driver over trading days -> Heber Silver via the coordinator."""

import csv
import gzip
from collections.abc import AsyncIterator
from datetime import date
from pathlib import Path
from typing import Any

from heber.backfill import (
    BackfillCoordinator,
    BackfillJob,
    BackfillJobDefinition,
    TsAvailablePolicy,
)
from heber.backfill.massive.parser import (
    FEED,
    PROVIDER,
    TIMEFRAME_BY_DATASET,
    RejectCollector,
    iter_record_chunks,
    validate_header,
)
from heber.backfill.massive.paths import archive_path
from heber.backfill.massive.promote import promote_date
from heber.backfill.massive.writer import massive_writer_factory

DATASET_DIR = f"{PROVIDER}_{FEED}"  # "massive_bars"
MAX_REJECT_RATE = 0.005


class MissingArchiveFileError(RuntimeError):
    """An expected trading-day file is absent from the archive."""


async def ingest(
    dataset: str,
    start: date,
    end: date,
    storage_root: str,
    archive_root: str,
    rejects: RejectCollector,
    chunk_rows: int = 200_000,
) -> BackfillJob:
    timeframe = TIMEFRAME_BY_DATASET[dataset]

    async def chunk_fetcher(*, date: date, symbols: list[str] | None) -> AsyncIterator[list[dict[str, Any]]]:
        path = archive_path(archive_root, dataset, date)
        if not path.exists():
            raise MissingArchiveFileError(f"expected trading day not in archive: {dataset} {date}")
        with gzip.open(path, "rt", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            validate_header(reader.fieldnames)
            for chunk in iter_record_chunks(reader, timeframe, rejects, chunk_rows):
                for rec in chunk:
                    rec["trade_date"] = date.isoformat()
                yield chunk

    coord = BackfillCoordinator(storage_root=storage_root, writer_factory=massive_writer_factory)
    definition = BackfillJobDefinition(
        provider=PROVIDER,
        feed=FEED,
        date_range_start=start,
        date_range_end=end,
        ts_available_policy=TsAvailablePolicy.CUSTOM,
    )
    job = coord.create_job(definition)
    result = await coord.run_job_chunked(
        job.backfill_id,
        definition,
        chunk_fetcher,
        post_date_hook=lambda d: promote_date(storage_root, DATASET_DIR, d.isoformat()),
    )
    if rejects.rate > MAX_REJECT_RATE:
        raise RuntimeError(f"reject rate {rejects.rate:.4f} exceeds {MAX_REJECT_RATE}; likely a broken parser")
    rejects.flush(Path(archive_root) / "rejects" / f"{result.backfill_id}.jsonl")
    return result
