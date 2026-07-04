"""Unattended daily Massive raw archive sync."""

from __future__ import annotations

import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import pandas as pd
import structlog

from heber.backfill.massive.corporate_actions import (
    DailyCorporateActionsSummary,
    MassiveCorporateActionsDownloader,
)
from heber.backfill.massive.downloader import MassiveFlatFileDownloader, create_massive_s3_client
from heber.backfill.massive.paths import archive_path

logger = structlog.get_logger(__name__)

RAW_STOCK_AGG_DATASETS = ("day_aggs_v1", "minute_aggs_v1")
PUBLISH_CUTOFF_ET = time(11, 30)
_XNYS = xcals.get_calendar("XNYS")


@dataclass(frozen=True)
class MassiveDailySyncResult:
    dates: list[date]
    raw_files: dict[date, dict[str, Path]]
    downloaded_counts: dict[date, dict[str, int]]
    corporate_action_counts: dict[date, dict[str, int]]


def latest_publishable_trading_day(
    now: datetime | None = None,
    *,
    publish_cutoff_et: time = PUBLISH_CUTOFF_ET,
) -> date:
    """Return the newest trading day whose Massive daily flat files should be published."""
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        raise ValueError("now must be timezone-aware")

    current_et = current.astimezone(ZoneInfo("America/New_York"))
    anchor = current_et.date() if current_et.time() >= publish_cutoff_et else current_et.date() - timedelta(days=1)
    sessions = _XNYS.sessions
    idx = sessions.searchsorted(pd.Timestamp(anchor), side="left") - 1
    if idx < 0:
        raise RuntimeError(f"no XNYS session exists before {anchor}")
    session = sessions[idx]
    return date(session.year, session.month, session.day)


def _session_dates(start: date, end: date) -> list[date]:
    if start > end:
        return []
    sessions = _XNYS.sessions_in_range(pd.Timestamp(start), pd.Timestamp(end))
    return [date(session.year, session.month, session.day) for session in sessions]


def latest_archived_date(archive_root: str | Path, dataset: str) -> date | None:
    base = Path(archive_root) / "us_stocks_sip" / dataset
    if not base.exists():
        return None

    latest: date | None = None
    for path in base.glob("*/*/*.csv.gz"):
        try:
            found = date.fromisoformat(path.name.removesuffix(".csv.gz"))
        except ValueError:
            continue
        if latest is None or found > latest:
            latest = found
    return latest


class MassiveDailySync:
    """Sync all missing publishable Massive stock aggregate days into raw storage."""

    def __init__(
        self,
        *,
        archive_root: str | Path,
        flat_file_downloader: Any | None = None,
        corporate_actions_downloader: Any | None = None,
        bucket: str = "flatfiles",
        endpoint_url: str = "https://files.massive.com",
        api_key: str | None = None,
    ) -> None:
        self._archive_root = Path(archive_root)
        if flat_file_downloader is None:
            flat_file_downloader = MassiveFlatFileDownloader(
                create_massive_s3_client(endpoint_url=endpoint_url),
                bucket=bucket,
                archive_root=str(self._archive_root),
            )
        self._flat_file_downloader = flat_file_downloader

        if corporate_actions_downloader is None:
            resolved_api_key = api_key if api_key is not None else os.environ.get("MASSIVE_API_KEY")
            if resolved_api_key:
                corporate_actions_downloader = MassiveCorporateActionsDownloader(
                    api_key=resolved_api_key,
                    archive_root=self._archive_root,
                )
        self._corporate_actions_downloader = corporate_actions_downloader

    def run(
        self,
        *,
        target_date: date | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        datasets: Sequence[str] = RAW_STOCK_AGG_DATASETS,
        sync_corporate_actions: bool = True,
        now: datetime | None = None,
    ) -> MassiveDailySyncResult:
        dates = self._plan_dates(
            target_date=target_date,
            start_date=start_date,
            end_date=end_date,
            datasets=datasets,
            now=now,
        )
        logger.info("massive_daily_sync_started", dates=[d.isoformat() for d in dates], datasets=list(datasets))

        raw_files: dict[date, dict[str, Path]] = {}
        downloaded_counts: dict[date, dict[str, int]] = {}
        corporate_action_counts: dict[date, dict[str, int]] = {}

        for sync_date in dates:
            raw_files[sync_date] = {}
            downloaded_counts[sync_date] = {}
            for dataset in datasets:
                written = self._flat_file_downloader.sync_date(dataset, sync_date)
                path = archive_path(str(self._archive_root), dataset, sync_date)
                if not path.exists():
                    raise FileNotFoundError(f"Massive raw archive file missing after sync: {path}")
                raw_files[sync_date][dataset] = path
                downloaded_counts[sync_date][dataset] = written
                logger.info(
                    "massive_daily_raw_file_synced",
                    dataset=dataset,
                    target_date=sync_date.isoformat(),
                    path=str(path),
                    written=written,
                )

            if sync_corporate_actions:
                if self._corporate_actions_downloader is None:
                    raise ValueError("MASSIVE_API_KEY is required unless --skip-corporate-actions is set")
                summary = self._corporate_actions_downloader.sync_date(sync_date)
                corporate_action_counts[sync_date] = self._coerce_corporate_action_counts(summary)

        logger.info("massive_daily_sync_completed", dates=[d.isoformat() for d in dates])
        return MassiveDailySyncResult(
            dates=dates,
            raw_files=raw_files,
            downloaded_counts=downloaded_counts,
            corporate_action_counts=corporate_action_counts,
        )

    def _plan_dates(
        self,
        *,
        target_date: date | None,
        start_date: date | None,
        end_date: date | None,
        datasets: Sequence[str],
        now: datetime | None,
    ) -> list[date]:
        if target_date and (start_date or end_date):
            raise ValueError("--date cannot be combined with --start-date or --end-date")
        if target_date:
            return [target_date]

        resolved_end = end_date or latest_publishable_trading_day(now)
        if start_date:
            return _session_dates(start_date, resolved_end)

        latest_dates = [latest_archived_date(self._archive_root, dataset) for dataset in datasets]
        complete_latest_dates = [d for d in latest_dates if d is not None]
        if len(complete_latest_dates) == len(datasets):
            start = min(complete_latest_dates) + timedelta(days=1)
            return _session_dates(start, resolved_end)

        # Avoid accidentally starting a full 20-year backfill from an empty archive.
        return [resolved_end]

    @staticmethod
    def _coerce_corporate_action_counts(summary: Any) -> dict[str, int]:
        if isinstance(summary, DailyCorporateActionsSummary):
            return dict(summary.result_counts)
        if isinstance(summary, dict):
            return {str(k): int(v) for k, v in summary.items()}
        result_counts = getattr(summary, "result_counts", None)
        if isinstance(result_counts, dict):
            return {str(k): int(v) for k, v in result_counts.items()}
        raise TypeError(f"unexpected corporate-action summary: {type(summary)!r}")
