"""Capture daily Massive REST corporate-action pages into the raw archive."""

from __future__ import annotations

import json
import os
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlsplit

import structlog

from heber.backfill.massive.http import (
    BASE_URL,
    HttpGet,
    fetch_with_retry,
    urllib_get_json,
    validate_next_url,
    write_json_gz,
)

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class DailyRestDataset:
    name: str
    path: str
    date_param: str
    params: dict[str, str]


@dataclass(frozen=True)
class DailyCorporateActionsSummary:
    target_date: date
    result_counts: dict[str, int]


DAILY_DATASETS: dict[str, DailyRestDataset] = {
    "splits": DailyRestDataset(
        name="splits",
        path="/stocks/v1/splits",
        date_param="execution_date",
        params={"limit": "5000", "sort": "execution_date.asc"},
    ),
    "dividends": DailyRestDataset(
        name="dividends",
        path="/stocks/v1/dividends",
        date_param="ex_dividend_date",
        params={"limit": "5000", "sort": "ex_dividend_date.asc"},
    ),
    "tickers_active": DailyRestDataset(
        name="tickers_active",
        path="/v3/reference/tickers",
        date_param="date",
        params={"market": "stocks", "active": "true", "limit": "1000", "sort": "ticker", "order": "asc"},
    ),
}


class MassiveCorporateActionsDownloader:
    """Daily raw REST capture for splits, dividends, and the active US stock ticker snapshot."""

    def __init__(
        self,
        *,
        api_key: str,
        archive_root: str | Path,
        http_get: HttpGet | None = None,
        base_url: str = BASE_URL,
        timeout_seconds: float = 90.0,
        max_retries: int = 7,
    ) -> None:
        if not api_key:
            raise ValueError("MASSIVE_API_KEY is required for corporate-action capture")
        self._api_key = api_key
        self._archive_root = Path(archive_root)
        self._http_get = http_get or urllib_get_json
        self._base_url = base_url.rstrip("/")
        self._api_host = urlsplit(self._base_url).netloc
        self._timeout_seconds = timeout_seconds
        self._max_retries = max_retries

    def sync_date(
        self,
        target_date: date,
        *,
        datasets: Iterable[str] = DAILY_DATASETS.keys(),
        force: bool = False,
    ) -> DailyCorporateActionsSummary:
        counts: dict[str, int] = {}
        for dataset in datasets:
            if dataset not in DAILY_DATASETS:
                raise ValueError(f"unknown Massive daily REST dataset: {dataset}")
            counts[dataset] = self._sync_dataset(DAILY_DATASETS[dataset], target_date, force=force)
        logger.info("massive_daily_corporate_actions_synced", target_date=target_date.isoformat(), counts=counts)
        return DailyCorporateActionsSummary(target_date=target_date, result_counts=counts)

    def _sync_dataset(self, dataset: DailyRestDataset, target_date: date, *, force: bool) -> int:
        out_dir = self._archive_root / "massive_corp_actions" / "daily" / target_date.isoformat() / dataset.name
        done = out_dir / ".done"
        manifest = out_dir / "manifest.json"
        if done.exists() and not force:
            return self._read_completed_count(manifest)

        out_dir.mkdir(parents=True, exist_ok=True)
        for old_page in out_dir.glob("page_*.json.gz"):
            old_page.unlink()
        done.unlink(missing_ok=True)

        params = dict(dataset.params)
        params[dataset.date_param] = target_date.isoformat()
        url: str | None = f"{self._base_url}{dataset.path}?{urlencode(params)}"
        page = 0
        total = 0
        page_files: list[str] = []

        while url:
            payload = self._fetch(url)
            status = payload.get("status")
            if status not in ("OK", "DELAYED", None):
                raise RuntimeError(f"Massive {dataset.name} returned status={status!r} for {target_date}")

            results = payload.get("results") or []
            page_path = out_dir / f"page_{page:05d}.json.gz"
            write_json_gz(page_path, payload)
            page_files.append(page_path.name)
            total += len(results)
            page += 1

            next_url = payload.get("next_url")
            if next_url:
                validate_next_url(str(next_url), self._api_host)
            url = str(next_url) if next_url else None

        manifest_payload = {
            "dataset": dataset.name,
            "target_date": target_date.isoformat(),
            "pages": page,
            "results": total,
            "page_files": page_files,
            "completed_at": datetime.now(UTC).isoformat(),
        }
        tmp_manifest = manifest.with_suffix(".json.tmp")
        tmp_manifest.write_text(json.dumps(manifest_payload, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(tmp_manifest, manifest)
        done.write_text(datetime.now(UTC).isoformat(), encoding="utf-8")
        return total

    def _fetch(self, url: str) -> dict[str, Any]:
        return fetch_with_retry(
            self._http_get,
            url,
            api_key=self._api_key,
            timeout_seconds=self._timeout_seconds,
            max_retries=self._max_retries,
            max_sleep_seconds=60.0,
        )

    @staticmethod
    def _read_completed_count(manifest: Path) -> int:
        try:
            return int(json.loads(manifest.read_text(encoding="utf-8")).get("results", 0))
        except (OSError, ValueError, TypeError):
            return 0
