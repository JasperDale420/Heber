"""Capture daily Massive REST corporate-action pages into the raw archive."""

from __future__ import annotations

import gzip
import json
import os
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlencode, urlsplit

import structlog

logger = structlog.get_logger(__name__)

BASE_URL = "https://api.massive.com"
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
HttpGet = Callable[[str, dict[str, str], float], dict[str, Any]]


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


def _urllib_get_json(url: str, headers: dict[str, str], timeout: float) -> dict[str, Any]:
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return cast(dict[str, Any], json.loads(resp.read().decode("utf-8")))


def _write_json_gz(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as fh:
        json.dump(payload, fh)
    os.replace(tmp, path)


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
        self._http_get = http_get or _urllib_get_json
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
            _write_json_gz(page_path, payload)
            page_files.append(page_path.name)
            total += len(results)
            page += 1

            next_url = payload.get("next_url")
            if next_url:
                self._validate_next_url(str(next_url))
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
        headers = {"Authorization": f"Bearer {self._api_key}"}
        for attempt in range(self._max_retries):
            try:
                return self._http_get(url, headers, self._timeout_seconds)
            except urllib.error.HTTPError as exc:
                if exc.code in RETRYABLE_STATUS_CODES and attempt < self._max_retries - 1:
                    time.sleep(min(60, 2**attempt))
                    continue
                raise
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                if attempt < self._max_retries - 1:
                    time.sleep(min(60, 2**attempt))
                    continue
                raise
        raise RuntimeError(f"exhausted Massive REST retries for {url[:120]}")

    def _validate_next_url(self, next_url: str) -> None:
        parsed = urlsplit(next_url)
        if parsed.netloc and parsed.netloc != self._api_host:
            raise RuntimeError(f"Massive next_url host changed unexpectedly: {parsed.netloc}")

    @staticmethod
    def _read_completed_count(manifest: Path) -> int:
        try:
            return int(json.loads(manifest.read_text(encoding="utf-8")).get("results", 0))
        except (OSError, ValueError, TypeError):
            return 0
