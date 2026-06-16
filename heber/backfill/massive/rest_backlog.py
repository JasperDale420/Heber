"""Resumable Massive REST backlog capture into vendor-raw storage."""

from __future__ import annotations

import gzip
import json
import os
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlencode, urlsplit

import structlog

logger = structlog.get_logger(__name__)

BASE_URL = "https://api.massive.com"
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
HttpGet = Callable[[str, dict[str, str], float], dict[str, Any]]


@dataclass(frozen=True)
class MassiveRestBacklogDataset:
    name: str
    path: str
    params: Mapping[str, str]


@dataclass(frozen=True)
class MassiveRestBacklogSummary:
    dataset: str
    pages: int
    records: int
    skipped: bool
    complete: bool
    out_dir: Path


DEFAULT_DATASETS: dict[str, MassiveRestBacklogDataset] = {
    "risk_categories": MassiveRestBacklogDataset(
        name="risk_categories",
        path="/stocks/taxonomies/vX/risk-factors",
        params={"limit": "1000"},
    ),
    "float": MassiveRestBacklogDataset(
        name="float",
        path="/stocks/vX/float",
        params={"limit": "1000"},
    ),
    "short_interest": MassiveRestBacklogDataset(
        name="short_interest",
        path="/stocks/v1/short-interest",
        params={"limit": "1000"},
    ),
    "short_volume": MassiveRestBacklogDataset(
        name="short_volume",
        path="/stocks/v1/short-volume",
        params={"limit": "1000"},
    ),
    "ratios": MassiveRestBacklogDataset(
        name="ratios",
        path="/stocks/financials/v1/ratios",
        params={"limit": "1000"},
    ),
    "news": MassiveRestBacklogDataset(
        name="news",
        path="/v2/reference/news",
        params={"limit": "1000", "sort": "published_utc", "order": "asc"},
    ),
    "edgar_index": MassiveRestBacklogDataset(
        name="edgar_index",
        path="/stocks/filings/vX/index",
        params={"limit": "1000"},
    ),
    "form_3": MassiveRestBacklogDataset(
        name="form_3",
        path="/stocks/filings/vX/form-3",
        params={"limit": "1000"},
    ),
    "form_4": MassiveRestBacklogDataset(
        name="form_4",
        path="/stocks/filings/vX/form-4",
        params={"limit": "1000"},
    ),
    "thirteen_f": MassiveRestBacklogDataset(
        name="thirteen_f",
        path="/stocks/filings/vX/13-F",
        params={"limit": "1000"},
    ),
    "risk_factors": MassiveRestBacklogDataset(
        name="risk_factors",
        path="/stocks/filings/vX/risk-factors",
        params={"limit": "1000"},
    ),
    "eight_k_text": MassiveRestBacklogDataset(
        name="eight_k_text",
        path="/stocks/filings/8-K/vX/text",
        params={"limit": "100"},
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


def _read_json_gz(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return cast(dict[str, Any], json.load(fh))


def _result_count(payload: dict[str, Any]) -> int:
    results = payload.get("results")
    if isinstance(results, list):
        return len(results)
    if results is None:
        return 0
    return 1


class MassiveRestBacklogSweeper:
    """Write Massive REST paginated responses as raw gzip pages with checkpoints."""

    def __init__(
        self,
        *,
        api_key: str,
        out_root: str | Path,
        http_get: HttpGet | None = None,
        base_url: str = BASE_URL,
        timeout_seconds: float = 120.0,
        max_retries: int = 7,
    ) -> None:
        if not api_key:
            raise ValueError("MASSIVE_API_KEY is required for Massive REST backlog capture")
        self._api_key = api_key
        self._out_root = Path(out_root)
        self._http_get = http_get or _urllib_get_json
        self._base_url = base_url.rstrip("/")
        self._api_host = urlsplit(self._base_url).netloc
        self._timeout_seconds = timeout_seconds
        self._max_retries = max_retries

    def sweep(
        self,
        dataset: MassiveRestBacklogDataset,
        *,
        force: bool = False,
        max_pages: int | None = None,
    ) -> MassiveRestBacklogSummary:
        out_dir = self._out_root / dataset.name
        done = out_dir / ".done"
        manifest = out_dir / "manifest.json"
        if done.exists() and not force:
            payload = self._read_manifest(manifest)
            return MassiveRestBacklogSummary(
                dataset=dataset.name,
                pages=int(payload.get("pages", 0)),
                records=int(payload.get("records", 0)),
                skipped=True,
                complete=True,
                out_dir=out_dir,
            )

        out_dir.mkdir(parents=True, exist_ok=True)
        if force:
            self._clear_existing_pages(out_dir)

        page_index, records, url = self._resume_state(out_dir, dataset)
        logger.info(
            "massive_rest_backlog_started",
            dataset=dataset.name,
            page_index=page_index,
            records=records,
            resume=page_index > 0,
        )

        fetched_pages = 0
        while url:
            payload = self._fetch(url)
            self._validate_status(dataset.name, payload)
            next_url = payload.get("next_url")
            if next_url:
                self._validate_next_url(str(next_url))

            page_path = out_dir / f"page_{page_index:06d}.json.gz"
            _write_json_gz(page_path, payload)
            records += _result_count(payload)
            page_index += 1
            fetched_pages += 1
            self._write_progress(out_dir, dataset.name, page_index, records, str(next_url) if next_url else None)

            if page_index % 25 == 0:
                logger.info("massive_rest_backlog_progress", dataset=dataset.name, pages=page_index, records=records)

            if max_pages is not None and fetched_pages >= max_pages:
                return MassiveRestBacklogSummary(
                    dataset=dataset.name,
                    pages=page_index,
                    records=records,
                    skipped=False,
                    complete=False,
                    out_dir=out_dir,
                )

            url = str(next_url) if next_url else None

        self._write_complete_manifest(out_dir, dataset.name, page_index, records)
        logger.info("massive_rest_backlog_completed", dataset=dataset.name, pages=page_index, records=records)
        return MassiveRestBacklogSummary(
            dataset=dataset.name,
            pages=page_index,
            records=records,
            skipped=False,
            complete=True,
            out_dir=out_dir,
        )

    def _resume_state(self, out_dir: Path, dataset: MassiveRestBacklogDataset) -> tuple[int, int, str | None]:
        progress = out_dir / "manifest.progress.json"
        if progress.exists():
            payload = self._read_manifest(progress)
            return int(payload.get("pages", 0)), int(payload.get("records", 0)), payload.get("next_url")

        pages = sorted(out_dir.glob("page_*.json.gz"))
        if not pages:
            return 0, 0, self._initial_url(dataset)

        records = 0
        for page in pages:
            records += _result_count(_read_json_gz(page))
        next_url = _read_json_gz(pages[-1]).get("next_url")
        return len(pages), records, str(next_url) if next_url else None

    def _fetch(self, url: str) -> dict[str, Any]:
        headers = {"Authorization": f"Bearer {self._api_key}"}
        for attempt in range(self._max_retries):
            try:
                return self._http_get(url, headers, self._timeout_seconds)
            except urllib.error.HTTPError as exc:
                if exc.code in RETRYABLE_STATUS_CODES and attempt < self._max_retries - 1:
                    time.sleep(min(90, 2**attempt))
                    continue
                raise
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                if attempt < self._max_retries - 1:
                    time.sleep(min(90, 2**attempt))
                    continue
                raise
        raise RuntimeError(f"exhausted Massive REST retries for {url[:120]}")

    def _initial_url(self, dataset: MassiveRestBacklogDataset) -> str:
        return f"{self._base_url}{dataset.path}?{urlencode(dict(dataset.params))}"

    def _validate_next_url(self, next_url: str) -> None:
        parsed = urlsplit(next_url)
        if parsed.netloc and parsed.netloc != self._api_host:
            raise RuntimeError(f"Massive next_url host changed unexpectedly: {parsed.netloc}")

    @staticmethod
    def _validate_status(dataset: str, payload: dict[str, Any]) -> None:
        status = payload.get("status")
        if status not in ("OK", "DELAYED", None):
            raise RuntimeError(f"Massive {dataset} returned status={status!r}: {str(payload)[:200]}")

    @staticmethod
    def _read_manifest(path: Path) -> dict[str, Any]:
        try:
            return cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))
        except FileNotFoundError:
            return {}

    @staticmethod
    def _clear_existing_pages(out_dir: Path) -> None:
        for path in out_dir.glob("page_*.json.gz"):
            path.unlink()
        for marker in ("manifest.json", "manifest.progress.json", ".done"):
            (out_dir / marker).unlink(missing_ok=True)

    @staticmethod
    def _write_progress(out_dir: Path, dataset: str, pages: int, records: int, next_url: str | None) -> None:
        payload = {
            "dataset": dataset,
            "pages": pages,
            "records": records,
            "next_url": next_url,
            "updated_at": datetime.now(UTC).isoformat(),
        }
        tmp = out_dir / "manifest.progress.json.tmp"
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(tmp, out_dir / "manifest.progress.json")

    @staticmethod
    def _write_complete_manifest(out_dir: Path, dataset: str, pages: int, records: int) -> None:
        completed_at = datetime.now(UTC).isoformat()
        payload = {
            "dataset": dataset,
            "pages": pages,
            "records": records,
            "completed_at": completed_at,
        }
        tmp = out_dir / "manifest.json.tmp"
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(tmp, out_dir / "manifest.json")
        (out_dir / ".done").write_text(completed_at, encoding="utf-8")


def sweep_default_datasets(
    *,
    api_key: str,
    out_root: str | Path,
    dataset_names: Sequence[str] | None = None,
    force: bool = False,
    max_pages: int | None = None,
) -> list[MassiveRestBacklogSummary]:
    sweeper = MassiveRestBacklogSweeper(api_key=api_key, out_root=out_root)
    names = list(dataset_names) if dataset_names else list(DEFAULT_DATASETS)
    summaries: list[MassiveRestBacklogSummary] = []
    for name in names:
        if name not in DEFAULT_DATASETS:
            raise ValueError(f"unknown Massive REST backlog dataset: {name}")
        summaries.append(sweeper.sweep(DEFAULT_DATASETS[name], force=force, max_pages=max_pages))
    return summaries
