"""Tests for daily raw Massive corporate-action capture."""

from __future__ import annotations

import gzip
import json
from datetime import date
from pathlib import Path
from typing import Any, cast
from urllib.parse import parse_qs, urlsplit

from heber.backfill.massive.corporate_actions import MassiveCorporateActionsDownloader


def _read_json_gz(path: Path) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(gzip.decompress(path.read_bytes()).decode("utf-8")))


def test_sync_date_writes_splits_dividends_and_active_ticker_snapshot_as_raw_pages(tmp_path):
    calls: list[str] = []

    def http_get(url: str, headers: dict[str, str], timeout: float) -> dict[str, Any]:
        calls.append(url)
        assert headers["Authorization"] == "Bearer test-token"
        parsed = urlsplit(url)
        params = parse_qs(parsed.query)

        if parsed.path == "/stocks/v1/splits":
            assert params["execution_date"] == ["2024-01-02"]
            assert params["limit"] == ["5000"]
            return {"status": "OK", "results": [{"ticker": "AAPL", "execution_date": "2024-01-02"}]}

        if parsed.path == "/stocks/v1/dividends":
            assert params["ex_dividend_date"] == ["2024-01-02"]
            assert params["limit"] == ["5000"]
            return {"status": "OK", "results": [{"ticker": "MSFT", "ex_dividend_date": "2024-01-02"}]}

        if parsed.path == "/v3/reference/tickers":
            assert params["market"] == ["stocks"]
            assert params["active"] == ["true"]
            assert params["date"] == ["2024-01-02"]
            assert params["limit"] == ["1000"]
            return {"status": "OK", "results": [{"ticker": "AAPL", "active": True}]}

        raise AssertionError(f"unexpected URL: {url}")

    downloader = MassiveCorporateActionsDownloader(
        api_key="test-token",  # pragma: allowlist secret
        archive_root=tmp_path,
        http_get=http_get,
    )

    summary = downloader.sync_date(date(2024, 1, 2))

    assert summary.target_date == date(2024, 1, 2)
    assert summary.result_counts == {"splits": 1, "dividends": 1, "tickers_active": 1}

    base = tmp_path / "massive_corp_actions" / "daily" / "2024-01-02"
    assert _read_json_gz(base / "splits" / "page_00000.json.gz")["results"][0]["ticker"] == "AAPL"
    assert _read_json_gz(base / "dividends" / "page_00000.json.gz")["results"][0]["ticker"] == "MSFT"
    assert _read_json_gz(base / "tickers_active" / "page_00000.json.gz")["results"][0]["ticker"] == "AAPL"

    manifest = json.loads((base / "splits" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["dataset"] == "splits"
    assert manifest["target_date"] == "2024-01-02"
    assert manifest["pages"] == 1
    assert manifest["results"] == 1
    assert (base / "splits" / ".done").exists()

    # A restart/re-run for the same date should not call the API again.
    downloader.sync_date(date(2024, 1, 2))
    assert len(calls) == 3
