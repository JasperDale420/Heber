"""Tests for Massive REST backlog raw-page sweeps."""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any, cast

import pytest

from heber.backfill.massive.rest_backlog import (
    DEFAULT_DATASETS,
    MassiveRestBacklogDataset,
    MassiveRestBacklogSweeper,
)


def _read_json_gz(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return cast(dict[str, Any], json.load(fh))


def test_sweep_skips_completed_dataset_without_fetching(tmp_path: Path) -> None:
    out_dir = tmp_path / "news"
    out_dir.mkdir()
    (out_dir / "manifest.json").write_text(
        json.dumps({"dataset": "news", "pages": 2, "records": 7}),
        encoding="utf-8",
    )
    (out_dir / ".done").write_text("", encoding="utf-8")

    def fail_fetch(_url: str, _headers: dict[str, str], _timeout: float) -> dict[str, Any]:
        raise AssertionError("completed datasets must not fetch")

    sweeper = MassiveRestBacklogSweeper(api_key="key", out_root=tmp_path, http_get=fail_fetch)

    summary = sweeper.sweep(MassiveRestBacklogDataset("news", "/v2/reference/news", {"limit": "1"}))

    assert summary.dataset == "news"
    assert summary.pages == 2
    assert summary.records == 7
    assert summary.skipped is True


def test_sweep_writes_pages_progress_manifest_and_done_marker(tmp_path: Path) -> None:
    responses: list[dict[str, Any]] = [
        {
            "status": "OK",
            "results": [{"id": "one"}],
            "next_url": "https://api.massive.com/v2/reference/news?cursor=next",
        },
        {"status": "OK", "results": [{"id": "two"}]},
    ]

    def fake_fetch(_url: str, _headers: dict[str, str], _timeout: float) -> dict[str, Any]:
        return responses.pop(0)

    sweeper = MassiveRestBacklogSweeper(api_key="key", out_root=tmp_path, http_get=fake_fetch)

    summary = sweeper.sweep(MassiveRestBacklogDataset("news", "/v2/reference/news", {"limit": "1"}))

    assert summary.pages == 2
    assert summary.records == 2
    assert (tmp_path / "news" / ".done").exists()
    assert _read_json_gz(tmp_path / "news" / "page_000000.json.gz")["results"] == [{"id": "one"}]
    assert _read_json_gz(tmp_path / "news" / "page_000001.json.gz")["results"] == [{"id": "two"}]
    manifest = json.loads((tmp_path / "news" / "manifest.json").read_text(encoding="utf-8"))
    progress = json.loads((tmp_path / "news" / "manifest.progress.json").read_text(encoding="utf-8"))
    assert manifest["records"] == 2
    assert progress["next_url"] is None


def test_sweep_resumes_from_last_page_next_url(tmp_path: Path) -> None:
    out_dir = tmp_path / "news"
    out_dir.mkdir()
    first_page = {
        "status": "OK",
        "results": [{"id": "one"}],
        "next_url": "https://api.massive.com/v2/reference/news?cursor=resume",
    }
    with gzip.open(out_dir / "page_000000.json.gz", "wt", encoding="utf-8") as fh:
        json.dump(first_page, fh)
    (out_dir / "manifest.progress.json").write_text(
        json.dumps(
            {
                "dataset": "news",
                "pages": 1,
                "records": 1,
                "next_url": "https://api.massive.com/v2/reference/news?cursor=resume",
            }
        ),
        encoding="utf-8",
    )
    seen_urls: list[str] = []

    def fake_fetch(url: str, _headers: dict[str, str], _timeout: float) -> dict[str, Any]:
        seen_urls.append(url)
        return {"status": "OK", "results": [{"id": "two"}]}

    sweeper = MassiveRestBacklogSweeper(api_key="key", out_root=tmp_path, http_get=fake_fetch)

    summary = sweeper.sweep(MassiveRestBacklogDataset("news", "/v2/reference/news", {"limit": "1"}))

    assert seen_urls == ["https://api.massive.com/v2/reference/news?cursor=resume"]
    assert summary.pages == 2
    assert summary.records == 2
    assert _read_json_gz(tmp_path / "news" / "page_000001.json.gz")["results"] == [{"id": "two"}]


def test_sweep_rejects_foreign_next_url(tmp_path: Path) -> None:
    def fake_fetch(_url: str, _headers: dict[str, str], _timeout: float) -> dict[str, Any]:
        return {
            "status": "OK",
            "results": [{"id": "one"}],
            "next_url": "https://example.com/v2/reference/news?cursor=bad",
        }

    sweeper = MassiveRestBacklogSweeper(api_key="key", out_root=tmp_path, http_get=fake_fetch)

    with pytest.raises(RuntimeError, match="next_url host changed"):
        sweeper.sweep(MassiveRestBacklogDataset("news", "/v2/reference/news", {"limit": "1"}))


def test_default_datasets_exclude_historical_trades_and_quotes() -> None:
    assert "trades" not in DEFAULT_DATASETS
    assert "quotes" not in DEFAULT_DATASETS
    assert "trades_v1" not in DEFAULT_DATASETS
    assert "quotes_v1" not in DEFAULT_DATASETS
    assert {"news", "short_interest", "short_volume", "float", "ratios"}.issubset(DEFAULT_DATASETS)
