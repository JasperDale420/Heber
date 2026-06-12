"""Tests for the Massive S3 flat-file downloader (manifest + resume)."""

import gzip
import json
from datetime import date

import pytest

from heber.backfill.massive.downloader import MassiveFlatFileDownloader, MassiveFlatFileNotAvailableError


class _FakeS3:
    """Minimal fake S3 client: one object per month, download_file writes the body."""

    def __init__(self, key: str, body: bytes):
        self._key = key
        self._body = body
        self.download_calls = 0

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self

    def paginate(self, *, Bucket, Prefix):
        if self._key.startswith(Prefix):
            yield {
                "Contents": [
                    {"Key": self._key, "Size": len(self._body), "ETag": '"abc123"'},
                ]
            }
        else:
            yield {"Contents": []}

    def download_file(self, bucket, key, dest):
        self.download_calls += 1
        with open(dest, "wb") as fh:
            fh.write(self._body)


class _FakeS3Many:
    """Minimal fake S3 client with several objects under the same monthly prefix."""

    def __init__(self, objects: dict[str, bytes]):
        self._objects = objects
        self.downloaded: list[str] = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self

    def paginate(self, *, Bucket, Prefix):
        yield {
            "Contents": [
                {"Key": key, "Size": len(body), "ETag": f'"etag-{idx}"'}
                for idx, (key, body) in enumerate(sorted(self._objects.items()))
                if key.startswith(Prefix)
            ]
        }

    def download_file(self, bucket, key, dest):
        self.downloaded.append(key)
        with open(dest, "wb") as fh:
            fh.write(self._objects[key])


def test_sync_writes_file_and_manifest_then_resumes(tmp_path):
    body = gzip.compress(b"ticker,volume\nAAPL,100\n")
    key = "us_stocks_sip/day_aggs_v1/2024/01/2024-01-02.csv.gz"
    s3 = _FakeS3(key, body)

    dl = MassiveFlatFileDownloader(s3, bucket="flatfiles", archive_root=str(tmp_path))
    written = dl.sync_month("day_aggs_v1", 2024, 1)

    assert written == 1
    assert s3.download_calls == 1

    dest = tmp_path / key
    assert dest.exists()
    assert dest.read_bytes() == body

    manifest = tmp_path / "manifest.jsonl"
    lines = manifest.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    row = json.loads(lines[0])
    assert row["s3_key"] == key
    assert row["size"] == len(body)
    assert row["etag"] == "abc123"
    assert len(row["sha256"]) == 64
    assert "downloaded_at" in row

    # Resume: a fresh downloader (re-reads manifest) must skip the already-synced file.
    dl2 = MassiveFlatFileDownloader(s3, bucket="flatfiles", archive_root=str(tmp_path))
    written2 = dl2.sync_month("day_aggs_v1", 2024, 1)

    assert written2 == 0
    assert s3.download_calls == 1  # no re-download
    assert len(manifest.read_text(encoding="utf-8").splitlines()) == 1  # no duplicate manifest line
    assert not list(tmp_path.rglob("*.tmp"))


def test_sync_date_downloads_only_the_requested_trading_day(tmp_path):
    jan2 = "us_stocks_sip/day_aggs_v1/2024/01/2024-01-02.csv.gz"
    jan3 = "us_stocks_sip/day_aggs_v1/2024/01/2024-01-03.csv.gz"
    s3 = _FakeS3Many(
        {
            jan2: gzip.compress(b"ticker,volume\nAAPL,100\n"),
            jan3: gzip.compress(b"ticker,volume\nMSFT,200\n"),
        }
    )

    dl = MassiveFlatFileDownloader(s3, bucket="flatfiles", archive_root=str(tmp_path))
    written = dl.sync_date("day_aggs_v1", date(2024, 1, 3))

    assert written == 1
    assert s3.downloaded == [jan3]
    assert not (tmp_path / jan2).exists()
    assert (tmp_path / jan3).exists()

    lines = (tmp_path / "manifest.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["s3_key"] == jan3


def test_sync_date_fails_loudly_when_massive_has_not_published_the_file(tmp_path):
    s3 = _FakeS3Many({})
    dl = MassiveFlatFileDownloader(s3, bucket="flatfiles", archive_root=str(tmp_path))

    with pytest.raises(MassiveFlatFileNotAvailableError, match="2024-01-03"):
        dl.sync_date("day_aggs_v1", date(2024, 1, 3))
