"""Tests for the Massive S3 flat-file downloader (manifest + resume)."""

import gzip
import json

from heber.backfill.massive.downloader import MassiveFlatFileDownloader


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
