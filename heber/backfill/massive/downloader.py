"""Download Massive flat files into an immutable vendor-raw archive with a manifest."""

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


class MassiveFlatFileDownloader:
    def __init__(self, s3_client: Any, bucket: str, archive_root: str):
        self._s3 = s3_client
        self._bucket = bucket
        self._archive_root = Path(archive_root)
        self._manifest_path = self._archive_root / "manifest.jsonl"
        self._seen = self._load_seen()

    def _load_seen(self) -> dict[str, str]:
        seen: dict[str, str] = {}
        if self._manifest_path.exists():
            for line in self._manifest_path.read_text(encoding="utf-8").splitlines():
                try:
                    row = json.loads(line)
                    seen[row["s3_key"]] = row["sha256"]
                except (ValueError, KeyError):
                    continue
        return seen

    def _append_manifest(self, row: dict[str, Any]) -> None:
        self._manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with self._manifest_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row) + "\n")

    def sync_month(self, dataset: str, year: int, month: int) -> int:
        prefix = f"us_stocks_sip/{dataset}/{year:04d}/{month:02d}/"
        written = 0
        for page in self._s3.get_paginator("list_objects_v2").paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                dest = self._archive_root / key
                if key in self._seen and dest.exists() and _sha256(dest) == self._seen[key]:
                    continue
                dest.parent.mkdir(parents=True, exist_ok=True)
                tmp = dest.with_suffix(dest.suffix + ".tmp")
                self._s3.download_file(self._bucket, key, str(tmp))
                os.replace(tmp, dest)
                digest = _sha256(dest)
                self._append_manifest(
                    {
                        "s3_key": key,
                        "size": obj.get("Size"),
                        "etag": (obj.get("ETag") or "").strip('"'),
                        "sha256": digest,
                        "downloaded_at": datetime.now(UTC).isoformat(),
                    }
                )
                self._seen[key] = digest
                written += 1
        logger.info("massive_sync_month", dataset=dataset, year=year, month=month, written=written)
        return written
