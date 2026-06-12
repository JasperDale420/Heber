"""Download Massive flat files into an immutable vendor-raw archive with a manifest."""

import hashlib
import json
import os
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class MassiveFlatFileNotAvailableError(RuntimeError):
    """Massive has not published the expected flat file yet."""


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

    def _sync_object(self, key: str, obj: dict[str, Any]) -> int:
        dest = self._archive_root / key
        if key in self._seen and dest.exists() and _sha256(dest) == self._seen[key]:
            return 0

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
        return 1

    def sync_month(self, dataset: str, year: int, month: int) -> int:
        prefix = f"us_stocks_sip/{dataset}/{year:04d}/{month:02d}/"
        written = 0
        for page in self._s3.get_paginator("list_objects_v2").paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                written += self._sync_object(key, obj)
        logger.info("massive_sync_month", dataset=dataset, year=year, month=month, written=written)
        return written

    def sync_date(self, dataset: str, target_date: date) -> int:
        key = f"us_stocks_sip/{dataset}/{target_date:%Y}/{target_date:%m}/{target_date:%Y-%m-%d}.csv.gz"
        for page in self._s3.get_paginator("list_objects_v2").paginate(Bucket=self._bucket, Prefix=key):
            for obj in page.get("Contents", []):
                if obj.get("Key") == key:
                    written = self._sync_object(key, obj)
                    logger.info(
                        "massive_sync_date",
                        dataset=dataset,
                        target_date=target_date.isoformat(),
                        written=written,
                    )
                    return written

        raise MassiveFlatFileNotAvailableError(
            f"Massive flat file is not available for {dataset} {target_date:%Y-%m-%d}: {key}"
        )


def create_massive_s3_client(*, endpoint_url: str = "https://files.massive.com") -> Any:
    """Create the S3-compatible client for Massive flat files from env credentials."""
    access_key_id = os.environ.get("MASSIVE_S3_ACCESS_KEY_ID") or os.environ.get("AWS_ACCESS_KEY_ID")
    secret_access_key = os.environ.get("MASSIVE_S3_SECRET_ACCESS_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not access_key_id or not secret_access_key:
        raise ValueError(
            "Massive S3 credentials are missing. Set MASSIVE_S3_ACCESS_KEY_ID/"
            "MASSIVE_S3_SECRET_ACCESS_KEY or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY."
        )

    import boto3  # type: ignore[import-untyped]

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
    )
