"""Durable file-backed fallback for failed DLQ writes.

When the Redis ``XADD`` to the DLQ stream fails, ``write_dlq_fallback_file``
serializes the event payload to a JSON file under ``<dlq_fallback_path>/dt=YYYY-MM-DD/``.
The write uses ``tempfile.NamedTemporaryFile`` in the same directory followed by
``os.replace`` so partially written files never become visible.

``try_xadd_with_retry`` wraps the Redis call in a bounded exponential backoff
(3 attempts at 0.5s, 1s, 2s by default).
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import tempfile
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import structlog

from heber.writer.durability import create_durable_directory

logger = structlog.get_logger(__name__)


DEFAULT_RETRY_DELAYS: tuple[float, ...] = (0.5, 1.0, 2.0)


async def try_xadd_with_retry(
    xadd_call: Callable[[], Awaitable[Any]],
    *,
    retry_delays: tuple[float, ...] = DEFAULT_RETRY_DELAYS,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> tuple[bool, Any | None, Exception | None]:
    """Invoke an async XADD callable with bounded exponential backoff.

    The ``xadd_call`` parameter is a zero-argument async callable that performs
    the Redis ``XADD`` and returns the resulting message ID. The first attempt
    runs immediately; subsequent attempts wait for the corresponding entry in
    ``retry_delays``. Returns ``(success, result, last_error)``.
    """
    attempts = len(retry_delays) + 1
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            result = await xadd_call()
        except Exception as exc:  # noqa: BLE001 — every xadd failure must trigger fallback
            last_error = exc
            if attempt < attempts:
                await sleep(retry_delays[attempt - 1])
            continue
        return True, result, None

    return False, None, last_error


def _event_id_hash(event_id: str) -> str:
    return hashlib.sha256(event_id.encode("utf-8")).hexdigest()[:16]


def _stream_basename(stream_name: str) -> str:
    """Reduce a Redis stream name to a filesystem-safe basename."""
    cleaned = stream_name.replace("/", "_").replace(":", "_").replace(" ", "_")
    return cleaned or "stream"


def write_dlq_fallback_file(
    fallback_root: Path,
    stream: str,
    event_id: str,
    payload: dict[str, Any],
    *,
    now: datetime | None = None,
    durable_root: Path | None = None,
) -> Path:
    """Persist a DLQ payload to disk and return the resulting file path.

    The file is first written to a tempfile in the destination directory and
    then atomically renamed; partially written files never become visible to
    readers. ``event_id`` is hashed (SHA256, 16-char prefix) to keep filenames
    short and filesystem-safe even for opaque event identifiers.
    """
    timestamp = now or datetime.now(UTC)
    partition_dir = fallback_root / f"dt={timestamp.strftime('%Y-%m-%d')}"
    create_durable_directory(partition_dir, root=durable_root or fallback_root)

    filename = f"{_stream_basename(stream)}_{_event_id_hash(event_id)}.json"
    target = partition_dir / filename

    serialized = json.dumps(payload, default=str, ensure_ascii=False)

    fd, tmp_path_str = tempfile.mkstemp(
        prefix=f".{filename}.",
        suffix=".tmp",
        dir=str(partition_dir),
    )
    tmp_path = Path(tmp_path_str)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, target)
        directory_fd = os.open(partition_dir, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except Exception:
        with contextlib.suppress(FileNotFoundError):
            tmp_path.unlink()
        raise

    return target


def count_fallback_files_for_today(fallback_root: Path, *, now: datetime | None = None) -> int:
    """Return the number of fallback files in today's partition (or 0 if missing).

    Name filters run before any ``stat`` call: on Apple Silicon Docker bind
    mounts, ``stat()`` on ``._*`` AppleDouble sidecar files raises EPERM,
    which would otherwise abort the whole scan.
    """
    timestamp = now or datetime.now(UTC)
    partition_dir = fallback_root / f"dt={timestamp.strftime('%Y-%m-%d')}"
    if not partition_dir.is_dir():
        return 0
    return sum(
        1
        for entry in partition_dir.iterdir()
        if entry.suffix == ".json" and not entry.name.startswith("._") and entry.is_file()
    )


def log_fallback_backlog(fallback_root: Path, *, service: str) -> None:
    """Log the count of today's fallback files at INFO so operators see backlog."""
    try:
        count = count_fallback_files_for_today(fallback_root)
    except OSError as exc:
        logger.warning(
            "dlq_fallback_scan_failed",
            service=service,
            fallback_dir=str(fallback_root),
            error=str(exc),
        )
        return

    logger.info(
        "dlq_fallback_backlog",
        service=service,
        fallback_dir=str(fallback_root),
        file_count=count,
    )
