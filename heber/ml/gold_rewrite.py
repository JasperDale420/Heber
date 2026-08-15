"""Safe in-place rewrite protocol for Gold partition files.

Rewriting a Gold partition means replacing training data that already exists,
on a volume (exFAT, no journaling) where a rename without an explicit flush has
previously published zero-byte files. Every rewrite therefore:

1. takes the same partition lock ``persist_features_to_gold`` uses, and reads
   the file *inside* it — a table read before the lock can be stale by the time
   it is published, which would silently drop a concurrently written row;
2. copies the original to a checksummed backup off the lakehouse volume;
3. stages the replacement and fsyncs it before publishing;
4. validates the staged file, publishes, then re-reads and validates again;
5. restores the backup if anything fails after the publish.
"""

from __future__ import annotations

import hashlib
import os
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

logger = structlog.get_logger(__name__)

# A rewrite plan returns the replacement table, or None to leave the file alone.
Planner = Callable[[pa.Table], "pa.Table | None"]
# A validator returns a list of problems; empty means the table is acceptable.
Validator = Callable[[pa.Table, pa.Table], list[str]]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_dir(directory: Path) -> None:
    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def stage(table: pa.Table, out_file: Path) -> Path:
    """Write the replacement to a staging file and flush it to disk."""
    temp_path = out_file.with_name(f".{out_file.name}.rewrite-{os.getpid()}")
    pq.write_table(table, temp_path, compression="snappy")  # type: ignore[no-untyped-call]
    with temp_path.open("rb+") as handle:
        os.fsync(handle.fileno())
    return temp_path


def publish(temp_path: Path, out_file: Path) -> None:
    os.replace(temp_path, out_file)
    _fsync_dir(out_file.parent)
    # Writing on exFAT leaves an AppleDouble xattr sidecar next to the file. It
    # holds no lake data and pyarrow's directory walk crashes on it, so a
    # rewrite must not leave new ones behind.
    out_file.with_name(f"._{out_file.name}").unlink(missing_ok=True)


class PublishFailed(Exception):
    """Raised when a rewrite may have replaced the live file and then failed.

    Anything thrown from the moment ``publish()`` is entered — a failed
    directory fsync, a failed sidecar unlink, an unreadable published file —
    means the live file can no longer be trusted, so the caller must restore.
    """


def restore(backup_path: Path, out_file: Path) -> None:
    """Put the verified backup back in place after a failed rewrite."""
    temp_path = out_file.with_name(f".{out_file.name}.restore-{os.getpid()}")
    shutil.copy2(backup_path, temp_path)
    with temp_path.open("rb+") as handle:
        os.fsync(handle.fileno())
    os.replace(temp_path, out_file)
    _fsync_dir(out_file.parent)
    out_file.with_name(f"._{out_file.name}").unlink(missing_ok=True)


def rewrite_partition(
    path: Path,
    *,
    plan: Planner,
    validate: Validator,
    backup_dir: Path | None,
    lock_timeout: float = 30,
) -> dict[str, Any]:
    """Rewrite one partition under the writer's lock, or explain why it did not.

    ``plan`` receives the table read under the lock and returns the replacement
    (or None to skip). ``validate`` receives (original, candidate) and returns a
    list of problems; it runs against the staged file and again after publish.
    """
    result: dict[str, Any] = {"partition": path.parent.name, "path": str(path), "status": "ok"}

    from filelock import FileLock, Timeout

    try:
        with FileLock(str(path.with_suffix(".parquet.lock")), timeout=lock_timeout):
            return _rewrite_locked(path, result, plan=plan, validate=validate, backup_dir=backup_dir)
    except Timeout:
        result["status"] = "locked"
        result["problems"] = ["could not acquire the partition write lock"]
        return result


def _rewrite_locked(
    path: Path,
    result: dict[str, Any],
    *,
    plan: Planner,
    validate: Validator,
    backup_dir: Path | None,
) -> dict[str, Any]:
    original = pq.ParquetFile(path).read()

    try:
        candidate = plan(original)
    except Exception as exc:
        result["status"] = "plan_failed"
        result["problems"] = [f"{exc.__class__.__name__}: {exc}"]
        return result

    if candidate is None:
        result["status"] = "skipped"
        return result

    backup_path: Path | None = None
    if backup_dir is not None:
        backup_dir.mkdir(parents=True, exist_ok=True)
        backup_path = backup_dir / f"{path.parent.name}-{path.name}"
        shutil.copy2(path, backup_path)
        if sha256(backup_path) != sha256(path):
            result["status"] = "backup_failed"
            result["problems"] = ["backup checksum did not match source"]
            return result
        result["backup"] = str(backup_path)
        result["source_sha256"] = sha256(path)

    temp_path = stage(candidate, path)
    try:
        # Validate before replacing anything. A failure here leaves the
        # original untouched, so there is nothing to roll back.
        staged_problems = validate(original, pq.ParquetFile(temp_path).read())
        if staged_problems:
            result["status"] = "staging_failed"
            result["problems"] = staged_problems
            return result

        try:
            publish(temp_path, path)
            problems = validate(original, pq.ParquetFile(path).read())
        except Exception as exc:
            # Past this point the live file may already be the replacement,
            # so every failure has to go through the rollback path.
            raise PublishFailed(f"{exc.__class__.__name__}: {exc}") from exc

        if not problems:
            result["status"] = "rewritten"
            result["output_sha256"] = sha256(path)
            return result
    except PublishFailed as exc:
        problems = [str(exc)]
    finally:
        temp_path.unlink(missing_ok=True)

    result["status"] = "verify_failed"
    result["problems"] = problems
    if backup_path is None:
        result["problems"].append("NO BACKUP TAKEN — the bad file is still live")
    else:
        try:
            restore(backup_path, path)
            result["problems"].append("original restored from backup")
        except Exception as restore_error:
            result["status"] = "restore_failed"
            result["problems"].append(
                f"RESTORE FAILED ({restore_error.__class__.__name__}: {restore_error}) — "
                f"the live file is unverified; the original is at {backup_path}"
            )
    logger.error("gold partition rewrite failed", path=str(path), problems=result["problems"])
    return result
