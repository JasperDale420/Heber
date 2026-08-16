"""Exclusive lock over one Gold ``dt=`` partition.

Two writers rewrite whole fragments of an existing partition: the Gold
compactor merges them into one file and deletes the originals, and a retro-flag
migration replaces individual fragments in place. Interleaved, they duplicate or
lose rows — the compactor can delete a fragment the migration then recreates via
``os.replace``, leaving both the compacted copy and a resurrected pre-compaction
one for the reader to pick up.

Lock files live in a sibling ``_locks`` tree rather than inside the partition:
a zero-byte file next to the data is picked up by ``pyarrow.dataset``'s
auto-walk and fails the read with ``Parquet file size is 0 bytes``.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from pathlib import Path

from filelock import FileLock

LOCK_DIRNAME = "_locks"
LOCK_TIMEOUT_SECONDS = 300


def partition_lock(partition_dir: Path, timeout: float = LOCK_TIMEOUT_SECONDS) -> AbstractContextManager[object]:
    """Return an exclusive lock over ``partition_dir``.

    Raises ``filelock.Timeout`` if another holder does not release in time —
    an unlocked rewrite is worse than a failed one.
    """
    lock_root = partition_dir.parent / LOCK_DIRNAME
    lock_root.mkdir(parents=True, exist_ok=True)
    return FileLock(lock_root / f"{partition_dir.name}.lock", timeout=timeout)
