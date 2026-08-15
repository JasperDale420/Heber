"""Publish a file only once its bytes are on disk.

The lakehouse volume is exFAT, which has no journaling: a rename can be
recorded before the data it points at, so a crash or unmount mid-write
publishes a file with a valid name and no contents. A single zero-byte parquet
file fails the pyarrow scan for its entire dataset — one in
``labels_alert_barriers`` made all 194,630 outcome rows unreadable — and the
rows it should have held are gone, because the write never landed.

Staging under a temp name, flushing the file, renaming, then flushing the
directory closes that window: the name only becomes visible after the bytes
are durable.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from uuid import uuid4


def fsync_dir(directory: Path) -> None:
    """Flush a directory entry so a rename inside it is itself durable."""
    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def publish_atomically(write: Callable[[Path], None], out_path: Path) -> None:
    """Write via ``write(temp_path)``, flush, then rename onto ``out_path``.

    ``write`` receives the staging path and must write the complete file to it.
    The staging file is removed on any failure, so a partial write never
    becomes visible under the destination name.
    """
    temp_path = out_path.with_name(f".{out_path.name}.tmp-{os.getpid()}-{uuid4().hex}")
    try:
        write(temp_path)
        with temp_path.open("rb+") as handle:
            os.fsync(handle.fileno())
        os.replace(temp_path, out_path)
        fsync_dir(out_path.parent)
        # Writing on exFAT leaves an AppleDouble xattr sidecar next to the
        # file; it holds no data and pyarrow's directory walk trips over it.
        out_path.with_name(f"._{out_path.name}").unlink(missing_ok=True)
    finally:
        temp_path.unlink(missing_ok=True)
