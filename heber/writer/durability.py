"""Filesystem durability helpers for writer publication paths."""

from __future__ import annotations

import os
from pathlib import Path


def _fsync_directory(path: Path) -> None:
    directory_fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def create_durable_directory(path: Path, *, root: Path) -> None:
    """Create ``path`` below an existing root and persist every new parent link."""
    durable_root = root.resolve(strict=True)
    target = path.resolve(strict=False)
    try:
        relative = target.relative_to(durable_root)
    except ValueError as exc:
        raise ValueError(f"{path} is outside durable root {root}") from exc
    if not durable_root.is_dir():
        raise NotADirectoryError(durable_root)

    current = durable_root
    for part in relative.parts:
        parent = current
        current /= part
        if current.is_dir():
            continue
        try:
            current.mkdir()
        except FileExistsError:
            if not current.is_dir():
                raise NotADirectoryError(current) from None
        _fsync_directory(parent)
