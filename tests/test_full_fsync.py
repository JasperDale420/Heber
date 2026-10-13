"""The shared durability primitives must ask Darwin for a real flush.

On macOS plain ``fsync()`` only hands the data to the drive, which may still
hold it in its own write cache when power is lost. ``fcntl(fd, F_FULLFSYNC)`` is
the documented way to make the drive itself flush. ``publish_file_atomically``
and ``fsync_directory`` are the primitives every lake writer publishes through,
so they are where that request belongs.

The platform is monkeypatched so both code paths run on any CI host.
"""

from __future__ import annotations

import errno
import fcntl
import os
import sys
from pathlib import Path

import pytest

from heber.writer import utils


class _Calls:
    def __init__(self) -> None:
        self.fullfsync: list[int] = []
        self.fsync: list[int] = []


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> _Calls:
    recorded = _Calls()
    real_fsync = os.fsync

    def fake_fsync(fd: int) -> None:
        recorded.fsync.append(fd)
        real_fsync(fd)

    monkeypatch.setattr(utils.os, "fsync", fake_fsync)
    return recorded


def _darwin(monkeypatch: pytest.MonkeyPatch, calls: _Calls, *, fails: bool = False) -> None:
    monkeypatch.setattr(utils.sys, "platform", "darwin")
    monkeypatch.setattr(utils.fcntl, "F_FULLFSYNC", 51, raising=False)

    def fake_fcntl(fd: int, cmd: int, *args: object) -> int:
        assert cmd == 51
        calls.fullfsync.append(fd)
        if fails:
            raise OSError(errno.ENOTSUP, "Operation not supported")
        return 0

    monkeypatch.setattr(utils.fcntl, "fcntl", fake_fcntl)


def _staged(tmp_path: Path) -> tuple[Path, Path]:
    tmp = tmp_path / "part-0.parquet.tmp"
    tmp.write_bytes(b"not empty")
    return tmp, tmp_path / "part-0.parquet"


def test_publish_uses_full_fsync_on_darwin(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, calls: _Calls) -> None:
    _darwin(monkeypatch, calls)
    tmp, final = _staged(tmp_path)

    utils.publish_file_atomically(tmp, final)

    assert len(calls.fullfsync) == 1
    assert calls.fsync == []
    assert final.read_bytes() == b"not empty"


def test_directory_fsync_uses_full_fsync_on_darwin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, calls: _Calls
) -> None:
    _darwin(monkeypatch, calls)

    utils.fsync_directory(tmp_path)

    assert len(calls.fullfsync) == 1
    assert calls.fsync == []


def test_darwin_falls_back_to_fsync_when_full_fsync_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, calls: _Calls
) -> None:
    """Some filesystems answer F_FULLFSYNC with ENOTSUP; the write must still flush."""
    _darwin(monkeypatch, calls, fails=True)
    tmp, final = _staged(tmp_path)

    utils.publish_file_atomically(tmp, final)
    utils.fsync_directory(tmp_path)

    assert len(calls.fullfsync) == 2
    assert len(calls.fsync) == 2
    assert final.exists()


def test_darwin_fallback_is_logged(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, calls: _Calls) -> None:
    """A weaker flush than requested must not pass silently."""
    _darwin(monkeypatch, calls, fails=True)
    warnings: list[tuple[str, dict[str, object]]] = []
    monkeypatch.setattr(utils.logger, "warning", lambda event, **kw: warnings.append((event, kw)))

    utils.fsync_directory(tmp_path)

    assert [event for event, _ in warnings] == ["heber_full_fsync_unsupported"]
    assert warnings[0][1]["errno"] == errno.ENOTSUP


def test_other_platforms_use_plain_fsync(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, calls: _Calls) -> None:
    monkeypatch.setattr(utils.sys, "platform", "linux")
    monkeypatch.setattr(utils.fcntl, "fcntl", lambda *a: pytest.fail("fcntl must not be called off Darwin"))
    tmp, final = _staged(tmp_path)

    utils.publish_file_atomically(tmp, final)
    utils.fsync_directory(tmp_path)

    assert len(calls.fsync) == 2


def test_darwin_without_the_constant_uses_plain_fsync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, calls: _Calls
) -> None:
    """A Python build that does not expose F_FULLFSYNC still flushes."""
    monkeypatch.setattr(utils.sys, "platform", "darwin")
    monkeypatch.delattr(utils.fcntl, "F_FULLFSYNC", raising=False)
    monkeypatch.setattr(utils.fcntl, "fcntl", lambda *a: pytest.fail("fcntl needs F_FULLFSYNC"))

    utils.fsync_directory(tmp_path)

    assert len(calls.fsync) == 1


@pytest.mark.skipif(sys.platform != "darwin", reason="real F_FULLFSYNC needs macOS")
def test_real_full_fsync_succeeds_on_a_local_file_and_directory(tmp_path: Path) -> None:
    """Unmocked: the call is accepted on both a file and a directory descriptor."""
    tmp, final = _staged(tmp_path)

    utils.publish_file_atomically(tmp, final)
    utils.fsync_directory(tmp_path)

    assert final.read_bytes() == b"not empty"
    assert hasattr(fcntl, "F_FULLFSYNC")
