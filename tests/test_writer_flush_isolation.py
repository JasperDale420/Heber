"""Unconditional ``flush()`` must attempt every partition, not stop at the first failure.

``flush()`` is the shutdown and durability-backstop path: it runs when the
process is going away, or when the consumer needs everything on disk before it
can acknowledge a batch. The serial implementation it replaces aborted the whole
loop on the first raising partition, so one bad partition silently took every
partition behind it down with it — at shutdown those buffers are simply gone.

``flush_partitions_concurrent`` (heber.writer.utils) already implements the
contract this needs: attempt everything, drop a partition only on success,
retain a failed partition's records, re-raise the first error.
"""

import gzip
import json
import threading

import pytest

from heber.writer.bronze import BronzeWriter
from heber.writer.silver import SilverWriter

pytestmark = pytest.mark.unit


def _poison_after_recording(monkeypatch, writer, poison_key: str, attempted: list[str]) -> None:
    """Record every partition ``flush()`` attempts; raise for ``poison_key``."""
    lock = threading.Lock()

    def wrapped(partition_key, records):
        with lock:
            attempted.append(partition_key)
        if partition_key == poison_key:
            raise OSError(f"bind mount EPERM ({len(records)} records)")

    monkeypatch.setattr(writer, "_flush_partition", wrapped)


def test_bronze_flush_attempts_every_partition_despite_one_failing(monkeypatch) -> None:
    writer = BronzeWriter()
    keys = [f"provider=test/feed=bars/dt=2026-01-0{i}/hour=00" for i in (1, 2, 3)]
    for i, key in enumerate(keys, start=1):
        writer.buffers[key] = [{"event_id": f"evt-{i}"}]

    poison = keys[1]
    attempted: list[str] = []
    _poison_after_recording(monkeypatch, writer, poison, attempted)

    with pytest.raises(OSError, match="bind mount EPERM"):
        writer.flush()

    # The partition *behind* the failure is the regression: a serial abort never
    # reaches it, so its records are lost at shutdown.
    assert sorted(attempted) == sorted(keys)
    assert set(writer.buffers) == {poison}
    assert writer.buffers[poison] == [{"event_id": "evt-2"}]


def test_silver_flush_attempts_every_partition_despite_one_failing(monkeypatch) -> None:
    writer = SilverWriter()
    keys = [f"feed=bars/instrument_type=equity/dt=2026-01-0{i}" for i in (1, 2, 3)]
    for i, key in enumerate(keys, start=1):
        writer.buffers[key] = [{"event_id": f"evt-{i}"}]

    poison = keys[1]
    attempted: list[str] = []
    _poison_after_recording(monkeypatch, writer, poison, attempted)

    with pytest.raises(OSError, match="bind mount EPERM"):
        writer.flush()

    assert sorted(attempted) == sorted(keys)
    assert set(writer.buffers) == {poison}
    assert writer.buffers[poison] == [{"event_id": "evt-2"}]


def test_bronze_flush_writes_surviving_partitions_to_disk(tmp_path, monkeypatch) -> None:
    """The surviving partitions are real files, not just dropped buffer keys."""
    from heber.config import settings

    monkeypatch.setattr(settings, "data_root", tmp_path)

    writer = BronzeWriter()
    keys = [f"provider=test/feed=bars/dt=2026-01-0{i}/hour=00" for i in (1, 2, 3)]
    for i, key in enumerate(keys, start=1):
        writer.buffers[key] = [{"event_id": f"evt-{i}"}]

    real_flush = writer._flush_partition
    poison = keys[1]

    def wrapped(partition_key, records):
        if partition_key == poison:
            raise OSError("bind mount EPERM")
        real_flush(partition_key, records)

    monkeypatch.setattr(writer, "_flush_partition", wrapped)

    with pytest.raises(OSError, match="bind mount EPERM"):
        writer.flush()

    written = sorted((tmp_path / "bronze").rglob("*.jsonl.gz"))
    assert len(written) == 2
    event_ids = set()
    for path in written:
        with gzip.open(path, "rt") as handle:
            event_ids.add(json.loads(handle.readline())["event_id"])
    assert event_ids == {"evt-1", "evt-3"}
