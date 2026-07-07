"""Tests for the concurrent partition-flush helper (heber.writer.utils).

The helper parallelizes per-partition writes so a backfill batch (records
scattered across hundreds of date partitions) drains without starving the
consumer. The safety contract must match the old serial path exactly: a
partition is dropped from the buffer only after its write succeeds, a failed
write retains its records for redelivery, and the first error is re-raised so
the caller does not ACK.
"""

import threading

import pytest

from heber.writer.utils import flush_partitions_concurrent

pytestmark = pytest.mark.unit


def _make_buffers(n: int) -> dict[str, list[int]]:
    return {f"feed=x/dt=2026-01-{i:02d}/hour=00": [i, i] for i in range(1, n + 1)}


@pytest.mark.parametrize("workers", [1, 8])
def test_all_partitions_flushed_and_dropped(workers: int) -> None:
    buffers = _make_buffers(20)
    written: list[str] = []
    lock = threading.Lock()

    def flush(pk: str, records: list[int]) -> None:
        with lock:
            written.append(pk)

    keys = list(buffers)
    assert flush_partitions_concurrent(buffers, flush, keys, workers) is True
    assert sorted(written) == sorted(keys)  # every partition written exactly once
    assert buffers == {}  # every key dropped on success


@pytest.mark.parametrize("workers", [1, 8])
def test_failed_partition_is_retained_and_error_reraised(workers: int) -> None:
    buffers = _make_buffers(10)
    poison = "feed=x/dt=2026-01-05/hour=00"

    def flush(pk: str, records: list[int]) -> None:
        if pk == poison:
            raise OSError("bind mount EPERM")

    keys = list(buffers)
    with pytest.raises(OSError, match="bind mount EPERM"):
        flush_partitions_concurrent(buffers, flush, keys, workers)

    # The failed partition keeps its records for redelivery; every other
    # partition succeeded and was dropped.
    assert poison in buffers
    assert buffers[poison] == [5, 5]
    assert set(buffers) == {poison}


def test_empty_partition_list_is_noop() -> None:
    buffers = _make_buffers(3)
    assert flush_partitions_concurrent(buffers, lambda pk, r: None, [], 8) is False
    assert len(buffers) == 3  # nothing touched


def test_empty_buffer_keys_are_dropped_without_flush() -> None:
    buffers: dict[str, list[int]] = {"feed=x/dt=2026-01-01/hour=00": []}
    called: list[str] = []
    # An empty partition passed in the due-list is dropped without a write.
    assert flush_partitions_concurrent(buffers, lambda pk, r: called.append(pk), list(buffers), 8) is False
    assert called == []
    assert buffers == {}
