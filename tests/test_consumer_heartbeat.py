"""The liveness heartbeat must tick during batch processing, not only between batches.

The heartbeat is set at the top of each consume iteration, but a 2000-message
batch fanned out across hundreds of historical partitions can run longer than
the 180s healthcheck window — the container reads as "stalled" mid-drain and
the watchdog restarts a consumer that is actually making progress.
"""

from __future__ import annotations

import time

import pytest

from heber.ops.metrics import consumer_loop_heartbeat_unixtime
from heber.writer.consumer import EventConsumer


@pytest.mark.unit
async def test_heartbeat_ticks_while_processing_a_batch(monkeypatch) -> None:
    consumer = EventConsumer()

    async def _ok(_data: dict) -> tuple[bool, str, int]:
        return (True, "", 1)

    monkeypatch.setattr(consumer, "_process_with_retry", _ok)

    consumer_loop_heartbeat_unixtime.set(1000.0)  # long-stale
    acked, failed = await consumer._process_stream_messages([(b"1-0", {"data": "a"}), (b"2-0", {"data": "b"})])

    assert len(acked) == 2 and not failed
    assert consumer_loop_heartbeat_unixtime._value.get() > time.time() - 5
