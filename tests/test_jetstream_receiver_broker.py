"""JetStream receiver against a real nats-server.

Skipped unless a ``nats-server`` binary is available (``HEBER_TEST_NATS_SERVER``
or ``PATH``). Streams are provisioned and messages published exactly the way
Data-Gateway's sender does at commit 5adf17a
(``gateway/core/jetstream.py::build_stream_configs`` and
``JetStreamOutboxPublisher.__call__``), so this pins the receiver to the real
wire format and the real broker's acknowledgement semantics.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import socket
import subprocess  # nosec B404 — starts a local test broker only
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import nats
import pytest
from nats.js.api import DiscardPolicy, RetentionPolicy, StorageType, StreamConfig
from pydantic import SecretStr

from heber.config import settings
from heber.writer.jetstream_consumer import JetStreamEventConsumer
from tests.test_jetstream_receiver import bronze_rows, dlq_records, gateway_body, gateway_envelope, gateway_subject

pytestmark = pytest.mark.integration

_NATS_SERVER = os.environ.get("HEBER_TEST_NATS_SERVER") or shutil.which("nats-server")
_USER = "heber"
_PASSWORD = "test-only"  # pragma: allowlist secret — throwaway local broker credential


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture
async def broker_url(tmp_path: Path) -> AsyncIterator[str]:
    if _NATS_SERVER is None:
        pytest.skip("nats-server binary not available")
    port = _free_port()
    process = subprocess.Popen(  # nosec B603 — fixed argv, local binary chosen by the developer
        [
            _NATS_SERVER,
            "-js",
            "-a",
            "127.0.0.1",
            "-p",
            str(port),
            "-sd",
            str(tmp_path / "jetstream"),
            "--user",
            _USER,
            "--pass",
            _PASSWORD,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"nats://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 10
        while True:
            try:
                connection = await nats.connect(url, user=_USER, password=_PASSWORD, max_reconnect_attempts=0)
                await connection.close()
                break
            except Exception:
                if time.monotonic() > deadline:
                    raise
                await asyncio.sleep(0.1)
        yield url
    finally:
        process.terminate()
        process.wait(timeout=10)


async def _gateway(url: str) -> Any:
    """Connect and provision streams the way Data-Gateway's publisher does."""
    connection = await nats.connect(url, user=_USER, password=_PASSWORD)
    jetstream = connection.jetstream()
    for name, subject in (("HEBER_LIVE", "heber.live.>"), ("HEBER_BACKFILL", "heber.backfill.>")):
        await jetstream.add_stream(
            config=StreamConfig(
                name=name,
                subjects=[subject],
                retention=RetentionPolicy.WORK_QUEUE,
                discard=DiscardPolicy.NEW,
                max_age=0,
                max_bytes=64 * 1024**2,
                storage=StorageType.FILE,
                num_replicas=1,
                max_msg_size=8 * 1024**2,
            )
        )
    return connection


async def _publish(connection: Any, envelope: dict[str, Any], *, body: bytes | None = None) -> None:
    ack = await connection.jetstream().publish(
        gateway_subject("live", envelope),
        gateway_body(envelope) if body is None else body,
        stream="HEBER_LIVE",
        headers={"Nats-Msg-Id": envelope["event_id"]},
    )
    assert ack.stream == "HEBER_LIVE"


@pytest.fixture
def receiver_settings(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, broker_url: str) -> Path:
    data_root = tmp_path / "lake"
    monkeypatch.setattr(settings, "data_root", data_root)
    monkeypatch.setattr(settings, "ingest_transport", "jetstream")
    monkeypatch.setattr(settings, "ingest_lane", "live")
    monkeypatch.setattr(settings, "nats_url", broker_url)
    monkeypatch.setattr(settings, "nats_username", _USER)
    monkeypatch.setattr(settings, "nats_password", SecretStr(_PASSWORD))
    monkeypatch.setattr(settings, "jetstream_fetch_timeout_seconds", 0.2)
    monkeypatch.setattr(settings, "redis_retry_backoff_seconds", 0.0)
    return data_root


async def _drain(consumer: JetStreamEventConsumer, *, until: Any, timeout: float = 15) -> None:
    deadline = time.monotonic() + timeout
    while not await until():
        assert time.monotonic() < deadline, "receiver did not settle in time"
        await consumer._consume_iteration()


async def test_gateway_published_events_land_durably_and_leave_the_work_queue(receiver_settings: Path) -> None:
    gateway = await _gateway(settings.nats_url)
    good = [gateway_envelope(f"{index:032x}", close=f"10{index}.5") for index in range(1, 4)]
    for envelope in good:
        await _publish(gateway, envelope)
    await _publish(gateway, good[0])  # gateway retry inside the Nats-Msg-Id window
    await _publish(gateway, gateway_envelope("f" * 32), body=b"{not json")

    consumer = JetStreamEventConsumer()
    await consumer.connect()
    manager = gateway.jsm()

    async def settled() -> bool:
        await consumer._ack_barrier(force=True)
        info = await manager.consumer_info("HEBER_LIVE", "heber-live-writers")
        return info.num_ack_pending == 0 and info.num_pending == 0 and not consumer._held

    await _drain(consumer, until=settled)
    await consumer._shutdown()

    stream = await manager.stream_info("HEBER_LIVE")
    assert stream.state.messages == 0  # work queue: every message acknowledged
    assert sorted(row["event_id"] for row in bronze_rows(receiver_settings)) == sorted(e["event_id"] for e in good)
    records = dlq_records(receiver_settings)
    assert [record["source_message_id"] for record in records] == ["HEBER_LIVE:4"]
    await gateway.close()


async def test_nothing_is_acknowledged_while_the_flush_fails(
    receiver_settings: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gateway = await _gateway(settings.nats_url)
    for index in range(1, 3):
        await _publish(gateway, gateway_envelope(f"{index:032x}"))

    consumer = JetStreamEventConsumer()
    await consumer.connect()

    def broken_flush(*_: Any, **__: Any) -> None:
        raise OSError("disk gone")

    monkeypatch.setattr(consumer.bronze_writer, "_flush_partition", broken_flush)
    await consumer._consume_iteration()
    await consumer._ack_barrier(force=True)

    info = await gateway.jsm().consumer_info("HEBER_LIVE", "heber-live-writers")
    assert info.num_ack_pending == 2
    assert (await gateway.jsm().stream_info("HEBER_LIVE")).state.messages == 2
    assert bronze_rows(receiver_settings) == []

    await consumer._shutdown()
    await gateway.close()


async def test_receiver_refuses_to_bind_before_the_gateway_provisions_streams(receiver_settings: Path) -> None:
    from heber.writer.jetstream_consumer import JetStreamBindingError

    consumer = JetStreamEventConsumer()
    with pytest.raises(JetStreamBindingError, match="HEBER_LIVE"):
        await consumer.connect()


async def test_messages_held_past_ack_wait_are_not_redelivered_or_duplicated(
    receiver_settings: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A barrier that keeps failing holds messages across many pulls for longer than ack_wait."""
    import heber.writer.jetstream_consumer as module

    monkeypatch.setattr(settings, "jetstream_ack_wait_seconds", 10)
    monkeypatch.setattr(settings, "jetstream_ack_barrier_seconds", 5.0)
    monkeypatch.setattr(settings, "jetstream_reconnect_backoff_seconds", 0.1)
    monkeypatch.setattr(module, "_HEARTBEAT_TICK_SECONDS", 0.5)
    gateway = await _gateway(settings.nats_url)
    await _publish(gateway, gateway_envelope())

    consumer = JetStreamEventConsumer()
    await consumer.connect()
    real_flush_partition = consumer.bronze_writer._flush_partition

    def broken_flush(*_: Any, **__: Any) -> None:
        raise OSError("disk gone")

    monkeypatch.setattr(consumer.bronze_writer, "_flush_partition", broken_flush)
    deadline = time.monotonic() + 13  # longer than ack_wait
    while time.monotonic() < deadline:
        await consumer._consume_iteration()
    assert len(consumer._held) == 1

    manager = gateway.jsm()
    assert (await manager.consumer_info("HEBER_LIVE", "heber-live-writers")).num_redelivered == 0

    monkeypatch.setattr(consumer.bronze_writer, "_flush_partition", real_flush_partition)
    await consumer._ack_barrier(force=True)
    info = await manager.consumer_info("HEBER_LIVE", "heber-live-writers")
    assert info.num_ack_pending == 0
    assert len(bronze_rows(receiver_settings)) == 1
    await consumer._shutdown()
    await gateway.close()
