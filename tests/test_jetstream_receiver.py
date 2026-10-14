"""JetStream receiver for the Bronze/Silver writer.

Wire contract under test (Data-Gateway master, commit 5adf17a):

* ``gateway/core/durable_outbox.py::SQLiteOutbox._serialize`` — the message body
  is ``json.dumps(envelope_dict, sort_keys=True, separators=(",", ":"),
  default=str)`` encoded as UTF-8, where ``envelope_dict`` is the
  ``EventEnvelope.model_dump(mode="json")`` output of ``wrap_event``.
* ``gateway/core/jetstream.py::JetStreamOutboxPublisher.__call__`` — the only
  header is ``Nats-Msg-Id`` set to the envelope's ``event_id``.
* ``gateway/core/jetstream.py::subject_for_entry`` / ``build_stream_configs`` —
  ``heber:events`` publishes to ``heber.live.<feed>`` on stream ``HEBER_LIVE``,
  ``heber:events:backfill`` to ``heber.backfill.<feed>`` on ``HEBER_BACKFILL``;
  both are WORK_QUEUE streams the gateway owns and provisions.

The broker is faked: these tests pin Heber's side of the contract (what it
binds to, when it acknowledges, what it dead-letters), not NATS itself.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import re
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, RetentionPolicy
from nats.js.errors import NotFoundError
from pydantic import SecretStr, ValidationError

from heber.config import Settings, settings
from heber.writer.consumer import EventConsumer, build_ingest_consumer
from heber.writer.jetstream_consumer import JetStreamBindingError, JetStreamEventConsumer

NOW = datetime.now(UTC).replace(microsecond=0)
TEST_NATS_PASSWORD = "test-only"  # pragma: allowlist secret — fake broker credential
_UNSAFE_SUBJECT_TOKEN = re.compile(r"[^A-Za-z0-9_-]+")


# ---------------------------------------------------------------------------
# Data-Gateway wire format (mirrors the sender; see module docstring)
# ---------------------------------------------------------------------------


def gateway_envelope(event_id: str = "a" * 32, *, feed: str = "bars", close: str = "100.5") -> dict[str, Any]:
    """An envelope shaped like ``wrap_event(...).model_dump(mode="json")``."""
    return {
        "event_id": event_id,
        "provider": "alpaca",
        "feed": feed,
        "source": "websocket",
        "instrument_type": "equity",
        "instrument_key": "equity:AAPL",
        "symbol": "AAPL",
        "ts_event": NOW.isoformat().replace("+00:00", "Z"),
        "ts_ingest": NOW.isoformat().replace("+00:00", "Z"),
        "schema_version": "v1",
        "lineage": {},
        "quality_flags": ["validated"],
        "payload": {
            "t": NOW.isoformat(),
            "o": "100",
            "h": "101",
            "l": "99",
            "c": close,
            "v": "1000",
        },
    }


def gateway_body(envelope: dict[str, Any]) -> bytes:
    return json.dumps(envelope, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")


def gateway_subject(lane: str, envelope: dict[str, Any]) -> str:
    token = _UNSAFE_SUBJECT_TOKEN.sub("_", str(envelope.get("feed") or "unknown")).strip("_") or "unknown"
    return f"heber.{lane}.{token}"


# ---------------------------------------------------------------------------
# Broker fakes
# ---------------------------------------------------------------------------


class FakeMsg:
    def __init__(
        self,
        data: bytes,
        seq: int,
        *,
        headers: dict[str, str] | None,
        subject: str,
        log: list[str],
        fail_ack: bool = False,
    ) -> None:
        self.data = data
        self.headers = headers
        self.subject = subject
        self.metadata = SimpleNamespace(sequence=SimpleNamespace(stream=seq, consumer=seq), num_delivered=1)
        self.acked = False
        self.naks: list[float | None] = []
        self.termed = False
        self.in_progress_calls = 0
        self._log = log
        self._fail_ack = fail_ack

    async def ack(self) -> None:
        if self._fail_ack:
            raise ConnectionError("broker gone")
        self.acked = True
        self._log.append(f"ack:{self.metadata.sequence.stream}")

    async def nak(self, delay: float | None = None) -> None:
        self.naks.append(delay)

    async def term(self) -> None:
        self.termed = True

    async def in_progress(self) -> None:
        self.in_progress_calls += 1


def wire_msg(
    envelope: dict[str, Any],
    seq: int,
    log: list[str],
    *,
    lane: str = "live",
    headers: dict[str, str] | None | object = ...,
    body: bytes | None = None,
    fail_ack: bool = False,
) -> FakeMsg:
    resolved_headers = {"Nats-Msg-Id": envelope["event_id"]} if headers is ... else headers
    return FakeMsg(
        gateway_body(envelope) if body is None else body,
        seq,
        headers=resolved_headers,  # type: ignore[arg-type]
        subject=gateway_subject(lane, envelope),
        log=log,
        fail_ack=fail_ack,
    )


class FakeSubscription:
    def __init__(self, batches: Iterable[list[FakeMsg]] = ()) -> None:
        self.batches = list(batches)
        self.fetch_calls: list[tuple[int, float | None]] = []

    async def fetch(self, batch: int = 1, timeout: float | None = 5) -> list[FakeMsg]:
        self.fetch_calls.append((batch, timeout))
        if not self.batches:
            raise NatsTimeoutError
        return self.batches.pop(0)


class FakeJetStreamManager:
    def __init__(self, *, streams: dict[str, list[str]], consumers: dict[tuple[str, str], Any] | None = None) -> None:
        self.streams = streams
        self.consumers = dict(consumers or {})
        self.added: list[tuple[str, Any]] = []
        self.retention = RetentionPolicy.WORK_QUEUE

    async def stream_info(self, name: str) -> Any:
        if name not in self.streams:
            raise NotFoundError
        return SimpleNamespace(config=SimpleNamespace(subjects=self.streams[name], retention=self.retention))

    async def consumer_info(self, stream: str, consumer: str) -> Any:
        if (stream, consumer) not in self.consumers:
            raise NotFoundError
        return SimpleNamespace(config=self.consumers[(stream, consumer)])

    async def add_consumer(self, stream: str, config: Any = None) -> Any:
        self.added.append((stream, config))
        self.consumers[(stream, config.durable_name)] = config
        return SimpleNamespace(config=config)


class FakeJetStream:
    def __init__(self, subscription: FakeSubscription) -> None:
        self.subscription = subscription
        self.bound: list[tuple[str | None, str | None]] = []

    async def pull_subscribe_bind(self, durable: str | None = None, stream: str | None = None, **_: Any) -> Any:
        self.bound.append((stream, durable))
        return self.subscription


class FakeConnection:
    def __init__(self, jsm: FakeJetStreamManager, js: FakeJetStream, log: list[str]) -> None:
        self._jsm = jsm
        self._js = js
        self._log = log
        self.is_connected = True
        self.is_closed = False

    def jetstream(self, **_: Any) -> FakeJetStream:
        return self._js

    def jsm(self, **_: Any) -> FakeJetStreamManager:
        return self._jsm

    async def flush(self, timeout: float = 10) -> None:
        self._log.append("nats_flush")

    async def close(self) -> None:
        self.is_closed = True
        self.is_connected = False
        self._log.append("nats_close")


class Broker:
    """One fake broker per test: records connect kwargs and hands out the fakes."""

    def __init__(self, lane: str = "live", *, subscription: FakeSubscription | None = None) -> None:
        self.log: list[str] = []
        self.subscription = subscription or FakeSubscription()
        stream = "HEBER_LIVE" if lane == "live" else "HEBER_BACKFILL"
        self.jsm = FakeJetStreamManager(streams={stream: [f"heber.{lane}.>"]})
        self.js = FakeJetStream(self.subscription)
        self.connections: list[FakeConnection] = []
        self.connect_kwargs: list[dict[str, Any]] = []

    async def connect(self, **kwargs: Any) -> FakeConnection:
        self.connect_kwargs.append(kwargs)
        connection = FakeConnection(self.jsm, self.js, self.log)
        self.connections.append(connection)
        return connection


@pytest.fixture
def jetstream_settings(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    monkeypatch.setattr(settings, "data_root", tmp_path)
    monkeypatch.setattr(settings, "ingest_transport", "jetstream")
    monkeypatch.setattr(settings, "ingest_lane", "live")
    monkeypatch.setattr(settings, "nats_username", "heber")
    monkeypatch.setattr(settings, "nats_password", SecretStr(TEST_NATS_PASSWORD))
    monkeypatch.setattr(settings, "redis_retry_backoff_seconds", 0.0)
    return tmp_path


def bronze_rows(data_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted((data_root / "bronze").rglob("*.jsonl.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            rows.extend(json.loads(line) for line in handle if line.strip())
    return rows


def dlq_records(data_root: Path) -> list[dict[str, Any]]:
    return [json.loads(path.read_text()) for path in sorted((data_root / "dlq_fallback").rglob("*.json"))]


async def connected(broker: Broker) -> JetStreamEventConsumer:
    consumer = JetStreamEventConsumer(connector=broker.connect)
    await consumer.connect()
    return consumer


# ---------------------------------------------------------------------------
# Transport selection and settings
# ---------------------------------------------------------------------------


def test_redis_remains_the_default_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("HEBER_INGEST_TRANSPORT", raising=False)
    assert Settings().ingest_transport == "redis"
    monkeypatch.setattr(settings, "ingest_transport", "redis")
    consumer = build_ingest_consumer()
    assert type(consumer) is EventConsumer


def test_jetstream_transport_builds_the_jetstream_receiver(jetstream_settings: Path) -> None:
    assert isinstance(build_ingest_consumer(), JetStreamEventConsumer)


def test_jetstream_transport_requires_broker_credentials() -> None:
    with pytest.raises(ValidationError, match="nats_username and nats_password"):
        Settings(ingest_transport="jetstream", nats_username="", nats_password=SecretStr(""))


def test_ack_wait_must_leave_room_for_the_ack_barrier() -> None:
    with pytest.raises(ValidationError, match="jetstream_ack_wait_seconds"):
        Settings(
            ingest_transport="jetstream",
            nats_username="heber",
            nats_password=SecretStr(TEST_NATS_PASSWORD),
            jetstream_ack_barrier_seconds=60,
            jetstream_ack_wait_seconds=90,
        )


def test_max_ack_pending_must_cover_held_messages_plus_one_fetch() -> None:
    with pytest.raises(ValidationError, match="jetstream_max_ack_pending"):
        Settings(
            ingest_transport="jetstream",
            nats_username="heber",
            nats_password=SecretStr(TEST_NATS_PASSWORD),
            jetstream_max_held_messages=10_000,
            jetstream_fetch_batch_size=500,
            jetstream_max_ack_pending=10_000,
        )


@pytest.mark.parametrize(
    ("lane", "stream", "durable", "subject"),
    [
        ("live", "HEBER_LIVE", "heber-live-writers", "heber.live.>"),
        ("backfill", "HEBER_BACKFILL", "heber-backfill-writers", "heber.backfill.>"),
    ],
)
def test_lane_selects_the_gateway_stream_and_durable(lane: str, stream: str, durable: str, subject: str) -> None:
    configured = Settings(ingest_lane=lane)
    assert configured.jetstream_stream_name == stream
    assert configured.jetstream_durable_name == durable
    assert configured.jetstream_filter_subject == subject


# ---------------------------------------------------------------------------
# Binding: bind to the gateway's streams, never create or mutate them
# ---------------------------------------------------------------------------


async def test_connect_creates_missing_durable_with_explicit_ack_and_lane_filter(jetstream_settings: Path) -> None:
    broker = Broker()
    await connected(broker)

    assert broker.connect_kwargs[0]["user"] == "heber"
    assert broker.connect_kwargs[0]["password"] == TEST_NATS_PASSWORD
    assert len(broker.jsm.added) == 1
    stream, config = broker.jsm.added[0]
    assert stream == "HEBER_LIVE"
    assert config.durable_name == "heber-live-writers"
    assert config.filter_subject == "heber.live.>"
    assert config.ack_policy == AckPolicy.EXPLICIT
    assert config.ack_wait == settings.jetstream_ack_wait_seconds
    assert config.max_ack_pending == settings.jetstream_max_ack_pending
    assert broker.js.bound == [("HEBER_LIVE", "heber-live-writers")]


async def test_connect_reuses_a_matching_durable_without_touching_it(jetstream_settings: Path) -> None:
    broker = Broker()
    broker.jsm.consumers[("HEBER_LIVE", "heber-live-writers")] = SimpleNamespace(
        durable_name="heber-live-writers",
        filter_subject="heber.live.>",
        filter_subjects=None,
        ack_policy=AckPolicy.EXPLICIT,
        ack_wait=float(settings.jetstream_ack_wait_seconds),
        max_ack_pending=settings.jetstream_max_ack_pending,
    )
    await connected(broker)
    assert broker.jsm.added == []
    assert broker.js.bound == [("HEBER_LIVE", "heber-live-writers")]


async def test_connect_fails_fast_when_the_gateway_stream_is_missing(jetstream_settings: Path) -> None:
    broker = Broker()
    broker.jsm.streams = {}
    with pytest.raises(JetStreamBindingError, match="HEBER_LIVE"):
        await connected(broker)
    assert broker.jsm.added == []
    assert broker.connections[0].is_closed


async def test_connect_fails_fast_when_the_stream_does_not_carry_the_lane(jetstream_settings: Path) -> None:
    broker = Broker()
    broker.jsm.streams = {"HEBER_LIVE": ["heber.backfill.>"]}
    with pytest.raises(JetStreamBindingError, match="subjects"):
        await connected(broker)


async def test_connect_fails_fast_on_a_non_work_queue_stream(jetstream_settings: Path) -> None:
    broker = Broker()
    broker.jsm.retention = RetentionPolicy.LIMITS
    with pytest.raises(JetStreamBindingError, match="retention"):
        await connected(broker)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("filter_subject", "heber.live.bars"),
        ("ack_policy", AckPolicy.NONE),
        ("ack_wait", 30.0),
        ("max_ack_pending", 1),
    ],
)
async def test_connect_refuses_a_durable_whose_config_drifted(jetstream_settings: Path, field: str, value: Any) -> None:
    config = SimpleNamespace(
        durable_name="heber-live-writers",
        filter_subject="heber.live.>",
        filter_subjects=None,
        ack_policy=AckPolicy.EXPLICIT,
        ack_wait=float(settings.jetstream_ack_wait_seconds),
        max_ack_pending=settings.jetstream_max_ack_pending,
    )
    setattr(config, field, value)
    broker = Broker()
    broker.jsm.consumers[("HEBER_LIVE", "heber-live-writers")] = config
    with pytest.raises(JetStreamBindingError, match=field):
        await connected(broker)
    assert broker.jsm.added == []


async def test_backfill_lane_binds_the_backfill_stream(
    jetstream_settings: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "ingest_lane", "backfill")
    broker = Broker("backfill")
    consumer = await connected(broker)
    assert broker.js.bound == [("HEBER_BACKFILL", "heber-backfill-writers")]
    assert broker.jsm.added[0][1].filter_subject == "heber.backfill.>"
    assert consumer._is_backfill_source() is True


# ---------------------------------------------------------------------------
# Acknowledge only after the write is durable
# ---------------------------------------------------------------------------


async def test_gateway_message_is_written_to_bronze_and_acked_only_after_the_flush(
    jetstream_settings: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    broker = Broker()
    envelope = gateway_envelope()
    message = wire_msg(envelope, 1, broker.log)
    broker.subscription.batches = [[message]]
    consumer = await connected(broker)

    # Writers' own size/age thresholds are not met, so nothing is due yet.
    monkeypatch.setattr(settings, "bronze_flush_interval_seconds", 3600)
    monkeypatch.setattr(settings, "silver_max_flush_time_seconds", 3600)
    await consumer._consume_iteration()

    assert message.acked is False
    assert bronze_rows(jetstream_settings) == []

    # The barrier forces a full flush, then acknowledges.
    consumer._oldest_held_monotonic = 0.0
    await consumer._consume_iteration()

    rows = bronze_rows(jetstream_settings)
    assert [row["event_id"] for row in rows] == [envelope["event_id"]]
    assert message.acked is True
    assert consumer._held == {}


async def test_no_ack_when_the_flush_fails(jetstream_settings: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    broker = Broker()
    message = wire_msg(gateway_envelope(), 1, broker.log)
    broker.subscription.batches = [[message]]
    consumer = await connected(broker)

    def broken_flush(*_: Any, **__: Any) -> None:
        raise OSError("disk gone")

    monkeypatch.setattr(consumer.bronze_writer, "_flush_partition", broken_flush)
    consumer._oldest_held_monotonic = 0.0
    await consumer._consume_iteration()
    await consumer._ack_barrier(force=True)

    assert message.acked is False
    assert 1 in consumer._held


async def test_held_limit_forces_a_full_flush_barrier(
    jetstream_settings: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "bronze_flush_interval_seconds", 3600)
    monkeypatch.setattr(settings, "silver_max_flush_time_seconds", 3600)
    monkeypatch.setattr(settings, "jetstream_max_held_messages", 3)
    broker = Broker()
    messages = [wire_msg(gateway_envelope(f"{i:032x}"), i, broker.log) for i in (1, 2, 3)]
    broker.subscription.batches = [messages[:2], messages[2:]]
    consumer = await connected(broker)

    await consumer._consume_iteration()
    assert not any(message.acked for message in messages)

    await consumer._consume_iteration()
    assert all(message.acked for message in messages)
    assert len(bronze_rows(jetstream_settings)) == 3


async def test_pulls_pause_while_held_messages_cannot_be_made_durable(
    jetstream_settings: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "jetstream_max_held_messages", 1)
    monkeypatch.setattr(settings, "jetstream_reconnect_backoff_seconds", 0.1)
    broker = Broker()
    message = wire_msg(gateway_envelope(), 1, broker.log)
    broker.subscription.batches = [[message], [wire_msg(gateway_envelope("c" * 32), 2, broker.log)]]
    consumer = await connected(broker)

    def broken_flush(*_: Any, **__: Any) -> None:
        raise OSError("disk gone")

    monkeypatch.setattr(consumer.bronze_writer, "_flush_partition", broken_flush)
    await consumer._consume_iteration()
    fetches_before = len(broker.subscription.fetch_calls)
    await consumer._consume_iteration()

    assert len(broker.subscription.fetch_calls) == fetches_before
    assert message.acked is False


async def test_shutdown_flushes_then_acks_then_closes_the_connection(jetstream_settings: Path) -> None:
    broker = Broker()
    message = wire_msg(gateway_envelope(), 7, broker.log)
    broker.subscription.batches = [[message]]
    consumer = JetStreamEventConsumer(connector=broker.connect)

    async def stop_after_first_batch() -> None:
        while not consumer._held and not message.acked:
            await asyncio.sleep(0)
        await consumer.stop()

    await asyncio.gather(consumer.run(), stop_after_first_batch())

    assert message.acked is True
    assert len(bronze_rows(jetstream_settings)) == 1
    assert broker.log.index("ack:7") < broker.log.index("nats_close")


async def test_a_failed_ack_is_released_for_redelivery_without_raising(jetstream_settings: Path) -> None:
    broker = Broker()
    message = wire_msg(gateway_envelope(), 3, broker.log, fail_ack=True)
    broker.subscription.batches = [[message]]
    consumer = await connected(broker)
    consumer._oldest_held_monotonic = 0.0

    await consumer._consume_iteration()

    assert message.acked is False
    assert consumer._held == {}
    assert len(bronze_rows(jetstream_settings)) == 1


# ---------------------------------------------------------------------------
# Idempotency: redelivery and gateway re-publication
# ---------------------------------------------------------------------------


async def test_redelivered_sequence_is_not_reprocessed_and_the_latest_delivery_is_acked(
    jetstream_settings: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "bronze_flush_interval_seconds", 3600)
    monkeypatch.setattr(settings, "silver_max_flush_time_seconds", 3600)
    broker = Broker()
    envelope = gateway_envelope()
    first = wire_msg(envelope, 11, broker.log)
    redelivery = wire_msg(envelope, 11, broker.log)
    broker.subscription.batches = [[first], [redelivery]]
    consumer = await connected(broker)

    await consumer._consume_iteration()
    await consumer._consume_iteration()
    assert redelivery.in_progress_calls == 1

    await consumer._ack_barrier(force=True)
    assert redelivery.acked is True
    assert first.acked is False
    assert len(bronze_rows(jetstream_settings)) == 1


async def test_republished_event_id_on_a_new_sequence_is_deduped_but_still_acked(jetstream_settings: Path) -> None:
    broker = Broker()
    envelope = gateway_envelope()
    original = wire_msg(envelope, 1, broker.log)
    republished = wire_msg(envelope, 2, broker.log)
    broker.subscription.batches = [[original, republished]]
    consumer = await connected(broker)

    await consumer._consume_iteration()
    await consumer._ack_barrier(force=True)

    assert original.acked and republished.acked
    assert len(bronze_rows(jetstream_settings)) == 1


# ---------------------------------------------------------------------------
# Fail closed: malformed messages go to the durable DLQ, never silently acked
# ---------------------------------------------------------------------------


async def test_malformed_body_is_dead_lettered_to_disk_then_acked_at_the_barrier(jetstream_settings: Path) -> None:
    broker = Broker()
    message = wire_msg(gateway_envelope(), 5, broker.log, body=b"{not json")
    broker.subscription.batches = [[message]]
    consumer = await connected(broker)

    await consumer._consume_iteration()
    await consumer._ack_barrier(force=True)

    records = dlq_records(jetstream_settings)
    assert len(records) == 1
    assert records[0]["source_transport"] == "jetstream"
    assert records[0]["source_stream"] == "HEBER_LIVE"
    assert records[0]["source_group"] == "heber-live-writers"
    assert records[0]["source_message_id"] == "HEBER_LIVE:5"
    assert json.loads(records[0]["payload"])["data"] == "{not json"
    assert bronze_rows(jetstream_settings) == []
    assert message.acked is True
    assert message.termed is False


@pytest.mark.parametrize(
    ("headers", "reason"),
    [
        (None, "missing_nats_msg_id"),
        ({}, "missing_nats_msg_id"),
        ({"Nats-Msg-Id": "b" * 32}, "event_id_header_mismatch"),
    ],
)
async def test_message_id_header_must_match_the_payload_event_id(
    jetstream_settings: Path, headers: dict[str, str] | None, reason: str
) -> None:
    broker = Broker()
    message = wire_msg(gateway_envelope(), 9, broker.log, headers=headers)
    broker.subscription.batches = [[message]]
    consumer = await connected(broker)

    await consumer._consume_iteration()
    await consumer._ack_barrier(force=True)

    records = dlq_records(jetstream_settings)
    assert [record["error"].split(":", 1)[0] for record in records] == [reason]
    assert bronze_rows(jetstream_settings) == []
    assert message.acked is True


async def test_message_is_nakked_not_acked_when_the_dlq_cannot_be_written(
    jetstream_settings: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    broker = Broker()
    message = wire_msg(gateway_envelope(), 4, broker.log, body=b"{not json")
    broker.subscription.batches = [[message]]
    consumer = await connected(broker)

    def broken_dlq(*_: Any, **__: Any) -> Path:
        raise OSError("dlq volume gone")

    monkeypatch.setattr("heber.writer.jetstream_consumer.write_dlq_fallback_file", broken_dlq)
    await consumer._consume_iteration()
    await consumer._ack_barrier(force=True)

    assert message.acked is False
    assert message.naks == [settings.jetstream_reconnect_backoff_seconds]
    assert 4 not in consumer._held


# ---------------------------------------------------------------------------
# Connection loss
# ---------------------------------------------------------------------------


async def test_closed_connection_is_rebound_after_a_fetch_failure(jetstream_settings: Path) -> None:
    broker = Broker()
    consumer = await connected(broker)

    async def broken_fetch(*_: Any, **__: Any) -> list[FakeMsg]:
        broker.connections[-1].is_closed = True
        raise ConnectionError("connection closed")

    broker.subscription.fetch = broken_fetch  # type: ignore[method-assign]
    with pytest.raises(ConnectionError):
        await consumer._consume_iteration()
    await consumer._recover_connection()

    assert len(broker.connections) == 2
    assert broker.connections[0].is_closed
    assert broker.js.bound == [("HEBER_LIVE", "heber-live-writers")] * 2


def test_redis_consumer_backfill_detection_is_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer = EventConsumer()
    monkeypatch.setattr(settings, "redis_stream_name", "heber:events")
    assert consumer._is_backfill_source() is False
    monkeypatch.setattr(settings, "redis_stream_name", settings.redis_backfill_stream_name)
    assert consumer._is_backfill_source() is True
