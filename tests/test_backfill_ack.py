"""Backfill acknowledgements must prove the exact durably committed chunk."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from heber.models.envelope import EventEnvelope
from heber.writer.backfill_ack import (
    BackfillAckWriter,
    BackfillProofMismatch,
    backfill_event_proof,
    proof_digests,
)
from heber.writer.consumer import EventConsumer

pytestmark = pytest.mark.unit

NOW = datetime(2026, 7, 29, 14, 30, tzinfo=UTC)


def _envelope(*, source: str = "backfill", lineage: dict | None = None) -> EventEnvelope:
    return EventEnvelope(
        event_id="event-a",
        provider="alpaca",
        feed="bars",
        source=source,
        instrument_type="equity",
        instrument_key="equity:AAPL",
        symbol="AAPL",
        ts_event=NOW,
        ts_ingest=NOW,
        payload={"close": "101.5", "nested": {"b": 2, "a": 1}},
        lineage=lineage or {},
    )


def _lineage() -> dict:
    payload_digest = hashlib.sha256(
        json.dumps(
            _envelope().payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode()
    ).hexdigest()
    event_ids_digest, records_digest = proof_digests({"event-a": payload_digest})
    return {
        "backfill_job_id": "bf-job",
        "backfill_chunk_id": "chunk-1",
        "backfill_manifest_hash": "manifest",
        "backfill_expected_record_count": 1,
        "backfill_expected_event_ids_sha256": event_ids_digest,
        "backfill_expected_records_sha256": records_digest,
    }


def test_live_event_has_no_backfill_control_proof() -> None:
    assert backfill_event_proof(_envelope(source="websocket")) is None


def test_backfill_event_proof_uses_canonical_payload_digest() -> None:
    proof = backfill_event_proof(_envelope(lineage=_lineage()))

    assert proof is not None
    assert proof.job_id == "bf-job"
    assert proof.chunk_id == "chunk-1"
    assert proof.event_id == "event-a"
    assert proof_digests({proof.event_id: proof.payload_sha256}) == (
        proof.expected_event_ids_sha256,
        proof.expected_records_sha256,
    )


def test_backfill_event_missing_expected_proof_fails_closed() -> None:
    with pytest.raises(BackfillProofMismatch, match="missing backfill proof field"):
        backfill_event_proof(
            _envelope(
                lineage={
                    "backfill_job_id": "bf-job",
                    "backfill_chunk_id": "chunk-1",
                    "backfill_manifest_hash": "manifest",
                }
            )
        )


def test_backfill_event_count_above_configured_bound_fails_closed(monkeypatch) -> None:
    from heber.config import settings

    monkeypatch.setattr(settings, "backfill_proof_max_expected_records", 1)
    lineage = _lineage()
    lineage["backfill_expected_record_count"] = 2

    with pytest.raises(BackfillProofMismatch, match="exceeds configured maximum"):
        backfill_event_proof(_envelope(lineage=lineage))


def test_proof_digests_are_order_independent_and_payload_sensitive() -> None:
    first = {"event-b": "payload-b", "event-a": "payload-a"}

    assert proof_digests(first) == proof_digests(dict(reversed(list(first.items()))))
    assert proof_digests(first) != proof_digests({**first, "event-a": "changed"})


@pytest.mark.asyncio
async def test_ack_writer_returns_only_finalized_chunks_and_refreshes_transient_ttl() -> None:
    proof = backfill_event_proof(_envelope(lineage=_lineage()))
    assert proof is not None
    client = AsyncMock()
    client.eval.side_effect = [
        [b"pending", b"1"],
        [b"acked", b"1"],
    ]
    client.hgetall.return_value = {
        b"__manifest_hash": b"manifest",
        b"__expected_count": b"1",
        b"__event_ids_sha256": proof.expected_event_ids_sha256.encode(),
        b"__records_sha256": proof.expected_records_sha256.encode(),
        b"event-a": proof.payload_sha256.encode(),
    }
    writer = BackfillAckWriter(client, proof_ttl_seconds=3600)

    finalized = await writer.record_committed([proof], commit_id="commit", committed_at=NOW)

    assert finalized == {("bf-job", "chunk-1")}
    assert "redis.call('EXPIRE', KEYS[1], tonumber(ARGV[7]))" in client.eval.await_args_list[0].args[0]
    assert client.eval.await_args_list[0].args[10] == "3600"


@pytest.mark.asyncio
async def test_control_store_failure_leaves_transport_message_unacknowledged(
    tmp_path,
    monkeypatch,
) -> None:
    from heber.config import settings

    monkeypatch.setattr(settings, "data_root", tmp_path)
    consumer = EventConsumer()
    consumer.redis = AsyncMock()
    consumer._backfill_ack_writer = AsyncMock()
    consumer._backfill_ack_writer.record_committed.side_effect = ConnectionError("control store down")
    consumer._stage_backfill_proof(_envelope(lineage=_lineage()))
    consumer._pending_register_ids.add("event-a")
    consumer._hold_for_commit(["1-0"])
    monkeypatch.setattr(consumer, "_flush_layers", lambda: True)

    with pytest.raises(ConnectionError, match="control store down"):
        await consumer._settle_and_commit()

    consumer.redis.xack.assert_not_awaited()
    assert consumer._pending_ack_ids == {"1-0"}
    assert consumer._pending_register_ids == {"event-a"}


@pytest.mark.asyncio
async def test_redis_pending_backfill_waits_until_chunk_proof_finalizes(
    tmp_path,
    monkeypatch,
) -> None:
    from heber.config import settings

    monkeypatch.setattr(settings, "data_root", tmp_path)
    consumer = EventConsumer()
    consumer.redis = AsyncMock()
    consumer._pending_message_chunks = {
        "1-0": ("bf-job", "chunk-1"),
        "2-0": None,
    }
    consumer._hold_for_commit(["1-0", "2-0"])
    prepare = AsyncMock(side_effect=[set(), {("bf-job", "chunk-1")}])
    monkeypatch.setattr(consumer, "_prepare_durable_commit", prepare)

    await consumer._settle_and_commit()

    consumer.redis.xack.assert_awaited_once_with(
        settings.redis_stream_name,
        settings.redis_consumer_group,
        "2-0",
    )
    assert consumer._pending_ack_ids == {"1-0"}
    assert consumer._pending_message_chunks == {"1-0": ("bf-job", "chunk-1")}

    await consumer._settle_and_commit()

    assert consumer.redis.xack.await_args_list[-1].args == (
        settings.redis_stream_name,
        settings.redis_consumer_group,
        "1-0",
    )
    assert consumer._pending_ack_ids == set()
    assert consumer._pending_message_chunks == {}


@pytest.mark.asyncio
async def test_redis_retries_finalized_chunk_after_transient_xack_failure(
    tmp_path,
    monkeypatch,
) -> None:
    from heber.config import settings

    monkeypatch.setattr(settings, "data_root", tmp_path)
    consumer = EventConsumer()
    consumer.redis = AsyncMock()
    consumer.redis.xack.side_effect = [ConnectionError("redis down"), 1]
    consumer._pending_message_chunks = {"1-0": ("bf-job", "chunk-1")}
    consumer._hold_for_commit(["1-0"])
    prepare = AsyncMock(side_effect=[{("bf-job", "chunk-1")}, set()])
    monkeypatch.setattr(consumer, "_prepare_durable_commit", prepare)

    with pytest.raises(ConnectionError, match="redis down"):
        await consumer._settle_and_commit()

    assert consumer._pending_ack_ids == {"1-0"}
    assert consumer._transport_ack_eligible_chunks == {("bf-job", "chunk-1")}

    await consumer._settle_and_commit()

    assert consumer.redis.xack.await_count == 2
    assert consumer._pending_ack_ids == set()
    assert consumer._pending_message_chunks == {}
    assert consumer._transport_ack_eligible_chunks == set()


@pytest.mark.asyncio
async def test_backfill_consumer_writes_true_readiness_only_after_all_checks(monkeypatch) -> None:
    from heber.config import settings

    monkeypatch.setattr(settings, "ingest_lane", "backfill")
    consumer = EventConsumer()
    consumer.redis = AsyncMock()
    consumer.redis.eval.return_value = b"PONG"
    consumer._backfill_binding = (
        "redis",
        "backfill",
        settings.redis_stream_name,
        settings.redis_consumer_group,
    )
    monkeypatch.setattr(consumer, "_backfill_consumer_bound", lambda: True)
    durable_probe = AsyncMock()
    monkeypatch.setattr(consumer, "_check_backfill_writer_durability", durable_probe)

    await consumer._write_backfill_readiness()

    durable_probe.assert_awaited_once()
    args = consumer.redis.hset.await_args
    assert args.args == ("gateway:backfill:heber:readiness:v1",)
    assert args.kwargs["mapping"]["consumer_healthy"] == "true"
    assert args.kwargs["mapping"]["writer_healthy"] == "true"
    assert args.kwargs["mapping"]["ack_store_ready"] == "true"
    assert args.kwargs["mapping"]["protocol_version"] == "1"
    assert args.kwargs["mapping"]["transport"] == "redis"
    assert args.kwargs["mapping"]["lane"] == "backfill"
    assert args.kwargs["mapping"]["stream"] == settings.redis_stream_name
    assert args.kwargs["mapping"]["durable_consumer"] == settings.redis_consumer_group


@pytest.mark.asyncio
async def test_backfill_consumer_writes_false_readiness_when_writer_probe_fails(monkeypatch) -> None:
    consumer = EventConsumer()
    consumer.redis = AsyncMock()
    consumer.redis.eval.return_value = b"PONG"
    monkeypatch.setattr(consumer, "_backfill_consumer_bound", lambda: True)
    monkeypatch.setattr(
        consumer,
        "_check_backfill_writer_durability",
        AsyncMock(side_effect=OSError("read only")),
    )

    with pytest.raises(OSError, match="read only"):
        await consumer._write_backfill_readiness()

    mapping = consumer.redis.hset.await_args.kwargs["mapping"]
    assert mapping["consumer_healthy"] == "true"
    assert mapping["writer_healthy"] == "false"
    assert mapping["ack_store_ready"] == "true"


@pytest.mark.asyncio
async def test_backfill_readiness_creates_fresh_writer_directories_durably(
    tmp_path,
    monkeypatch,
) -> None:
    from heber.config import settings

    data_root = tmp_path / "data"
    data_root.mkdir()
    monkeypatch.setattr(settings, "data_root", data_root)
    fsynced = []
    monkeypatch.setattr("heber.writer.durability._fsync_directory", fsynced.append)

    await EventConsumer()._check_backfill_writer_durability()

    assert settings.bronze_path.is_dir()
    assert settings.silver_path.is_dir()
    assert fsynced == [data_root, data_root]


def test_final_backfill_ack_key_has_no_expiration_command() -> None:
    from heber.writer.backfill_ack import _FINALIZE_SCRIPT

    assert "EXPIRE', KEYS[2]" not in _FINALIZE_SCRIPT
    assert "PEXPIRE', KEYS[2]" not in _FINALIZE_SCRIPT
