"""Durable receipt capacity and operator-only maintenance contracts."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier

import pytest

from heber.config import settings
from heber.writer.backfill_ack import BackfillEventProof, proof_digests
from heber.writer.event_receipts import DurableEventReceipts


def test_receipts_are_lane_scoped_and_capacity_fails_closed(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(settings, "ingest_lane", "live")
    monkeypatch.setattr(settings, "durable_event_receipt_max_rows", 1)
    live = DurableEventReceipts(tmp_path)
    live.record({"one"})
    with pytest.raises(RuntimeError, match="capacity exhausted"):
        live.record({"two"})

    monkeypatch.setattr(settings, "ingest_lane", "backfill")
    backfill = DurableEventReceipts(tmp_path)
    backfill.record({"two"})
    assert live.path != backfill.path
    assert backfill.contains("two")


def test_watch_receipts_use_a_physical_watch_lane_database(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(settings, "ingest_lane", "live")
    live = DurableEventReceipts(tmp_path)
    watch = DurableEventReceipts(tmp_path, lane="watch")

    assert live.path.name == "event_receipts-live.sqlite"
    assert watch.path.name == "event_receipts-watch.sqlite"
    assert live.path != watch.path


def test_split_backfill_chunk_finalizes_durably_after_restart(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(settings, "ingest_lane", "backfill")
    event_digest, record_digest = proof_digests({"event-a": "hash-a", "event-b": "hash-b"})
    common = dict(
        job_id="job",
        chunk_id="chunk",
        manifest_hash="manifest",
        expected_count=2,
        expected_event_ids_sha256=event_digest,
        expected_records_sha256=record_digest,
    )
    first = BackfillEventProof(event_id="event-a", payload_sha256="hash-a", **common)
    second = BackfillEventProof(event_id="event-b", payload_sha256="hash-b", **common)
    receipts = DurableEventReceipts(tmp_path)
    receipts.record_finalized_backfill_chunks([first], "commit-1")
    assert receipts.connection.execute("SELECT COUNT(*) FROM committed_backfill_chunks").fetchone()[0] == 0

    restarted = DurableEventReceipts(tmp_path)
    restarted.record_committed_backfill_outcomes([first, second])
    restarted.record_finalized_backfill_chunks([second], "commit-2")
    row = restarted.connection.execute(
        "SELECT commit_id, record_count, manifest_hash, event_ids_sha256, records_sha256, finalized_at "
        "FROM committed_backfill_chunks WHERE job_id = 'job' AND chunk_id = 'chunk'"
    ).fetchone()
    assert row[:5] == ("commit-2", 2, "manifest", event_digest, record_digest)
    assert row[5] > 0
    assert restarted.backfill_proof_is_finalized(first) is True

    altered = replace(first, manifest_hash="altered-manifest")
    with pytest.raises(RuntimeError, match="proof conflict"):
        restarted.validate_backfill_proof(altered)
    assert restarted.backfill_proof_is_finalized(altered) is False


def test_backfill_ledger_capacity_fails_closed_without_partial_rows(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(settings, "ingest_lane", "backfill")
    monkeypatch.setattr(settings, "durable_backfill_ledger_max_rows", 1)
    first_event_digest, first_record_digest = proof_digests({"event-a": "hash-a"})
    second_event_digest, second_record_digest = proof_digests({"event-b": "hash-b"})
    first = BackfillEventProof(
        job_id="job-a",
        chunk_id="chunk-a",
        manifest_hash="manifest-a",
        expected_count=1,
        expected_event_ids_sha256=first_event_digest,
        expected_records_sha256=first_record_digest,
        event_id="event-a",
        payload_sha256="hash-a",
    )
    second = BackfillEventProof(
        job_id="job-b",
        chunk_id="chunk-b",
        manifest_hash="manifest-b",
        expected_count=1,
        expected_event_ids_sha256=second_event_digest,
        expected_records_sha256=second_record_digest,
        event_id="event-b",
        payload_sha256="hash-b",
    )
    receipts = DurableEventReceipts(tmp_path)

    receipts.record_backfill_commit([first], "commit-a")
    with pytest.raises(RuntimeError, match="backfill durable ledger capacity exhausted"):
        receipts.record_backfill_commit([second], "commit-b")

    assert receipts.backfill_ledger_stats() == {"pending": 1, "outcomes": 1, "chunks": 1}
    assert (
        receipts.connection.execute(
            "SELECT COUNT(*) FROM pending_backfill_proofs WHERE event_id = ?", (b"event-b",)
        ).fetchone()[0]
        == 0
    )
    assert (
        receipts.connection.execute(
            "SELECT COUNT(*) FROM committed_backfill_outcomes WHERE event_id = ?", (b"event-b",)
        ).fetchone()[0]
        == 0
    )


def test_confirmed_broker_ack_reclaims_only_its_backfill_ambiguity_evidence(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(settings, "ingest_lane", "backfill")
    event_digest, record_digest = proof_digests({"event-a": "hash-a", "event-b": "hash-b"})
    common = dict(
        job_id="job",
        chunk_id="chunk",
        manifest_hash="manifest",
        expected_count=2,
        expected_event_ids_sha256=event_digest,
        expected_records_sha256=record_digest,
    )
    first = BackfillEventProof(event_id="event-a", payload_sha256="hash-a", **common)
    second = BackfillEventProof(event_id="event-b", payload_sha256="hash-b", **common)
    receipts = DurableEventReceipts(tmp_path)
    receipts.record({"event-a", "event-b"})
    receipts.record_stream_sequences({"event-a": 7, "event-b": 9})
    receipts.record_backfill_commit([first, second], "commit")
    receipts.delete_pending_backfill_proofs([first, second])

    receipts.confirm_broker_ack("event-a")

    assert not receipts.contains("event-a")
    assert receipts.contains("event-b")
    assert receipts.backfill_proof_is_finalized(first) is False
    assert receipts.backfill_proof_is_finalized(second) is True
    assert receipts.backfill_ledger_stats() == {"pending": 0, "outcomes": 1, "chunks": 1}

    assert receipts.delete_confirmed_stream_sequences(9) == 1
    assert receipts.backfill_ledger_stats() == {"pending": 0, "outcomes": 0, "chunks": 0}


def test_ack_floor_cleanup_is_indexed_bounded_and_repeatable(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(settings, "redis_read_batch_size", 2)
    receipts = DurableEventReceipts(tmp_path, lane="live")
    receipts.record({"event-a", "event-b", "event-c", "event-pending"})
    receipts.record_stream_sequences({"event-a": 1, "event-b": 2, "event-c": 3, "event-pending": 4})

    indexes = {row[1] for row in receipts.connection.execute("PRAGMA index_list(committed_event_ids)")}
    assert "idx_committed_event_ids_stream_sequence" in indexes

    assert receipts.delete_confirmed_stream_sequences(3) == 2
    assert receipts.contains("event-pending")
    assert receipts.delete_confirmed_stream_sequences(3) == 1
    assert receipts.delete_confirmed_stream_sequences(3) == 0
    assert receipts.contains("event-pending")


def test_pending_redis_projection_does_not_recreate_confirmed_ack_evidence(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(settings, "ingest_lane", "backfill")
    event_digest, record_digest = proof_digests({"event-a": "hash-a"})
    proof = BackfillEventProof(
        job_id="job",
        chunk_id="chunk",
        manifest_hash="manifest",
        expected_count=1,
        expected_event_ids_sha256=event_digest,
        expected_records_sha256=record_digest,
        event_id="event-a",
        payload_sha256="hash-a",
    )
    receipts = DurableEventReceipts(tmp_path)
    receipts.record_durable_commit({"event-a"}, [proof], "commit-1")
    receipts.confirm_broker_ack("event-a")

    receipts.record_durable_commit(set(), [proof], "commit-2")

    assert receipts.backfill_ledger_stats() == {"pending": 1, "outcomes": 0, "chunks": 0}


def test_watch_ack_floor_cleanup_is_indexed_bounded_and_repeatable(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(settings, "redis_read_batch_size", 2)
    receipts = DurableEventReceipts(tmp_path, lane="watch")
    receipts.store_pending_watch_message("acked-a", b"{}", 1)
    receipts.store_pending_watch_message("acked-b", b"{}", 2)
    receipts.store_pending_watch_message("acked-c", b"{}", 3)
    receipts.store_pending_watch_message("pending", b"{}", 4)

    indexes = {row[1] for row in receipts.connection.execute("PRAGMA index_list(pending_watch_messages)")}
    assert "idx_pending_watch_messages_stream_sequence" in indexes

    assert receipts.delete_confirmed_watch_messages(3) == 2
    assert set(receipts.load_pending_watch_messages()) == {"acked-c", "pending"}
    assert receipts.delete_confirmed_watch_messages(3) == 1
    assert receipts.delete_confirmed_watch_messages(3) == 0
    assert set(receipts.load_pending_watch_messages()) == {"pending"}


def test_watch_receipt_rejects_changed_payload_for_same_event_id(tmp_path) -> None:
    receipts = DurableEventReceipts(tmp_path, lane="watch")
    receipts.store_pending_watch_message("watch-1", b'{"payload":1}', 7)

    with pytest.raises(RuntimeError, match="watch event payload conflict"):
        receipts.store_pending_watch_message("watch-1", b'{"payload":2}', 7)

    assert receipts.load_pending_watch_messages() == {"watch-1": b'{"payload":1}'}


def test_concurrent_watch_receipts_preserve_exactly_one_payload(tmp_path) -> None:
    first = DurableEventReceipts(tmp_path, lane="watch")
    second = DurableEventReceipts(tmp_path, lane="watch")
    barrier = Barrier(2)
    payloads = (b'{"payload":1}', b'{"payload":2}')

    def store(receipts: DurableEventReceipts, payload: bytes) -> tuple[str, bytes]:
        barrier.wait()
        try:
            receipts.store_pending_watch_message("watch-1", payload, 7)
        except RuntimeError as exc:
            assert str(exc) == "watch event payload conflict"
            return "conflict", payload
        return "stored", payload

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(lambda args: store(*args), zip((first, second), payloads, strict=True)))

    assert sorted(outcome for outcome, _payload in outcomes) == ["conflict", "stored"]
    winner = next(payload for outcome, payload in outcomes if outcome == "stored")
    assert first.load_pending_watch_messages() == {"watch-1": winner}
