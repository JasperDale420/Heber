"""Durable event-id receipts written before a broker acknowledgement."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import asdict
from json import loads
from pathlib import Path
from threading import Lock
from typing import TYPE_CHECKING

from heber.config import settings
from heber.writer.durability import create_durable_directory

if TYPE_CHECKING:
    from heber.writer.backfill_ack import BackfillEventProof


class DurableEventReceipts:
    """Exact event-id receipts independent of Redis-backed deduplication."""

    def __init__(self, data_root: Path, *, lane: str | None = None) -> None:
        receipt_dir = data_root / "_ingest_commits"
        create_durable_directory(receipt_dir, root=data_root)
        # A writer lane is the durability/ACK ownership boundary; sharing an
        # unbounded index lets historical backfill exhaust live ingress state.
        self.lane = lane or settings.ingest_lane
        if self.lane not in {"live", "backfill", "watch"}:
            raise ValueError(f"unsupported durable receipt lane: {self.lane}")
        self.path = receipt_dir / f"event_receipts-{self.lane}.sqlite"
        self.connection = sqlite3.connect(self.path, check_same_thread=False)
        self._watch_lock = Lock()
        self.connection.execute("PRAGMA journal_mode=DELETE")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS committed_event_ids ("
            "event_id BLOB PRIMARY KEY, committed_at INTEGER NOT NULL, stream_sequence INTEGER) WITHOUT ROWID"
        )
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS pending_watch_messages ("
            "event_id BLOB PRIMARY KEY, payload BLOB NOT NULL, stream_sequence INTEGER) WITHOUT ROWID"
        )
        watch_columns = {row[1] for row in self.connection.execute("PRAGMA table_info(pending_watch_messages)")}
        if "stream_sequence" not in watch_columns:
            self.connection.execute("ALTER TABLE pending_watch_messages ADD COLUMN stream_sequence INTEGER")
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_pending_watch_messages_stream_sequence "
            "ON pending_watch_messages(stream_sequence)"
        )
        columns = {row[1] for row in self.connection.execute("PRAGMA table_info(committed_event_ids)")}
        if "committed_at" not in columns:
            self.connection.execute(
                "ALTER TABLE committed_event_ids ADD COLUMN committed_at INTEGER NOT NULL DEFAULT 0"
            )
        if "stream_sequence" not in columns:
            self.connection.execute("ALTER TABLE committed_event_ids ADD COLUMN stream_sequence INTEGER")
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_committed_event_ids_stream_sequence ON committed_event_ids(stream_sequence)"
        )
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS pending_backfill_proofs ("
            "event_id BLOB PRIMARY KEY, proof_json TEXT NOT NULL, job_id TEXT, chunk_id TEXT) WITHOUT ROWID"
        )
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS committed_backfill_outcomes ("
            "event_id BLOB PRIMARY KEY, proof_json TEXT NOT NULL, job_id TEXT, chunk_id TEXT, "
            "projected_at INTEGER) WITHOUT ROWID"
        )
        for table in ("pending_backfill_proofs", "committed_backfill_outcomes"):
            proof_columns = {row[1] for row in self.connection.execute(f"PRAGMA table_info({table})")}
            if "job_id" not in proof_columns:
                self.connection.execute(f"ALTER TABLE {table} ADD COLUMN job_id TEXT")
            if "chunk_id" not in proof_columns:
                self.connection.execute(f"ALTER TABLE {table} ADD COLUMN chunk_id TEXT")
            for event_id, proof_json in self.connection.execute(
                f"SELECT event_id, proof_json FROM {table} WHERE job_id IS NULL OR chunk_id IS NULL"
            ):
                proof = loads(proof_json)
                self.connection.execute(
                    f"UPDATE {table} SET job_id = ?, chunk_id = ? WHERE event_id = ?",
                    (str(proof["job_id"]), str(proof["chunk_id"]), event_id),
                )
            self.connection.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_chunk ON {table}(job_id, chunk_id)")
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS committed_backfill_chunks ("
            "job_id TEXT NOT NULL, chunk_id TEXT NOT NULL, commit_id TEXT NOT NULL, record_count INTEGER NOT NULL, "
            "manifest_hash TEXT NOT NULL, event_ids_sha256 TEXT NOT NULL, records_sha256 TEXT NOT NULL, "
            "finalized_at INTEGER NOT NULL, "
            "projected_at INTEGER, "
            "PRIMARY KEY(job_id, chunk_id)) WITHOUT ROWID"
        )
        chunk_columns = {row[1] for row in self.connection.execute("PRAGMA table_info(committed_backfill_chunks)")}
        if "manifest_hash" not in chunk_columns:
            self.connection.execute(
                "ALTER TABLE committed_backfill_chunks ADD COLUMN manifest_hash TEXT NOT NULL DEFAULT ''"
            )
        self.connection.commit()

    def contains(self, event_id: str) -> bool:
        return (
            self.connection.execute(
                "SELECT 1 FROM committed_event_ids WHERE event_id = ?", (event_id.encode(),)
            ).fetchone()
            is not None
        )

    def record(self, event_ids: set[str]) -> None:
        with self.connection:
            self._record_event_ids(event_ids)

    def _record_event_ids(self, event_ids: set[str]) -> None:
        if not event_ids:
            return
        existing = {
            row[0]
            for row in self.connection.execute(
                f"SELECT event_id FROM committed_event_ids WHERE event_id IN ({','.join('?' for _ in event_ids)})",
                tuple(event_id.encode() for event_id in event_ids),
            )
        }
        new_count = len(event_ids) - len(existing)
        current_count = self.connection.execute("SELECT COUNT(*) FROM committed_event_ids").fetchone()[0]
        if current_count + new_count > settings.durable_event_receipt_max_rows:
            raise RuntimeError("durable event receipt capacity exhausted; refusing broker acknowledgement")
        self.connection.executemany(
            "INSERT OR IGNORE INTO committed_event_ids(event_id, committed_at) VALUES (?, ?)",
            ((event_id.encode(), int(time.time())) for event_id in event_ids),
        )

    def stats(self) -> dict[str, int]:
        rows, oldest = self.connection.execute("SELECT COUNT(*), MIN(committed_at) FROM committed_event_ids").fetchone()
        return {
            "rows": rows,
            "bytes": self.path.stat().st_size if self.path.exists() else 0,
            "oldest_age_seconds": 0 if oldest is None else max(0, int(time.time()) - oldest),
        }

    def store_pending_backfill_proofs(self, proofs: list[BackfillEventProof]) -> None:
        """Persist proof publication work before a JetStream ACK can release it."""
        with self.connection:
            self._store_pending_backfill_proofs(proofs)

    def record_committed_backfill_outcomes(self, proofs: list[BackfillEventProof]) -> None:
        with self.connection:
            self._record_committed_backfill_outcomes(proofs)

    def _ensure_backfill_capacity(self, table: str, proofs: list[BackfillEventProof]) -> None:
        if not proofs:
            return
        current = int(self.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        event_ids = {proof.event_id.encode() for proof in proofs}
        existing = int(
            self.connection.execute(
                f"SELECT COUNT(*) FROM {table} WHERE event_id IN ({','.join('?' for _ in event_ids)})",
                tuple(event_ids),
            ).fetchone()[0]
        )
        if current + len(event_ids) - existing > settings.durable_backfill_ledger_max_rows:
            raise RuntimeError("backfill durable ledger capacity exhausted; refusing broker acknowledgement")

    def _store_pending_backfill_proofs(self, proofs: list[BackfillEventProof]) -> None:
        self._ensure_backfill_capacity("pending_backfill_proofs", proofs)
        for proof in proofs:
            serialized = self._proof_json(proof)
            self._validate_backfill_proof_consistency(proof, serialized)
            self.connection.execute(
                "INSERT OR IGNORE INTO pending_backfill_proofs(event_id, proof_json, job_id, chunk_id) "
                "VALUES (?, ?, ?, ?)",
                (proof.event_id.encode(), serialized, proof.job_id, proof.chunk_id),
            )

    def _record_committed_backfill_outcomes(self, proofs: list[BackfillEventProof]) -> None:
        self._ensure_backfill_capacity("committed_backfill_outcomes", proofs)
        for proof in proofs:
            serialized = self._proof_json(proof)
            self._validate_backfill_proof_consistency(proof, serialized)
            self.connection.execute(
                "INSERT OR IGNORE INTO committed_backfill_outcomes(event_id, proof_json, job_id, chunk_id) "
                "VALUES (?, ?, ?, ?)",
                (proof.event_id.encode(), serialized, proof.job_id, proof.chunk_id),
            )

    @staticmethod
    def _proof_json(proof: BackfillEventProof) -> str:
        import json

        return json.dumps(asdict(proof), sort_keys=True)

    def _validate_backfill_proof_consistency(self, proof: BackfillEventProof, serialized: str) -> None:
        for table in ("pending_backfill_proofs", "committed_backfill_outcomes"):
            existing = self.connection.execute(
                f"SELECT proof_json FROM {table} WHERE event_id = ?", (proof.event_id.encode(),)
            ).fetchone()
            if existing is not None and existing[0] != serialized:
                raise RuntimeError("backfill event proof conflict")

    def validate_backfill_proof(self, proof: BackfillEventProof) -> None:
        """Reject a redelivery whose event proof differs from the durable ledger."""
        self._validate_backfill_proof_consistency(proof, self._proof_json(proof))

    def backfill_proof_is_finalized(self, proof: BackfillEventProof) -> bool:
        """Return whether this exact event proof is committed in its exact finalized chunk."""
        outcome = self.connection.execute(
            "SELECT proof_json FROM committed_backfill_outcomes WHERE event_id = ?", (proof.event_id.encode(),)
        ).fetchone()
        if outcome is None or outcome[0] != self._proof_json(proof):
            return False
        chunk = self.connection.execute(
            "SELECT manifest_hash, record_count, event_ids_sha256, records_sha256 "
            "FROM committed_backfill_chunks WHERE job_id = ? AND chunk_id = ?",
            (proof.job_id, proof.chunk_id),
        ).fetchone()
        if chunk is None:
            return False
        return bool(
            str(chunk[0]) == proof.manifest_hash
            and int(chunk[1]) == proof.expected_count
            and str(chunk[2]) == proof.expected_event_ids_sha256
            and str(chunk[3]) == proof.expected_records_sha256
        )

    def mark_backfill_outcomes_projected(self, proofs: list[BackfillEventProof]) -> None:
        with self.connection:
            self.connection.executemany(
                "UPDATE committed_backfill_outcomes SET projected_at = ? WHERE event_id = ?",
                ((int(time.time()), proof.event_id.encode()) for proof in proofs),
            )

    def record_finalized_backfill_chunks(
        self, proofs: list[BackfillEventProof], commit_id: str
    ) -> set[tuple[str, str]]:
        """Persist final chunk authority before releasing its broker messages."""
        with self.connection:
            return self._record_finalized_backfill_chunks(proofs, commit_id)

    def _record_finalized_backfill_chunks(
        self, proofs: list[BackfillEventProof], commit_id: str
    ) -> set[tuple[str, str]]:
        from heber.writer.backfill_ack import proof_digests

        affected: set[tuple[str, str]] = set()
        for proof in proofs:
            affected.add((proof.job_id, proof.chunk_id))
        finalized: set[tuple[str, str]] = set()
        for job_id, chunk_id in affected:
            items = [
                self._proof_from_json(row[0])
                for row in self.connection.execute(
                    "SELECT proof_json FROM committed_backfill_outcomes WHERE job_id = ? AND chunk_id = ?",
                    (job_id, chunk_id),
                )
            ]
            if items:
                expected = items[0].expected_count
                if any(
                    p.manifest_hash != items[0].manifest_hash
                    or p.expected_count != expected
                    or p.expected_event_ids_sha256 != items[0].expected_event_ids_sha256
                    or p.expected_records_sha256 != items[0].expected_records_sha256
                    for p in items
                ):
                    raise RuntimeError("backfill chunk proof metadata conflict")
                records = {p.event_id: p.payload_sha256 for p in items}
                if len(records) > expected:
                    raise RuntimeError("backfill chunk proof count exceeded expected count")
                if len(records) != expected:
                    continue
                event_digest, record_digest = proof_digests(records)
                if (
                    event_digest != items[0].expected_event_ids_sha256
                    or record_digest != items[0].expected_records_sha256
                ):
                    raise RuntimeError("backfill chunk proof digest mismatch")
                existing = self.connection.execute(
                    "SELECT manifest_hash, record_count, event_ids_sha256, records_sha256 "
                    "FROM committed_backfill_chunks WHERE job_id = ? AND chunk_id = ?",
                    (job_id, chunk_id),
                ).fetchone()
                expected_metadata = (
                    items[0].manifest_hash,
                    expected,
                    event_digest,
                    record_digest,
                )
                if existing is not None and existing != expected_metadata:
                    raise RuntimeError("backfill finalized chunk metadata conflict")
                chunk_count = int(
                    self.connection.execute("SELECT COUNT(*) FROM committed_backfill_chunks").fetchone()[0]
                )
                if existing is None and chunk_count >= settings.durable_backfill_ledger_max_rows:
                    raise RuntimeError("backfill durable ledger capacity exhausted; refusing broker acknowledgement")
                self.connection.execute(
                    "INSERT OR IGNORE INTO committed_backfill_chunks "
                    "(job_id, chunk_id, commit_id, record_count, manifest_hash, event_ids_sha256, "
                    "records_sha256, finalized_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        job_id,
                        chunk_id,
                        commit_id,
                        expected,
                        items[0].manifest_hash,
                        event_digest,
                        record_digest,
                        int(time.time()),
                    ),
                )
                finalized.add((job_id, chunk_id))
        return finalized

    @staticmethod
    def _proof_from_json(value: str) -> BackfillEventProof:
        from heber.writer.backfill_ack import BackfillEventProof

        return BackfillEventProof(**loads(value))

    def record_backfill_commit(self, proofs: list[BackfillEventProof], commit_id: str) -> set[tuple[str, str]]:
        """Atomically retain proof publication, outcomes, and affected chunk authority."""
        with self.connection:
            self._store_pending_backfill_proofs(proofs)
            self._record_committed_backfill_outcomes(proofs)
            return self._record_finalized_backfill_chunks(proofs, commit_id)

    def record_durable_commit(
        self,
        event_ids: set[str],
        proofs: list[BackfillEventProof],
        commit_id: str,
    ) -> set[tuple[str, str]]:
        """Atomically retain event receipts and any backfill ambiguity evidence."""
        with self.connection:
            self._record_event_ids(event_ids)
            self._store_pending_backfill_proofs(proofs)
            proof_ids = {proof.event_id.encode() for proof in proofs}
            receipt_ids = (
                {
                    bytes(row[0])
                    for row in self.connection.execute(
                        f"SELECT event_id FROM committed_event_ids "
                        f"WHERE event_id IN ({','.join('?' for _ in proof_ids)})",
                        tuple(proof_ids),
                    )
                }
                if proof_ids
                else set()
            )
            ambiguity_proofs = [proof for proof in proofs if proof.event_id.encode() in receipt_ids]
            self._record_committed_backfill_outcomes(ambiguity_proofs)
            return self._record_finalized_backfill_chunks(ambiguity_proofs, commit_id)

    def backfill_ledger_stats(self) -> dict[str, int]:
        return {
            "pending": int(self.connection.execute("SELECT COUNT(*) FROM pending_backfill_proofs").fetchone()[0]),
            "outcomes": int(self.connection.execute("SELECT COUNT(*) FROM committed_backfill_outcomes").fetchone()[0]),
            "chunks": int(self.connection.execute("SELECT COUNT(*) FROM committed_backfill_chunks").fetchone()[0]),
        }

    def mark_finalized_backfill_chunks_projected(self, chunks: set[tuple[str, str]]) -> None:
        with self.connection:
            self.connection.executemany(
                "UPDATE committed_backfill_chunks SET projected_at = ? WHERE job_id = ? AND chunk_id = ?",
                ((int(time.time()), job_id, chunk_id) for job_id, chunk_id in chunks),
            )

    def load_pending_backfill_proofs(self) -> list[BackfillEventProof]:
        import json

        from heber.writer.backfill_ack import BackfillEventProof

        rows = self.connection.execute("SELECT proof_json FROM pending_backfill_proofs")
        return [BackfillEventProof(**json.loads(row[0])) for row in rows]

    def delete_pending_backfill_proofs(self, proofs: list[BackfillEventProof]) -> None:
        with self.connection:
            self.connection.executemany(
                "DELETE FROM pending_backfill_proofs WHERE event_id = ?",
                ((proof.event_id.encode(),) for proof in proofs),
            )

    def record_stream_sequences(self, event_sequences: dict[str, int]) -> None:
        """Attach JetStream sequence proof before any broker acknowledgement."""
        with self.connection:
            for event_id, sequence in event_sequences.items():
                updated = self.connection.execute(
                    "UPDATE committed_event_ids SET stream_sequence = ? WHERE event_id = ?",
                    (sequence, event_id.encode()),
                ).rowcount
                if updated != 1:
                    raise RuntimeError("durable event receipt missing before broker acknowledgement")

    def _delete_confirmed_event(self, event_id: bytes) -> None:
        outcome = self.connection.execute(
            "SELECT job_id, chunk_id FROM committed_backfill_outcomes WHERE event_id = ?", (event_id,)
        ).fetchone()
        self.connection.execute("DELETE FROM committed_event_ids WHERE event_id = ?", (event_id,))
        self.connection.execute("DELETE FROM committed_backfill_outcomes WHERE event_id = ?", (event_id,))
        if outcome is not None:
            remaining = self.connection.execute(
                "SELECT 1 FROM committed_backfill_outcomes WHERE job_id = ? AND chunk_id = ? LIMIT 1",
                (outcome[0], outcome[1]),
            ).fetchone()
            if remaining is None:
                self.connection.execute(
                    "DELETE FROM committed_backfill_chunks WHERE job_id = ? AND chunk_id = ?",
                    (outcome[0], outcome[1]),
                )

    def confirm_broker_ack(self, event_id: str) -> None:
        """Reclaim ambiguity evidence only after a confirmed synchronous ACK."""
        self.confirm_broker_acks({event_id})

    def confirm_broker_acks(self, event_ids: set[str]) -> None:
        """Reclaim a confirmed ACK batch in one durable transaction."""
        with self.connection:
            for event_id in event_ids:
                self._delete_confirmed_event(event_id.encode())

    def delete_confirmed_stream_sequences(self, ack_floor_stream_sequence: int) -> int:
        """Reclaim receipts proven acknowledged by the durable consumer ACK floor."""
        with self.connection:
            event_ids = [
                bytes(row[0])
                for row in self.connection.execute(
                    "SELECT event_id FROM committed_event_ids "
                    "WHERE stream_sequence IS NOT NULL AND stream_sequence <= ? "
                    "ORDER BY stream_sequence LIMIT ?",
                    (ack_floor_stream_sequence, settings.redis_read_batch_size),
                )
            ]
            for event_id in event_ids:
                self._delete_confirmed_event(event_id)
            return len(event_ids)

    def store_pending_watch_message(self, event_id: str, payload: bytes, stream_sequence: int) -> None:
        with self._watch_lock, self.connection:
            self.connection.execute("BEGIN IMMEDIATE")
            existing = self.connection.execute(
                "SELECT payload FROM pending_watch_messages WHERE event_id = ?", (event_id.encode(),)
            ).fetchone()
            if existing is not None and bytes(existing[0]) != payload:
                raise RuntimeError("watch event payload conflict")
            count = self.connection.execute("SELECT COUNT(*) FROM pending_watch_messages").fetchone()[0]
            if existing is None and count >= settings.durable_watch_receipt_max_rows:
                raise RuntimeError("durable watch receipt capacity exhausted; refusing broker acknowledgement")
            if existing is None:
                self.connection.execute(
                    "INSERT INTO pending_watch_messages(event_id, payload, stream_sequence) VALUES (?, ?, ?)",
                    (event_id.encode(), payload, stream_sequence),
                )

    def load_pending_watch_messages(self) -> dict[str, bytes]:
        with self._watch_lock:
            return {
                bytes(row[0]).decode(): bytes(row[1])
                for row in self.connection.execute("SELECT event_id, payload FROM pending_watch_messages")
            }

    def delete_confirmed_watch_messages(self, ack_floor_stream_sequence: int) -> int:
        with self._watch_lock, self.connection:
            event_ids = [
                bytes(row[0])
                for row in self.connection.execute(
                    "SELECT event_id FROM pending_watch_messages "
                    "WHERE stream_sequence IS NOT NULL AND stream_sequence <= ? "
                    "ORDER BY stream_sequence, event_id LIMIT ?",
                    (ack_floor_stream_sequence, settings.redis_read_batch_size),
                )
            ]
            self.connection.executemany(
                "DELETE FROM pending_watch_messages WHERE event_id = ?",
                ((event_id,) for event_id in event_ids),
            )
            return len(event_ids)

    def delete_pending_watch_message(self, event_id: str) -> None:
        with self._watch_lock, self.connection:
            self.connection.execute("DELETE FROM pending_watch_messages WHERE event_id = ?", (event_id.encode(),))
