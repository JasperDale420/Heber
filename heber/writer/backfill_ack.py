"""Redis control-plane proofs for durably committed backfill chunks."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from heber.models.envelope import EventEnvelope

_META_FIELDS = {
    "__manifest_hash",
    "__expected_count",
    "__event_ids_sha256",
    "__records_sha256",
}

_ACCUMULATE_SCRIPT = """
local ack_status = redis.call('HGET', KEYS[2], 'status')
if ack_status then
  if ack_status ~= 'committed'
     or redis.call('HGET', KEYS[2], 'job_id') ~= ARGV[1]
     or redis.call('HGET', KEYS[2], 'chunk_id') ~= ARGV[2]
     or redis.call('HGET', KEYS[2], 'manifest_hash') ~= ARGV[3]
     or redis.call('HGET', KEYS[2], 'record_count') ~= ARGV[4]
     or redis.call('HGET', KEYS[2], 'event_ids_sha256') ~= ARGV[5]
     or redis.call('HGET', KEYS[2], 'records_sha256') ~= ARGV[6] then
    return redis.error_reply('backfill acknowledgement conflict')
  end
  redis.call('DEL', KEYS[1])
  return {'acked', ARGV[4]}
end

local metadata = {
  {'__manifest_hash', ARGV[3]},
  {'__expected_count', ARGV[4]},
  {'__event_ids_sha256', ARGV[5]},
  {'__records_sha256', ARGV[6]}
}
for _, item in ipairs(metadata) do
  local current = redis.call('HGET', KEYS[1], item[1])
  if current and current ~= item[2] then
    return redis.error_reply('backfill accumulator metadata conflict')
  end
  redis.call('HSET', KEYS[1], item[1], item[2])
end

for index = 8, #ARGV, 2 do
  local current = redis.call('HGET', KEYS[1], ARGV[index])
  if current and current ~= ARGV[index + 1] then
    return redis.error_reply('backfill event payload conflict')
  end
  redis.call('HSET', KEYS[1], ARGV[index], ARGV[index + 1])
end
local accumulated = redis.call('HLEN', KEYS[1]) - 4
if accumulated > tonumber(ARGV[4]) then
  return redis.error_reply('backfill committed event count exceeded expected count')
end
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[7]))
return {'pending', tostring(accumulated)}
"""

_FINALIZE_SCRIPT = """
local ack_status = redis.call('HGET', KEYS[2], 'status')
if ack_status then
  if ack_status ~= 'committed'
     or redis.call('HGET', KEYS[2], 'job_id') ~= ARGV[1]
     or redis.call('HGET', KEYS[2], 'chunk_id') ~= ARGV[2]
     or redis.call('HGET', KEYS[2], 'manifest_hash') ~= ARGV[3]
     or redis.call('HGET', KEYS[2], 'record_count') ~= ARGV[4]
     or redis.call('HGET', KEYS[2], 'event_ids_sha256') ~= ARGV[5]
     or redis.call('HGET', KEYS[2], 'records_sha256') ~= ARGV[6] then
    return redis.error_reply('backfill acknowledgement conflict')
  end
  redis.call('DEL', KEYS[1])
  return {'acked', ARGV[4]}
end

if redis.call('HLEN', KEYS[1]) ~= tonumber(ARGV[4]) + 4 then
  return redis.error_reply('backfill accumulator size changed')
end
if redis.call('HGET', KEYS[1], '__manifest_hash') ~= ARGV[3]
   or redis.call('HGET', KEYS[1], '__expected_count') ~= ARGV[4]
   or redis.call('HGET', KEYS[1], '__event_ids_sha256') ~= ARGV[5]
   or redis.call('HGET', KEYS[1], '__records_sha256') ~= ARGV[6] then
  return redis.error_reply('backfill accumulator metadata changed')
end
for index = 9, #ARGV, 2 do
  if redis.call('HGET', KEYS[1], ARGV[index]) ~= ARGV[index + 1] then
    return redis.error_reply('backfill accumulator changed')
  end
end
redis.call(
  'HSET', KEYS[2],
  'job_id', ARGV[1],
  'chunk_id', ARGV[2],
  'manifest_hash', ARGV[3],
  'record_count', ARGV[4],
  'event_ids_sha256', ARGV[5],
  'records_sha256', ARGV[6],
  'commit_id', ARGV[7],
  'committed_at', ARGV[8],
  'status', 'committed'
)
redis.call('DEL', KEYS[1])
return {'acked', ARGV[4]}
"""


class BackfillProofMismatch(RuntimeError):
    """A replay chunk's durable records do not match Gateway's proof."""


@dataclass(frozen=True)
class BackfillEventProof:
    job_id: str
    chunk_id: str
    manifest_hash: str
    expected_count: int
    expected_event_ids_sha256: str
    expected_records_sha256: str
    event_id: str
    payload_sha256: str


def proof_digests(records: dict[str, str]) -> tuple[str, str]:
    """Hash sorted event IDs and their canonical payload hashes."""
    ordered = sorted(records.items())
    event_ids_sha256 = hashlib.sha256("\n".join(event_id for event_id, _ in ordered).encode()).hexdigest()
    records_sha256 = hashlib.sha256(
        "\n".join(f"{event_id}:{payload_hash}" for event_id, payload_hash in ordered).encode()
    ).hexdigest()
    return event_ids_sha256, records_sha256


def backfill_event_proof(envelope: EventEnvelope) -> BackfillEventProof | None:
    """Extract and validate Gateway's expected chunk proof from a backfill event."""
    if envelope.source != "backfill":
        return None

    lineage = envelope.lineage
    names = {
        "job_id": "backfill_job_id",
        "chunk_id": "backfill_chunk_id",
        "manifest_hash": "backfill_manifest_hash",
        "expected_count": "backfill_expected_record_count",
        "expected_event_ids_sha256": "backfill_expected_event_ids_sha256",
        "expected_records_sha256": "backfill_expected_records_sha256",
    }
    missing = [field for field in names.values() if lineage.get(field) in (None, "")]
    if missing:
        raise BackfillProofMismatch(f"missing backfill proof field: {missing[0]}")
    try:
        expected_count = int(lineage[names["expected_count"]])
    except (TypeError, ValueError) as exc:
        raise BackfillProofMismatch("invalid backfill expected record count") from exc
    if expected_count < 1:
        raise BackfillProofMismatch("invalid backfill expected record count")
    from heber.config import settings

    if expected_count > settings.backfill_proof_max_expected_records:
        raise BackfillProofMismatch(
            "backfill expected record count exceeds configured maximum "
            f"({expected_count} > {settings.backfill_proof_max_expected_records})"
        )

    payload_sha256 = hashlib.sha256(
        json.dumps(
            envelope.payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode()
    ).hexdigest()
    return BackfillEventProof(
        job_id=str(lineage[names["job_id"]]),
        chunk_id=str(lineage[names["chunk_id"]]),
        manifest_hash=str(lineage[names["manifest_hash"]]),
        expected_count=expected_count,
        expected_event_ids_sha256=str(lineage[names["expected_event_ids_sha256"]]),
        expected_records_sha256=str(lineage[names["expected_records_sha256"]]),
        event_id=envelope.event_id,
        payload_sha256=payload_sha256,
    )


def _decode_hash(raw: dict[Any, Any]) -> dict[str, str]:
    return {
        key.decode() if isinstance(key, bytes) else str(key): (
            value.decode() if isinstance(value, bytes) else str(value)
        )
        for key, value in raw.items()
    }


class BackfillAckWriter:
    """Accumulate committed event proofs and atomically publish a chunk ACK."""

    def __init__(self, client: Any, *, proof_ttl_seconds: int) -> None:
        self._redis = client
        self._proof_ttl_seconds = proof_ttl_seconds

    @staticmethod
    def _proof_key(proof: BackfillEventProof) -> str:
        return f"gateway:backfill:proof:{proof.job_id}:{proof.chunk_id}"

    @staticmethod
    def _ack_key(proof: BackfillEventProof) -> str:
        return f"gateway:backfill:ack:{proof.job_id}:{proof.chunk_id}"

    async def record_committed(
        self,
        proofs: list[BackfillEventProof],
        *,
        commit_id: str,
        committed_at: datetime,
    ) -> set[tuple[str, str]]:
        chunks: dict[tuple[str, str], list[BackfillEventProof]] = {}
        for proof in proofs:
            chunks.setdefault((proof.job_id, proof.chunk_id), []).append(proof)
        finalized: set[tuple[str, str]] = set()
        for chunk_key, chunk_proofs in chunks.items():
            if await self._record_chunk(
                chunk_proofs,
                commit_id=commit_id,
                committed_at=committed_at,
            ):
                finalized.add(chunk_key)
        return finalized

    async def _record_chunk(
        self,
        proofs: list[BackfillEventProof],
        *,
        commit_id: str,
        committed_at: datetime,
    ) -> bool:
        first = proofs[0]
        expected = (
            first.manifest_hash,
            first.expected_count,
            first.expected_event_ids_sha256,
            first.expected_records_sha256,
        )
        if any(
            (
                proof.manifest_hash,
                proof.expected_count,
                proof.expected_event_ids_sha256,
                proof.expected_records_sha256,
            )
            != expected
            for proof in proofs[1:]
        ):
            raise BackfillProofMismatch("backfill chunk metadata changed within committed batch")

        pairs = sorted({proof.event_id: proof.payload_sha256 for proof in proofs}.items())
        base_args = [
            first.job_id,
            first.chunk_id,
            first.manifest_hash,
            str(first.expected_count),
            first.expected_event_ids_sha256,
            first.expected_records_sha256,
        ]
        result = await self._redis.eval(
            _ACCUMULATE_SCRIPT,
            2,
            self._proof_key(first),
            self._ack_key(first),
            *base_args,
            str(self._proof_ttl_seconds),
            *(value for pair in pairs for value in pair),
        )
        state = result[0].decode() if isinstance(result[0], bytes) else str(result[0])
        if state == "acked":
            return True

        accumulated = _decode_hash(await self._redis.hgetall(self._proof_key(first)))
        records = {key: value for key, value in accumulated.items() if key not in _META_FIELDS}
        if len(records) < first.expected_count:
            return False
        if len(records) != first.expected_count:
            raise BackfillProofMismatch("backfill committed event count exceeded expected count")
        event_ids_sha256, records_sha256 = proof_digests(records)
        if event_ids_sha256 != first.expected_event_ids_sha256 or records_sha256 != first.expected_records_sha256:
            raise BackfillProofMismatch("backfill committed chunk proof mismatch")

        ordered = sorted(records.items())
        result = await self._redis.eval(
            _FINALIZE_SCRIPT,
            2,
            self._proof_key(first),
            self._ack_key(first),
            *base_args,
            commit_id,
            committed_at.isoformat(),
            *(value for pair in ordered for value in pair),
        )
        state = result[0].decode() if isinstance(result[0], bytes) else str(result[0])
        return state == "acked"
