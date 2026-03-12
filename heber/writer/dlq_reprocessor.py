"""DLQ Reprocessor -- retries failed events from the dead letter queue.

Reads events from the ``heber:events:dlq`` Redis stream, parses the original
EventEnvelope payload stored in each DLQ entry, and re-attempts Bronze + Silver
normalization through the same pipeline used by ``EventConsumer``.

Usage:
    # Analyze DLQ contents (dry run)
    python -m heber.writer.dlq_reprocessor --analyze

    # Reprocess all DLQ events
    python -m heber.writer.dlq_reprocessor --reprocess

    # Reprocess only specific feeds
    python -m heber.writer.dlq_reprocessor --reprocess --feed insider_trades

    # Reprocess with custom batch size
    python -m heber.writer.dlq_reprocessor --reprocess --batch-size 500

    # Purge old DLQ entries (dry run)
    python -m heber.writer.dlq_reprocessor --purge --older-than 30d

    # Purge old DLQ entries (execute)
    python -m heber.writer.dlq_reprocessor --purge --older-than 30d --confirm
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

import redis.asyncio as aioredis
import structlog

from heber.config import settings
from heber.ops.logging import configure_logging
from heber.writer.consumer import EventConsumer

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DURATION_RE = re.compile(r"^(\d+)\s*([dhms])$", re.IGNORECASE)

_DURATION_MULTIPLIERS: dict[str, int] = {
    "d": 86400,
    "h": 3600,
    "m": 60,
    "s": 1,
}


def _parse_duration(raw: str) -> timedelta:
    """Parse a human duration like ``30d``, ``12h``, ``90m`` into a timedelta."""
    match = _DURATION_RE.match(raw.strip())
    if not match:
        raise ValueError(f"Invalid duration format: {raw!r}. Expected <number><d|h|m|s>, e.g. 30d")
    value = int(match.group(1))
    unit = match.group(2).lower()
    return timedelta(seconds=value * _DURATION_MULTIPLIERS[unit])


def _decode(value: Any) -> str:
    """Decode bytes to str, pass through strings."""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _redis_id_to_datetime(redis_id: str) -> datetime:
    """Convert a Redis stream ID (``<ms_timestamp>-<seq>``) to a UTC datetime."""
    ms_str = redis_id.split("-")[0]
    return datetime.fromtimestamp(int(ms_str) / 1000.0, tz=UTC)


def _extract_dlq_fields(entry_data: dict) -> dict[str, str]:
    """Normalize a raw DLQ entry dict (bytes keys) to string keys/values."""
    return {_decode(k): _decode(v) for k, v in entry_data.items()}


def _extract_feed_from_dlq_payload(payload_json: str) -> str:
    """Best-effort feed extraction from the serialized DLQ payload."""
    try:
        payload = json.loads(payload_json)
        data_str = payload.get("data", "{}")
        if isinstance(data_str, str):
            data = json.loads(data_str)
        else:
            data = data_str
        return str(data.get("feed", "unknown"))
    except (json.JSONDecodeError, TypeError, KeyError):
        return "unknown"


def _reconstruct_event_data(dlq_fields: dict[str, str]) -> dict:
    """Reconstruct the original ``event_data`` dict that ``EventConsumer`` expects.

    The DLQ entry stores the original message payload under the ``payload`` key
    as a JSON string of ``{field: value}`` pairs.  We need to give back the
    same structure that ``_parse_and_validate_envelope`` received.
    """
    raw_payload = dlq_fields.get("payload", "{}")
    try:
        original_fields = json.loads(raw_payload)
    except json.JSONDecodeError:
        original_fields = {"data": raw_payload}
    return original_fields


# ---------------------------------------------------------------------------
# Analyze
# ---------------------------------------------------------------------------


async def analyze_dlq(
    redis_conn: aioredis.Redis,
    *,
    stream_name: str,
    feed_filter: str | None = None,
) -> dict[str, Any]:
    """Read every DLQ entry and produce a summary report."""
    logger.info("dlq_analyze_start", stream=stream_name, feed_filter=feed_filter)

    feed_counts: Counter[str] = Counter()
    error_counts: Counter[str] = Counter()
    error_messages: Counter[str] = Counter()
    sample_payloads: dict[str, list[str]] = defaultdict(list)
    total = 0
    earliest_ts: datetime | None = None
    latest_ts: datetime | None = None

    cursor = "0-0"
    batch_size = 5000

    while True:
        entries = await redis_conn.xrange(stream_name, min=cursor, count=batch_size)
        if not entries:
            break

        for entry_id, entry_data in entries:
            entry_id_str = _decode(entry_id)
            fields = _extract_dlq_fields(entry_data)

            error_raw = fields.get("error", "unknown_error")
            error_type = error_raw.split(":", 1)[0] if error_raw else "unknown_error"
            payload_json = fields.get("payload", "{}")
            feed = _extract_feed_from_dlq_payload(payload_json)

            if feed_filter and feed != feed_filter:
                # Advance cursor even when filtering
                cursor = _advance_cursor(entry_id_str)
                continue

            total += 1
            feed_counts[feed] += 1
            error_counts[error_type] += 1
            error_messages[error_raw] += 1

            entry_ts = _redis_id_to_datetime(entry_id_str)
            if earliest_ts is None or entry_ts < earliest_ts:
                earliest_ts = entry_ts
            if latest_ts is None or entry_ts > latest_ts:
                latest_ts = entry_ts

            # Keep up to 2 sample payloads per error type
            if len(sample_payloads[error_type]) < 2:
                sample_payloads[error_type].append(payload_json[:500])

        # Advance past last entry
        last_id = _decode(entries[-1][0])
        cursor = _advance_cursor(last_id)

    top_errors = error_messages.most_common(5)

    report = {
        "total_dlq_entries": total,
        "date_range": {
            "earliest": earliest_ts.isoformat() if earliest_ts else None,
            "latest": latest_ts.isoformat() if latest_ts else None,
        },
        "by_feed": dict(feed_counts.most_common()),
        "by_error_type": dict(error_counts.most_common()),
        "top_error_messages": [{"message": msg, "count": cnt} for msg, cnt in top_errors],
        "sample_payloads": {k: v for k, v in sample_payloads.items()},
    }
    logger.info("dlq_analyze_complete", total=total, feed_count=len(feed_counts), error_type_count=len(error_counts))
    return report


def _advance_cursor(last_id: str) -> str:
    """Advance a Redis stream cursor past ``last_id``."""
    parts = last_id.split("-")
    ms = parts[0]
    seq = int(parts[1]) if len(parts) > 1 else 0
    return f"{ms}-{seq + 1}"


# ---------------------------------------------------------------------------
# Reprocess
# ---------------------------------------------------------------------------


async def reprocess_dlq(
    redis_conn: aioredis.Redis,
    *,
    stream_name: str,
    feed_filter: str | None = None,
    batch_size: int = 1000,
    max_retries: int = 1,
) -> dict[str, int]:
    """Re-attempt processing of DLQ events through the standard pipeline.

    Successfully reprocessed entries are deleted from the DLQ stream.
    """
    logger.info(
        "dlq_reprocess_start",
        stream=stream_name,
        feed_filter=feed_filter,
        batch_size=batch_size,
        max_retries=max_retries,
    )

    consumer = EventConsumer()
    # We do not need a full Redis connection for the consumer itself since
    # we only use its synchronous processing path (no XREADGROUP).

    success_count = 0
    failure_count = 0
    skipped_count = 0
    delete_ids: list[str] = []

    cursor = "0-0"
    t0 = time.monotonic()

    while True:
        entries = await redis_conn.xrange(stream_name, min=cursor, count=batch_size)
        if not entries:
            break

        for entry_id, entry_data in entries:
            entry_id_str = _decode(entry_id)
            fields = _extract_dlq_fields(entry_data)

            payload_json = fields.get("payload", "{}")
            feed = _extract_feed_from_dlq_payload(payload_json)

            if feed_filter and feed != feed_filter:
                skipped_count += 1
                cursor = _advance_cursor(entry_id_str)
                continue

            event_data = _reconstruct_event_data(fields)
            processed = False

            for attempt in range(1, max_retries + 1):
                try:
                    ok = consumer.process_event(event_data)
                    if ok:
                        processed = True
                        break
                except Exception as exc:  # noqa: BLE001
                    logger.debug(
                        "dlq_reprocess_attempt_failed",
                        entry_id=entry_id_str,
                        attempt=attempt,
                        error=str(exc),
                    )

            if processed:
                success_count += 1
                delete_ids.append(entry_id_str)
            else:
                failure_count += 1
                logger.debug(
                    "dlq_reprocess_failed",
                    entry_id=entry_id_str,
                    feed=feed,
                    original_error=fields.get("error", ""),
                )

            # Flush Silver/Bronze writers periodically
            if (success_count + failure_count) % batch_size == 0 and success_count > 0:
                _flush_writers(consumer)

        # Advance past last entry
        last_id = _decode(entries[-1][0])
        cursor = _advance_cursor(last_id)

        # Batch-delete successfully reprocessed entries
        if delete_ids:
            await redis_conn.xdel(stream_name, *delete_ids)
            logger.info("dlq_entries_deleted", count=len(delete_ids))
            delete_ids.clear()

    # Final flush
    _flush_writers(consumer)

    # Delete any remaining successful entries
    if delete_ids:
        await redis_conn.xdel(stream_name, *delete_ids)

    elapsed = time.monotonic() - t0
    stats = {
        "success": success_count,
        "failure": failure_count,
        "skipped": skipped_count,
        "elapsed_seconds": round(elapsed, 2),
    }
    logger.info("dlq_reprocess_complete", **stats)
    return stats


def _flush_writers(consumer: EventConsumer) -> None:
    """Flush Bronze and Silver writers on the consumer."""
    try:
        consumer.bronze_writer.flush()
    except Exception as exc:  # noqa: BLE001
        logger.error("dlq_bronze_flush_failed", error=str(exc), exc_info=True)
    try:
        consumer.silver_writer.flush()
    except Exception as exc:  # noqa: BLE001
        logger.error("dlq_silver_flush_failed", error=str(exc), exc_info=True)


# ---------------------------------------------------------------------------
# Purge
# ---------------------------------------------------------------------------


async def purge_dlq(
    redis_conn: aioredis.Redis,
    *,
    stream_name: str,
    older_than: timedelta,
    confirm: bool = False,
) -> dict[str, int]:
    """Remove DLQ entries older than the specified threshold.

    Without ``confirm=True``, performs a dry-run count only.
    """
    cutoff = datetime.now(UTC) - older_than
    cutoff_ms = int(cutoff.timestamp() * 1000)
    cutoff_id = f"{cutoff_ms}-0"

    logger.info(
        "dlq_purge_start",
        stream=stream_name,
        cutoff=cutoff.isoformat(),
        cutoff_id=cutoff_id,
        dry_run=not confirm,
    )

    # Count entries before cutoff
    entries = await redis_conn.xrange(stream_name, min="-", max=cutoff_id)
    eligible_count = len(entries)

    if not confirm:
        logger.info("dlq_purge_dry_run", eligible_count=eligible_count, cutoff=cutoff.isoformat())
        return {"eligible": eligible_count, "deleted": 0, "dry_run": True}

    # Delete in batches
    deleted = 0
    batch: list[str] = []
    for entry_id, _data in entries:
        batch.append(_decode(entry_id))
        if len(batch) >= 5000:
            await redis_conn.xdel(stream_name, *batch)
            deleted += len(batch)
            batch.clear()

    if batch:
        await redis_conn.xdel(stream_name, *batch)
        deleted += len(batch)

    logger.info("dlq_purge_complete", eligible=eligible_count, deleted=deleted)
    return {"eligible": eligible_count, "deleted": deleted, "dry_run": False}


# ---------------------------------------------------------------------------
# DLQ Size Check (importable by dataflow_health)
# ---------------------------------------------------------------------------


def check_dlq_size(
    redis_url: str,
    dlq_stream_name: str,
    threshold: int = 10_000,
) -> dict[str, Any]:
    """Check DLQ stream length and return a health-check-style dict.

    Uses synchronous Redis to match the ``dataflow_health`` module pattern.
    """
    import redis as sync_redis

    client: sync_redis.Redis | None = None
    try:
        client = sync_redis.Redis.from_url(
            redis_url,
            socket_connect_timeout=2,
            socket_timeout=2,
            decode_responses=False,
        )
        length = client.xlen(dlq_stream_name)
        exceeded = length > threshold

        if exceeded:
            logger.warning(
                "dlq_size_exceeded",
                dlq_stream=dlq_stream_name,
                length=length,
                threshold=threshold,
            )

        status = "warn" if exceeded else "ok"
        return {
            "id": "dlq_queue_size",
            "status": status,
            "severity": "warning",
            "observed": {
                "dlq_stream": dlq_stream_name,
                "length": length,
            },
            "threshold": {"max_length": threshold},
            "message": (
                f"DLQ has {length} entries."
                if not exceeded
                else f"DLQ has {length} entries (exceeds {threshold} threshold)."
            ),
        }
    except Exception as exc:
        return {
            "id": "dlq_queue_size",
            "status": "warn",
            "severity": "warning",
            "observed": {"dlq_stream": dlq_stream_name, "length": None},
            "threshold": {"max_length": threshold},
            "message": f"Could not check DLQ size: {exc}",
        }
    finally:
        if client is not None:
            client.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Heber DLQ Reprocessor - retry or manage failed events",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--analyze", action="store_true", help="Analyze DLQ contents and print summary report")
    mode.add_argument("--reprocess", action="store_true", help="Reprocess DLQ events through the ingest pipeline")
    mode.add_argument("--purge", action="store_true", help="Remove old DLQ entries")

    parser.add_argument("--feed", type=str, default=None, help="Filter to a specific feed name")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for reprocessing (default: 1000)")
    parser.add_argument("--max-retries", type=int, default=1, help="Max retry attempts per event (default: 1)")
    parser.add_argument("--older-than", type=str, default=None, help="Purge threshold, e.g. 30d, 12h, 90m")
    parser.add_argument("--confirm", action="store_true", help="Required for destructive purge operations")
    parser.add_argument("--dlq-stream", type=str, default=None, help="Override DLQ stream name")
    parser.add_argument("--redis-url", type=str, default=None, help="Override Redis URL")

    return parser


async def _async_main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    redis_url = args.redis_url or settings.redis_url
    dlq_stream = args.dlq_stream or settings.redis_dlq_stream_name

    conn = aioredis.from_url(redis_url)
    try:
        await conn.ping()
    except Exception as exc:
        logger.error("dlq_redis_connection_failed", redis_url=redis_url, error=str(exc))
        await conn.aclose()
        return 1

    try:
        if args.analyze:
            report = await analyze_dlq(conn, stream_name=dlq_stream, feed_filter=args.feed)
            print(json.dumps(report, indent=2, default=str))
            return 0

        if args.reprocess:
            stats = await reprocess_dlq(
                conn,
                stream_name=dlq_stream,
                feed_filter=args.feed,
                batch_size=args.batch_size,
                max_retries=args.max_retries,
            )
            print(json.dumps(stats, indent=2))
            return 0

        if args.purge:
            if not args.older_than:
                logger.error("dlq_purge_missing_older_than")
                print("Error: --purge requires --older-than (e.g. --older-than 30d)", file=sys.stderr)
                return 1
            duration = _parse_duration(args.older_than)
            stats = await purge_dlq(
                conn,
                stream_name=dlq_stream,
                older_than=duration,
                confirm=args.confirm,
            )
            print(json.dumps(stats, indent=2))
            if not args.confirm and stats.get("eligible", 0) > 0:
                print(
                    f"\nDry run: {stats['eligible']} entries eligible for deletion. Add --confirm to execute.",
                    file=sys.stderr,
                )
            return 0

    except KeyboardInterrupt:
        logger.info("dlq_reprocessor_interrupted")
        return 0
    except Exception as exc:
        logger.error("dlq_reprocessor_fatal", error=str(exc), exc_info=True)
        return 1
    finally:
        await conn.aclose()

    return 0


def main() -> int:
    configure_logging(service_name="heber-dlq-reprocessor", log_level=settings.log_level, json_output=True)
    return asyncio.run(_async_main())


if __name__ == "__main__":
    raise SystemExit(main())
