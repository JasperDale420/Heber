"""Capture the pending-entry-list (PEL) state for every Heber consumer group.

A PEL entry is a message that was delivered to a consumer but never
acknowledged. When the stream trims past that entry's id the payload is gone,
so the entry can never be re-read — the delivery is permanently unrecoverable.

This script records what is defensible from Redis alone:

- how many entries each group holds, and how many are already past retention
- which consumer owns each entry and how long it has been idle
- the wall-clock window the unrecoverable entries span

What it deliberately does NOT do is estimate how many rows were lost. A stream
id carries the XADD time and nothing else; once the entry is trimmed the feed,
symbol and payload are gone. Comparing Bronze row counts against neighbouring
hours cannot separate a real loss from a quiet market, an upstream outage, or a
feed that was never sent. Answering that needs a provider-side re-pull over the
reported windows and a diff against Bronze.

Read-only. Nothing here claims, acknowledges, or purges — the PEL is the only
surviving record of these deliveries.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime

import redis

REDIS_URL = "redis://localhost:6379"

# (stream, group) for every consumer group Heber runs against these streams.
GROUPS: tuple[tuple[str, str], ...] = (
    ("heber:events", "heber-writers"),
    ("heber:events", "watch-consumer"),
    ("heber:events:backfill", "heber-backfill-writers"),
)

# XPENDING has no cursor, so this must exceed the real backlog or the tail is
# invisible. Observed peak was ~37k; 500k is the stream's own MAXLEN.
PEL_FETCH_LIMIT = 500_000


def _decode(value: object) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _id_millis(entry_id: str) -> int:
    """Redis stream ids are '<unix_millis>-<sequence>'."""
    return int(entry_id.split("-")[0])


def _id_time(entry_id: str) -> datetime:
    return datetime.fromtimestamp(_id_millis(entry_id) / 1000, tz=UTC)


def audit_group(client: redis.Redis, stream: str, group: str) -> dict:
    info = {_decode(k): v for k, v in client.xinfo_stream(stream).items()}
    first_retained = _decode(info["recorded-first-entry-id"])
    retention_floor = _id_millis(first_retained)

    pending = client.xpending_range(stream, group, "-", "+", PEL_FETCH_LIMIT)

    trimmed: list[str] = []
    retained: list[str] = []
    by_consumer: dict[str, int] = {}

    for entry in pending:
        entry_id = _decode(entry["message_id"])
        owner = _decode(entry["consumer"])
        by_consumer[owner] = by_consumer.get(owner, 0) + 1
        # Strictly-older ids no longer exist in the stream: XCLAIM cannot return
        # them and the payload is gone.
        (trimmed if _id_millis(entry_id) < retention_floor else retained).append(entry_id)

    result = {
        "stream": stream,
        "group": group,
        "stream_length": info["length"],
        "first_retained_id": first_retained,
        "first_retained_at": _id_time(first_retained).isoformat(),
        "retention_window_minutes": round(
            (_id_millis(_decode(info["last-generated-id"])) - retention_floor) / 60_000, 1
        ),
        "pending_total": len(pending),
        "unrecoverable": len(trimmed),
        "still_recoverable": len(retained),
        "by_consumer": dict(sorted(by_consumer.items(), key=lambda kv: -kv[1])),
    }

    if trimmed:
        result["unrecoverable_window"] = {
            "earliest": _id_time(min(trimmed, key=_id_millis)).isoformat(),
            "latest": _id_time(max(trimmed, key=_id_millis)).isoformat(),
        }
    return result


def main() -> int:
    client = redis.from_url(REDIS_URL)
    report = {
        "captured_at": datetime.now(UTC).isoformat(),
        "groups": [],
    }

    for stream, group in GROUPS:
        try:
            report["groups"].append(audit_group(client, stream, group))
        except redis.ResponseError as exc:
            report["groups"].append({"stream": stream, "group": group, "error": str(exc)})

    print(json.dumps(report, indent=2, default=str))

    totals = [g for g in report["groups"] if "error" not in g]
    print("\n--- summary ---", file=sys.stderr)
    for g in totals:
        print(
            f"{g['stream']} / {g['group']}: {g['pending_total']} pending, "
            f"{g['unrecoverable']} unrecoverable, {g['still_recoverable']} recoverable "
            f"(retention {g['retention_window_minutes']} min)",
            file=sys.stderr,
        )
    print(
        f"TOTAL unrecoverable deliveries: {sum(g['unrecoverable'] for g in totals)}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
