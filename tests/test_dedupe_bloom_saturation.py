"""Regression tests for bloom-filter saturation in consumer dedupe.

Incident 2026-06-10: the heber:events stream carries ~105M events/day, but the
dedupe bloom filter was sized at 10M bits (~1M ids per rotation window at 1%
FPR). During market hours the filter saturated, its false-positive rate climbed
toward 100%, and `EventDeduplicator.check()` treated every bloom match (with no
backing store) as a confirmed duplicate — silently dropping ~98% of fresh
events across all feeds (198/200 insider_trades, 2647/2710 ftds, 192/200
darkpool in one minute) while ACKing them as successes.

A saturated bloom match is statistically meaningless, so the deduplicator must
fail open (pass the event through) instead of dropping fresh data.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from heber.ops.reliability import BloomFilter, EventDeduplicator
from heber.writer.consumer import EventConsumer

pytestmark = pytest.mark.unit


def _saturate(deduper: EventDeduplicator, n: int) -> None:
    for i in range(n):
        deduper.register(f"fill-{i}")


def test_estimated_fp_rate_grows_with_load() -> None:
    bloom = BloomFilter(size=1024, num_hashes=7)
    assert bloom.estimated_fp_rate == 0.0

    for i in range(1000):
        bloom.add(f"event-{i}")

    assert bloom.estimated_fp_rate > 0.9


def test_saturated_bloom_fails_open_for_fresh_events() -> None:
    deduper = EventDeduplicator(bloom_size=1024, bloom_rotation_seconds=0)
    _saturate(deduper, 1000)

    # With ~100% FPR every fresh id would previously be dropped as a
    # "duplicate". Fail-open must pass all of them through.
    results = [deduper.check(f"fresh-{i}") for i in range(50)]

    assert all(not r.is_duplicate for r in results)
    assert deduper.stats["saturated_passes"] > 0


def test_healthy_bloom_still_detects_duplicates() -> None:
    deduper = EventDeduplicator(bloom_rotation_seconds=0)

    first = deduper.check_and_register("event-1")
    second = deduper.check_and_register("event-1")

    assert first.is_duplicate is False
    assert second.is_duplicate is True
    assert second.reason == "bloom_filter_match"
    assert deduper.stats["saturated_passes"] == 0


def test_saturated_previous_bloom_also_fails_open() -> None:
    now = [0.0]
    deduper = EventDeduplicator(
        bloom_size=1024,
        bloom_rotation_seconds=10.0,
        now_fn=lambda: now[0],
    )
    _saturate(deduper, 1000)

    # Rotate: the saturated filter becomes the previous filter.
    now[0] = 11.0
    results = [deduper.check(f"fresh-{i}") for i in range(50)]

    assert all(not r.is_duplicate for r in results)


def _insider_event(event_id: str, suffix: int) -> dict:
    envelope = {
        "event_id": event_id,
        "provider": "unusual_whales",
        "feed": "insider_trades",
        "source": "rest",
        "instrument_type": "equity",
        "instrument_key": "equity:QCRH",
        "symbol": "QCRH",
        "ts_event": f"2026-06-10T20:33:32.{318000 + suffix:06d}Z",
        "ts_ingest": f"2026-06-10T20:33:32.{318000 + suffix:06d}Z",
        "payload": {
            "ticker": "QCRH",
            "owner_name": "EKIZIAN LAURA",
            "officer_title": "President & CEO",
            "transaction_code": "S",
            "amount": -750,
            "price": "94.9600",
            "transaction_date": "2026-06-09",
            "filing_date": "2026-06-10",
        },
    }
    return {"data": json.dumps(envelope)}


def test_consumer_writes_all_fresh_events_through_saturated_bloom() -> None:
    """Replay of the 2026-06-10 incident: 200 unique insider envelopes must all
    reach Bronze even when the dedupe bloom filter is fully saturated."""
    consumer = EventConsumer()
    consumer.event_deduplicator = EventDeduplicator(bloom_size=1024, bloom_rotation_seconds=0)
    _saturate(consumer.event_deduplicator, 1000)

    consumer.bronze_writer.write = MagicMock()
    consumer.silver_writer.write_row = MagicMock()

    for i in range(200):
        success, error, _ = consumer._process_event_once(_insider_event(f"insider-{i}", i))
        assert success is True, error

    assert consumer.bronze_writer.write.call_count == 200
