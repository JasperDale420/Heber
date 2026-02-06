"""Regression tests for Bloom filter dedupe rotation behavior."""

from __future__ import annotations

from heber.ops.reliability import EventDeduplicator


def test_duplicate_detected_within_same_rotation_window() -> None:
    now = [0.0]
    deduper = EventDeduplicator(
        bloom_size=100_000,
        bloom_rotation_seconds=60.0,
        now_fn=lambda: now[0],
    )

    first = deduper.check_and_register("event-1")
    second = deduper.check_and_register("event-1")

    assert first.is_duplicate is False
    assert second.is_duplicate is True
    assert second.reason == "bloom_filter_match"


def test_event_ages_out_after_two_rotation_windows_without_backing_store() -> None:
    now = [0.0]
    deduper = EventDeduplicator(
        bloom_size=100_000,
        bloom_rotation_seconds=10.0,
        now_fn=lambda: now[0],
    )

    assert deduper.check_and_register("event-2").is_duplicate is False

    now[0] = 11.0
    assert deduper.check_and_register("event-2").is_duplicate is True

    now[0] = 22.0
    aged_out = deduper.check_and_register("event-2")
    stats = deduper.stats

    assert aged_out.is_duplicate is False
    assert stats["rotations"] == 2
    assert stats["bloom_rotation_seconds"] == 10.0
    assert stats["previous_bloom_count"] == 0
