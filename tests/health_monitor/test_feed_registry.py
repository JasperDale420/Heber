"""Tests for the must-flow feed registry."""

from __future__ import annotations

import pytest

from heber.health_monitor.feed_registry import DEFAULT_REGISTRY, resolved_registry


@pytest.mark.unit
def test_default_registry_has_expected_feeds() -> None:
    feeds = {r.feed for r in DEFAULT_REGISTRY}
    assert feeds == {"flow_alerts", "darkpool", "bars", "trades", "oi_change", "greek_exposure"}


@pytest.mark.unit
def test_default_registry_kinds() -> None:
    by_feed = {r.feed: r for r in DEFAULT_REGISTRY}
    assert by_feed["flow_alerts"].kind == "continuous"
    assert by_feed["darkpool"].kind == "continuous"
    assert by_feed["oi_change"].kind == "daily"
    assert by_feed["greek_exposure"].kind == "daily"


@pytest.mark.unit
def test_floor_override_changes_floor() -> None:
    rules = resolved_registry({"darkpool": 8})
    darkpool = next(r for r in rules if r.feed == "darkpool")
    assert darkpool.floor == 8


@pytest.mark.unit
def test_floor_zero_disables_feed() -> None:
    rules = resolved_registry({"bars": 0})
    assert all(r.feed != "bars" for r in rules)


@pytest.mark.unit
def test_no_overrides_returns_defaults() -> None:
    assert resolved_registry({}) == list(DEFAULT_REGISTRY)


@pytest.mark.unit
def test_darkpool_lookback_spans_batched_delivery() -> None:
    # Darkpool delivers in batches with long inter-batch gaps (esp. after-hours);
    # the lookback must be wide enough to span those gaps so a healthy-but-quiet
    # feed is not read as "dark". A 60m window flapped CRITICAL/RECOVERED all evening.
    darkpool = next(r for r in DEFAULT_REGISTRY if r.feed == "darkpool")
    assert darkpool.lookback_minutes >= 180
