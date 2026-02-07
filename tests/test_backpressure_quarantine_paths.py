from __future__ import annotations

from heber.bus.backpressure import DLQHandler


def test_quarantine_uses_top_level_provider_feed(tmp_path) -> None:
    handler = DLQHandler(bus=object(), quarantine_base_path=str(tmp_path))

    envelope = {
        "event_id": "evt-1",
        "provider": "alpaca",
        "feed": "bars",
        "meta": {"provider": "legacy-provider", "feed": "legacy-feed"},
    }

    path = handler.write_to_quarantine(envelope, RuntimeError("failure"))
    parts = set(path.parts)

    assert "provider=alpaca" in parts
    assert "feed=bars" in parts


def test_quarantine_falls_back_to_legacy_meta_provider_feed(tmp_path) -> None:
    handler = DLQHandler(bus=object(), quarantine_base_path=str(tmp_path))

    envelope = {
        "event_id": "evt-2",
        "meta": {"provider": "unusual_whales", "feed": "flow_alerts"},
    }

    path = handler.write_to_quarantine(envelope, RuntimeError("failure"))
    parts = set(path.parts)

    assert "provider=unusual_whales" in parts
    assert "feed=flow_alerts" in parts
