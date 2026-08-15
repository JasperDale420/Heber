from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from heber.watch import consumer as consumer_module
from heber.watch.consumer import AlertWatchConsumer


class _NoopRedis:
    pass


class _CaptureManager:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def has_alert_claim_async(self, alert_id: str) -> bool:
        return False

    async def create_watch_async(self, **kwargs):  # noqa: ANN003
        self.calls.append(kwargs)
        return SimpleNamespace(watch_id=f"watch-{len(self.calls)}")


def _consumer() -> AlertWatchConsumer:
    return AlertWatchConsumer(redis_client=_NoopRedis(), watch_manager=_CaptureManager())


def _stream_data(payload: dict) -> dict[bytes, bytes]:
    return {b"data": json.dumps(payload).encode("utf-8")}


def test_parse_alert_single_payload_success() -> None:
    consumer = _consumer()

    parsed = consumer._parse_alert(
        _stream_data(
            {
                "feed": "flow_alerts",
                "payload": {
                    "id": "a1",
                    "symbol": "AAPL",
                    "option_chain": "AAPL260220C00100000",
                    "put_call": "C",
                    "expiry": "2026-02-20",
                    "strike": 100.0,
                    "created_at": "2026-02-07T15:00:00Z",
                },
            }
        )
    )

    assert parsed is not None
    assert parsed["id"] == "a1"
    assert parsed["underlying"] == "AAPL"
    assert parsed["occ_symbol"] == "AAPL260220C00100000"


def test_parse_alerts_batch_payload_parses_all_valid_items() -> None:
    consumer = _consumer()

    parsed_items = consumer._parse_alerts(
        _stream_data(
            {
                "feed": "flow_alerts",
                "payload": {
                    "items": [
                        {
                            "id": "a1",
                            "symbol": "AAPL",
                            "option_chain": "AAPL260220C00100000",
                            "expiry": "2026-02-20",
                            "put_call": "C",
                            "strike": 100.0,
                        },
                        {
                            "id": "a2",
                            "symbol": "MSFT",
                            "option_chain": "MSFT260220C00300000",
                            "expiry": "2026-02-20",
                            "put_call": "P",
                            "strike": 300.0,
                        },
                    ]
                },
            }
        )
    )

    assert len(parsed_items) == 2
    assert parsed_items[0]["id"] == "a1"
    assert parsed_items[1]["id"] == "a2"


def test_parse_alerts_batch_keeps_valid_items_and_logs_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer = _consumer()
    info_calls: list[tuple[str, dict]] = []
    parse_metric_calls: list[dict] = []

    def _capture_info(message: str, **kwargs) -> None:  # noqa: ANN003
        info_calls.append((message, kwargs))

    monkeypatch.setattr(consumer_module.logger, "info", _capture_info)
    monkeypatch.setattr(
        consumer_module,
        "record_watch_alert_parse",
        lambda **kwargs: parse_metric_calls.append(kwargs),
    )

    parsed_items = consumer._parse_alerts(
        _stream_data(
            {
                "feed": "flow_alerts",
                "payload": {
                    "items": [
                        {
                            "id": "a1",
                            "symbol": "AAPL",
                            "option_chain": "AAPL260220C00100000",
                            "expiry": "2026-02-20",
                            "put_call": "C",
                            "strike": 100.0,
                        },
                        {"option_chain": "BROKEN"},
                    ]
                },
            }
        )
    )

    assert len(parsed_items) == 1
    assert parsed_items[0]["id"] == "a1"
    parse_summary = next(call for call in info_calls if call[0] == "Alert parsing summary")
    assert parse_summary[1]["batch_items_total"] == 2
    assert parse_summary[1]["batch_items_failed"] == 1
    assert parse_summary[1]["alert_parse_success"] == 1
    assert parse_summary[1]["alert_parse_failed"] == 1
    assert {"status": "success", "count": 1} in parse_metric_calls
    assert {"status": "failed", "count": 1} in parse_metric_calls


def test_parse_alerts_malformed_payload_is_rejected_with_failure_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer = _consumer()
    info_calls: list[tuple[str, dict]] = []
    parse_metric_calls: list[dict] = []

    def _capture_info(message: str, **kwargs) -> None:  # noqa: ANN003
        info_calls.append((message, kwargs))

    monkeypatch.setattr(consumer_module.logger, "info", _capture_info)
    monkeypatch.setattr(
        consumer_module,
        "record_watch_alert_parse",
        lambda **kwargs: parse_metric_calls.append(kwargs),
    )

    parsed_items = consumer._parse_alerts(
        _stream_data({"feed": "flow_alerts", "payload": {"items": "not-a-list"}}),
    )

    assert parsed_items == []
    parse_summary = next(call for call in info_calls if call[0] == "Alert parsing summary")
    assert parse_summary[1]["alert_parse_success"] == 0
    assert parse_summary[1]["alert_parse_failed"] == 1
    assert parse_metric_calls == [{"status": "failed", "count": 1}]


@pytest.mark.asyncio
async def test_process_alert_batch_continues_after_invalid_items() -> None:
    manager = _CaptureManager()
    consumer = AlertWatchConsumer(redis_client=_NoopRedis(), watch_manager=manager)
    consumer._get_entry_price = AsyncMock(return_value=1.23)  # type: ignore[method-assign]
    consumer._extract_and_store_features = AsyncMock(return_value=None)  # type: ignore[method-assign]

    result = await consumer._process_alert(
        "1-0",
        _stream_data(
            {
                "feed": "flow_alerts",
                "payload": {
                    "items": [
                        {
                            "id": "a1",
                            "symbol": "AAPL",
                            "option_chain": "AAPL260220C00100000",
                            "put_call": "C",
                            "expiry": "2026-02-20",
                            "strike": 100.0,
                            "created_at": "2026-02-07T15:00:00Z",
                        },
                        {"id": "bad-item", "option_chain": "BROKEN"},
                        {
                            "id": "a2",
                            "symbol": "MSFT",
                            "option_chain": "MSFT260220C00300000",
                            "put_call": "P",
                            "expiry": "2026-02-20",
                            "strike": 300.0,
                            "created_at": "2026-02-07T15:01:00Z",
                        },
                    ]
                },
            }
        ),
    )

    assert result == (True, False, "watch_created")
    assert len(manager.calls) == 2
    assert {call["alert_id"] for call in manager.calls} == {"a1", "a2"}
