from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from heber.watch import poller as poller_module
from heber.watch.poller import SnapshotPoller


class _Response:
    def __init__(self, status_code: int, payload: dict[str, Any]):
        self.status_code = status_code
        self._payload = payload

    def json(self) -> dict[str, Any]:
        return self._payload


class _StubClient:
    def __init__(self, responses: dict[str, _Response]):
        self._responses = responses

    async def get(
        self,
        url: str,
        params: dict[str, Any] | None = None,  # noqa: ARG002
        headers: dict[str, str] | None = None,  # noqa: ARG002
    ) -> _Response:
        return self._responses[url]


class _Manager:
    def __init__(self) -> None:
        self.snapshots: list[Any] = []
        self.updated: list[tuple[str, float]] = []

    async def get_active_watches_async(self) -> list[Any]:
        return [
            SimpleNamespace(
                watch_id="watch-1",
                occ_symbol="AAPL260220C00100000",
                entry_price=1.0,
                updated_at=None,
                alert_time=datetime(2026, 2, 7, 15, 0, tzinfo=UTC),
            )
        ]

    async def add_snapshot_async(self, snapshot: Any) -> None:
        self.snapshots.append(snapshot)

    async def update_watch_price_async(self, watch_id: str, price: float, ts: datetime) -> None:  # noqa: ARG002
        self.updated.append((watch_id, price))

    async def update_watch_prices_bulk_async(self, updates: list) -> int:
        for watch, price, ts in updates:
            await self.update_watch_price_async(watch.watch_id, price, ts)
        return len(updates)


@pytest.mark.asyncio
async def test_poll_once_records_poll_and_gateway_success_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = _Manager()
    poller = SnapshotPoller(manager, gateway_url="http://gateway")
    poll_cycle_calls: list[dict] = []
    gateway_calls: list[dict] = []

    monkeypatch.setattr(
        poller_module,
        "record_watch_poll_cycle",
        lambda **kwargs: poll_cycle_calls.append(kwargs),
    )
    monkeypatch.setattr(
        poller_module,
        "record_watch_gateway_request",
        lambda **kwargs: gateway_calls.append(kwargs),
    )

    async def _fake_fetch_quotes(symbols: list[str]) -> dict[str, dict]:
        return {symbols[0]: {"bp": 1.0, "ap": 1.2, "timestamp": datetime.now(UTC).isoformat()}}

    monkeypatch.setattr(poller, "_fetch_quotes", _fake_fetch_quotes)

    stats = await poller.poll_once()

    assert stats["updated"] == 1
    assert any(call["status"] == "success" for call in poll_cycle_calls)
    assert any(call["outcome"] == "success" for call in gateway_calls)


@pytest.mark.asyncio
async def test_poll_once_records_error_when_due_watches_have_no_quotes(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = _Manager()
    poller = SnapshotPoller(manager, gateway_url="http://gateway")
    poll_cycle_calls: list[dict] = []

    monkeypatch.setattr(
        poller_module,
        "record_watch_poll_cycle",
        lambda **kwargs: poll_cycle_calls.append(kwargs),
    )

    async def _fake_fetch_quotes(symbols: list[str]) -> dict[str, dict]:  # noqa: ARG001
        return {}

    monkeypatch.setattr(poller, "_fetch_quotes", _fake_fetch_quotes)

    stats = await poller.poll_once()

    assert stats["due_watches"] == 1
    assert stats["quotes"] == 0
    assert stats["updated"] == 0
    assert stats["errors"] == 1
    assert any(call["status"] == "error" for call in poll_cycle_calls)


@pytest.mark.asyncio
async def test_fetch_batch_records_partial_coverage_outcome(monkeypatch: pytest.MonkeyPatch) -> None:
    poller = SnapshotPoller(SimpleNamespace(), gateway_url="http://gateway")
    gateway_calls: list[dict] = []
    monkeypatch.setattr(
        poller_module,
        "record_watch_gateway_request",
        lambda **kwargs: gateway_calls.append(kwargs),
    )

    first_route = "http://gateway/api/v1/alpaca/options/quotes"
    second_route = "http://gateway/alpaca/options/quotes"
    client = _StubClient(
        {
            first_route: _Response(
                200,
                {
                    "data": {
                        "quotes": {
                            "AAPL260220C00100000": {"bp": 1.0, "ap": 1.2},
                        }
                    }
                },
            ),
            second_route: _Response(
                200,
                {
                    "data": {
                        "quotes": {
                            "AAPL260220C00100000": {
                                "bp": 1.0,
                                "ap": 1.2,
                                "timestamp": "2020-01-01T00:00:00Z",
                            },
                            "MSFT260220C00100000": {
                                "bp": 2.0,
                                "ap": 2.4,
                                "timestamp": "2020-01-01T00:00:00Z",
                            },
                        }
                    }
                },
            ),
        }
    )

    quotes = await poller._fetch_batch(client, ["AAPL260220C00100000", "MSFT260220C00100000"])

    assert quotes
    assert any(call["outcome"] == "partial_coverage" for call in gateway_calls)


@pytest.mark.asyncio
async def test_fetch_batch_records_stale_fallback_outcome(monkeypatch: pytest.MonkeyPatch) -> None:
    poller = SnapshotPoller(SimpleNamespace(), gateway_url="http://gateway")
    gateway_calls: list[dict] = []
    monkeypatch.setattr(
        poller_module,
        "record_watch_gateway_request",
        lambda **kwargs: gateway_calls.append(kwargs),
    )

    first_route = "http://gateway/api/v1/alpaca/options/quotes"
    second_route = "http://gateway/alpaca/options/quotes"
    client = _StubClient(
        {
            first_route: _Response(
                200,
                {
                    "data": {
                        "quotes": {
                            "AAPL260220C00100000": {
                                "bp": 1.0,
                                "ap": 1.2,
                                "timestamp": "2020-01-01T00:00:00Z",
                            }
                        }
                    }
                },
            ),
            second_route: _Response(500, {"error": "upstream"}),
        }
    )

    quotes = await poller._fetch_batch(client, ["AAPL260220C00100000"])

    assert quotes
    assert any(call["outcome"] == "stale_fallback" for call in gateway_calls)
