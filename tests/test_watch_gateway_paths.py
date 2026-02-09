from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from heber.watch import consumer as consumer_module
from heber.watch import poller as poller_module
from heber.watch.consumer import AlertWatchConsumer
from heber.watch.gateway import gateway_url_candidates
from heber.watch.poller import SnapshotPoller


class _StubResponse:
    def __init__(self, status_code: int, payload: dict[str, Any]):
        self.status_code = status_code
        self._payload = payload

    def json(self) -> dict[str, Any]:
        return self._payload


class _InvalidJsonResponse(_StubResponse):
    def __init__(self, status_code: int):
        super().__init__(status_code=status_code, payload={})

    def json(self) -> dict[str, Any]:
        raise ValueError("invalid json")


class _StubAsyncClient:
    responses: dict[str, _StubResponse] = {}
    calls: list[tuple[str, dict[str, Any] | None]] = []

    def __init__(self, *args, **kwargs):  # noqa: ANN002, ANN003
        pass

    async def __aenter__(self) -> _StubAsyncClient:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False

    async def get(self, url: str, params: dict[str, Any] | None = None) -> _StubResponse:
        self.calls.append((url, params))
        return self.responses[url]


def test_gateway_url_candidates_prefix_first_and_deduped() -> None:
    candidates = gateway_url_candidates("http://gateway", "/alpaca/options/quotes")
    assert candidates == [
        "http://gateway/api/v1/alpaca/options/quotes",
        "http://gateway/alpaca/options/quotes",
    ]

    already_prefixed = gateway_url_candidates("http://gateway/", "/api/v1/alpaca/options/quotes")
    assert already_prefixed == ["http://gateway/api/v1/alpaca/options/quotes"]


def test_gateway_url_candidates_normalizes_prefix_without_leading_slash() -> None:
    candidates = gateway_url_candidates(
        "http://gateway",
        "/alpaca/options/quotes",
        api_prefix="api/v1",
    )
    assert candidates == [
        "http://gateway/api/v1/alpaca/options/quotes",
        "http://gateway/alpaca/options/quotes",
    ]


def test_gateway_url_candidates_avoids_double_prefix_when_base_already_has_prefix() -> None:
    candidates = gateway_url_candidates(
        "http://gateway/api/v1",
        "/alpaca/options/quotes",
    )
    assert candidates == [
        "http://gateway/api/v1/alpaca/options/quotes",
        "http://gateway/alpaca/options/quotes",
    ]


def test_gateway_url_candidates_does_not_treat_partial_suffix_as_prefix() -> None:
    candidates = gateway_url_candidates(
        "http://gateway/notapi/v1",
        "/alpaca/options/quotes",
    )
    assert candidates == [
        "http://gateway/notapi/v1/api/v1/alpaca/options/quotes",
        "http://gateway/notapi/v1/alpaca/options/quotes",
    ]


def test_gateway_url_candidates_strips_query_from_base_url() -> None:
    candidates = gateway_url_candidates(
        "http://gateway/api/v1?token=abc123",
        "/alpaca/options/quotes",
    )
    assert candidates == [
        "http://gateway/api/v1/alpaca/options/quotes",
        "http://gateway/alpaca/options/quotes",
    ]


@pytest.mark.asyncio
async def test_poller_fetch_quotes_falls_back_to_legacy_route(monkeypatch: pytest.MonkeyPatch) -> None:
    _StubAsyncClient.calls = []
    _StubAsyncClient.responses = {
        "http://gateway/api/v1/alpaca/options/quotes": _StubResponse(404, {}),
        "http://gateway/alpaca/options/quotes": _StubResponse(
            200,
            {"data": {"quotes": {"AAPL260220C00100000": {"bp": 1.0, "ap": 1.2}}}},
        ),
    }
    monkeypatch.setattr(poller_module.httpx, "AsyncClient", _StubAsyncClient)

    poller = SnapshotPoller(SimpleNamespace(), gateway_url="http://gateway")
    quotes = await poller._fetch_quotes(["AAPL260220C00100000"])

    assert "AAPL260220C00100000" in quotes
    assert _StubAsyncClient.calls[0][0] == "http://gateway/api/v1/alpaca/options/quotes"
    assert _StubAsyncClient.calls[1][0] == "http://gateway/alpaca/options/quotes"


@pytest.mark.asyncio
async def test_poller_fetch_quotes_falls_back_when_prefixed_json_is_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _StubAsyncClient.calls = []
    _StubAsyncClient.responses = {
        "http://gateway/api/v1/alpaca/options/quotes": _InvalidJsonResponse(200),
        "http://gateway/alpaca/options/quotes": _StubResponse(
            200,
            {"data": {"quotes": {"AAPL260220C00100000": {"bp": 1.0, "ap": 1.2}}}},
        ),
    }
    monkeypatch.setattr(poller_module.httpx, "AsyncClient", _StubAsyncClient)

    poller = SnapshotPoller(SimpleNamespace(), gateway_url="http://gateway")
    quotes = await poller._fetch_quotes(["AAPL260220C00100000"])

    assert "AAPL260220C00100000" in quotes
    assert _StubAsyncClient.calls[0][0] == "http://gateway/api/v1/alpaca/options/quotes"
    assert _StubAsyncClient.calls[1][0] == "http://gateway/alpaca/options/quotes"


@pytest.mark.asyncio
async def test_poller_fetch_quotes_falls_back_when_prefixed_payload_shape_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _StubAsyncClient.calls = []
    _StubAsyncClient.responses = {
        "http://gateway/api/v1/alpaca/options/quotes": _StubResponse(
            200,
            {"data": {"quotes": []}},
        ),
        "http://gateway/alpaca/options/quotes": _StubResponse(
            200,
            {"data": {"quotes": {"AAPL260220C00100000": {"bp": 1.0, "ap": 1.2}}}},
        ),
    }
    monkeypatch.setattr(poller_module.httpx, "AsyncClient", _StubAsyncClient)

    poller = SnapshotPoller(SimpleNamespace(), gateway_url="http://gateway")
    quotes = await poller._fetch_quotes(["AAPL260220C00100000"])

    assert "AAPL260220C00100000" in quotes
    assert _StubAsyncClient.calls[0][0] == "http://gateway/api/v1/alpaca/options/quotes"
    assert _StubAsyncClient.calls[1][0] == "http://gateway/alpaca/options/quotes"


@pytest.mark.asyncio
async def test_consumer_entry_price_falls_back_to_legacy_route(monkeypatch: pytest.MonkeyPatch) -> None:
    _StubAsyncClient.calls = []
    _StubAsyncClient.responses = {
        "http://gateway/api/v1/alpaca/options/quotes": _StubResponse(404, {}),
        "http://gateway/alpaca/options/quotes": _StubResponse(
            200,
            {
                "data": {
                    "quotes": {
                        "AAPL260220C00100000": {
                            "bp": 1.0,
                            "ap": 1.2,
                        }
                    }
                }
            },
        ),
    }
    monkeypatch.setattr(consumer_module.httpx, "AsyncClient", _StubAsyncClient)

    consumer = AlertWatchConsumer(
        redis_client=SimpleNamespace(),
        watch_manager=SimpleNamespace(),
        gateway_url="http://gateway",
    )

    price = await consumer._get_entry_price("AAPL260220C00100000")

    assert price == 1.1
    assert _StubAsyncClient.calls[0][0] == "http://gateway/api/v1/alpaca/options/quotes"
    assert _StubAsyncClient.calls[1][0] == "http://gateway/alpaca/options/quotes"


@pytest.mark.asyncio
async def test_consumer_entry_price_falls_back_when_prefixed_json_is_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _StubAsyncClient.calls = []
    _StubAsyncClient.responses = {
        "http://gateway/api/v1/alpaca/options/quotes": _InvalidJsonResponse(200),
        "http://gateway/alpaca/options/quotes": _StubResponse(
            200,
            {
                "data": {
                    "quotes": {
                        "AAPL260220C00100000": {
                            "bp": 1.0,
                            "ap": 1.2,
                        }
                    }
                }
            },
        ),
    }
    monkeypatch.setattr(consumer_module.httpx, "AsyncClient", _StubAsyncClient)

    consumer = AlertWatchConsumer(
        redis_client=SimpleNamespace(),
        watch_manager=SimpleNamespace(),
        gateway_url="http://gateway",
    )

    price = await consumer._get_entry_price("AAPL260220C00100000")

    assert price == 1.1
    assert _StubAsyncClient.calls[0][0] == "http://gateway/api/v1/alpaca/options/quotes"
    assert _StubAsyncClient.calls[1][0] == "http://gateway/alpaca/options/quotes"


@pytest.mark.asyncio
async def test_consumer_entry_price_falls_back_when_prefixed_payload_shape_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _StubAsyncClient.calls = []
    _StubAsyncClient.responses = {
        "http://gateway/api/v1/alpaca/options/quotes": _StubResponse(
            200,
            {"data": {"quotes": []}},
        ),
        "http://gateway/alpaca/options/quotes": _StubResponse(
            200,
            {
                "data": {
                    "quotes": {
                        "AAPL260220C00100000": {
                            "bp": 1.0,
                            "ap": 1.2,
                        }
                    }
                }
            },
        ),
    }
    monkeypatch.setattr(consumer_module.httpx, "AsyncClient", _StubAsyncClient)

    consumer = AlertWatchConsumer(
        redis_client=SimpleNamespace(),
        watch_manager=SimpleNamespace(),
        gateway_url="http://gateway",
    )

    price = await consumer._get_entry_price("AAPL260220C00100000")

    assert price == 1.1
    assert _StubAsyncClient.calls[0][0] == "http://gateway/api/v1/alpaca/options/quotes"
    assert _StubAsyncClient.calls[1][0] == "http://gateway/alpaca/options/quotes"


@pytest.mark.asyncio
async def test_consumer_entry_price_keeps_zero_bid_quotes(monkeypatch: pytest.MonkeyPatch) -> None:
    _StubAsyncClient.calls = []
    _StubAsyncClient.responses = {
        "http://gateway/api/v1/alpaca/options/quotes": _StubResponse(
            200,
            {
                "data": {
                    "quotes": {
                        "AAPL260220C00100000": {
                            "bp": 0.0,
                            "ap": 1.2,
                        }
                    }
                }
            },
        ),
    }
    monkeypatch.setattr(consumer_module.httpx, "AsyncClient", _StubAsyncClient)

    consumer = AlertWatchConsumer(
        redis_client=SimpleNamespace(),
        watch_manager=SimpleNamespace(),
        gateway_url="http://gateway",
    )

    price = await consumer._get_entry_price("AAPL260220C00100000")
    assert price == 0.6


@pytest.mark.asyncio
async def test_consumer_entry_price_uses_last_price_when_bid_ask_not_numeric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _StubAsyncClient.calls = []
    _StubAsyncClient.responses = {
        "http://gateway/api/v1/alpaca/options/quotes": _StubResponse(
            200,
            {
                "data": {
                    "quotes": {
                        "AAPL260220C00100000": {
                            "bp": "N/A",
                            "ap": "bad",
                            "last_price": "1.25",
                        }
                    }
                }
            },
        ),
    }
    monkeypatch.setattr(consumer_module.httpx, "AsyncClient", _StubAsyncClient)

    consumer = AlertWatchConsumer(
        redis_client=SimpleNamespace(),
        watch_manager=SimpleNamespace(),
        gateway_url="http://gateway",
    )

    price = await consumer._get_entry_price("AAPL260220C00100000")
    assert price == 1.25


@pytest.mark.asyncio
async def test_consumer_entry_price_uses_last_price_when_bid_ask_non_finite(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _StubAsyncClient.calls = []
    _StubAsyncClient.responses = {
        "http://gateway/api/v1/alpaca/options/quotes": _StubResponse(
            200,
            {
                "data": {
                    "quotes": {
                        "AAPL260220C00100000": {
                            "bp": "NaN",
                            "ap": "inf",
                            "last_price": "1.25",
                        }
                    }
                }
            },
        ),
    }
    monkeypatch.setattr(consumer_module.httpx, "AsyncClient", _StubAsyncClient)

    consumer = AlertWatchConsumer(
        redis_client=SimpleNamespace(),
        watch_manager=SimpleNamespace(),
        gateway_url="http://gateway",
    )

    price = await consumer._get_entry_price("AAPL260220C00100000")
    assert price == 1.25
