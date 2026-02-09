from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from heber.watch import consumer as consumer_module
from heber.watch import poller as poller_module
from heber.watch.consumer import AlertWatchConsumer
from heber.watch.gateway import (
    classify_gateway_http_error,
    coerce_utc_timestamp,
    gateway_url_candidates,
)
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
    responses: dict[str, _StubResponse | Exception] = {}
    calls: list[tuple[str, dict[str, Any] | None]] = []

    def __init__(self, *args, **kwargs):  # noqa: ANN002, ANN003
        pass

    async def __aenter__(self) -> _StubAsyncClient:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False

    async def get(self, url: str, params: dict[str, Any] | None = None) -> _StubResponse:
        self.calls.append((url, params))
        response = self.responses[url]
        if isinstance(response, Exception):
            raise response
        return response


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


def test_classify_gateway_http_error_maps_timeout_and_transport() -> None:
    assert classify_gateway_http_error(httpx.ReadTimeout("timeout")) == "timeout"
    assert classify_gateway_http_error(httpx.ConnectError("connect")) == "transport_error"
    assert classify_gateway_http_error(httpx.HTTPError("error")) == "request_error"


def test_coerce_utc_timestamp_normalizes_epoch_milliseconds() -> None:
    assert coerce_utc_timestamp(1_704_067_200_000) == datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    assert coerce_utc_timestamp("1704067200000") == datetime(2024, 1, 1, 0, 0, tzinfo=UTC)


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
async def test_poller_fetch_quotes_falls_back_when_prefixed_route_times_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _StubAsyncClient.calls = []
    _StubAsyncClient.responses = {
        "http://gateway/api/v1/alpaca/options/quotes": httpx.ReadTimeout("timeout"),
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
async def test_poller_fetch_quotes_falls_back_when_prefixed_route_is_partial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _StubAsyncClient.calls = []
    _StubAsyncClient.responses = {
        "http://gateway/api/v1/alpaca/options/quotes": _StubResponse(
            200,
            {
                "data": {
                    "quotes": {
                        "AAPL260220C00100000": {"bp": 1.0, "ap": 1.2},
                    }
                }
            },
        ),
        "http://gateway/alpaca/options/quotes": _StubResponse(
            200,
            {
                "data": {
                    "quotes": {
                        "AAPL260220C00100000": {"bp": 1.0, "ap": 1.2},
                        "MSFT260220C00100000": {"bp": 2.0, "ap": 2.4},
                    }
                }
            },
        ),
    }
    monkeypatch.setattr(poller_module.httpx, "AsyncClient", _StubAsyncClient)

    poller = SnapshotPoller(SimpleNamespace(), gateway_url="http://gateway")
    quotes = await poller._fetch_quotes(["AAPL260220C00100000", "MSFT260220C00100000"])

    assert "AAPL260220C00100000" in quotes
    assert "MSFT260220C00100000" in quotes
    assert _StubAsyncClient.calls[0][0] == "http://gateway/api/v1/alpaca/options/quotes"
    assert _StubAsyncClient.calls[1][0] == "http://gateway/alpaca/options/quotes"


@pytest.mark.asyncio
async def test_poller_fetch_quotes_falls_back_when_prefixed_quote_item_shape_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _StubAsyncClient.calls = []
    _StubAsyncClient.responses = {
        "http://gateway/api/v1/alpaca/options/quotes": _StubResponse(
            200,
            {
                "data": {
                    "quotes": {
                        "AAPL260220C00100000": ["invalid"],
                    }
                }
            },
        ),
        "http://gateway/alpaca/options/quotes": _StubResponse(
            200,
            {
                "data": {
                    "quotes": {
                        "AAPL260220C00100000": {"bp": 1.0, "ap": 1.2},
                    }
                }
            },
        ),
    }
    monkeypatch.setattr(poller_module.httpx, "AsyncClient", _StubAsyncClient)

    poller = SnapshotPoller(SimpleNamespace(), gateway_url="http://gateway")
    quotes = await poller._fetch_quotes(["AAPL260220C00100000"])

    assert "AAPL260220C00100000" in quotes
    assert _StubAsyncClient.calls[0][0] == "http://gateway/api/v1/alpaca/options/quotes"
    assert _StubAsyncClient.calls[1][0] == "http://gateway/alpaca/options/quotes"


@pytest.mark.asyncio
async def test_poller_fetch_quotes_falls_back_when_prefixed_quotes_are_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stale_ts = (datetime.now(UTC) - timedelta(minutes=15)).isoformat()
    fresh_ts = datetime.now(UTC).isoformat()
    _StubAsyncClient.calls = []
    _StubAsyncClient.responses = {
        "http://gateway/api/v1/alpaca/options/quotes": _StubResponse(
            200,
            {"data": {"quotes": {"AAPL260220C00100000": {"bp": 1.0, "ap": 1.2, "timestamp": stale_ts}}}},
        ),
        "http://gateway/alpaca/options/quotes": _StubResponse(
            200,
            {"data": {"quotes": {"AAPL260220C00100000": {"bp": 2.0, "ap": 2.4, "timestamp": fresh_ts}}}},
        ),
    }
    monkeypatch.setattr(poller_module.httpx, "AsyncClient", _StubAsyncClient)

    poller = SnapshotPoller(SimpleNamespace(), gateway_url="http://gateway")
    quotes = await poller._fetch_quotes(["AAPL260220C00100000"])

    assert quotes["AAPL260220C00100000"]["bp"] == 2.0
    assert _StubAsyncClient.calls[0][0] == "http://gateway/api/v1/alpaca/options/quotes"
    assert _StubAsyncClient.calls[1][0] == "http://gateway/alpaca/options/quotes"


@pytest.mark.asyncio
async def test_poller_fetch_quotes_uses_stale_fallback_when_all_routes_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    older_ts = (datetime.now(UTC) - timedelta(minutes=30)).isoformat()
    newer_stale_ts = (datetime.now(UTC) - timedelta(minutes=10)).isoformat()
    _StubAsyncClient.calls = []
    _StubAsyncClient.responses = {
        "http://gateway/api/v1/alpaca/options/quotes": _StubResponse(
            200,
            {"data": {"quotes": {"AAPL260220C00100000": {"bp": 1.0, "ap": 1.2, "timestamp": older_ts}}}},
        ),
        "http://gateway/alpaca/options/quotes": _StubResponse(
            200,
            {"data": {"quotes": {"AAPL260220C00100000": {"bp": 1.5, "ap": 1.9, "timestamp": newer_stale_ts}}}},
        ),
    }
    monkeypatch.setattr(poller_module.httpx, "AsyncClient", _StubAsyncClient)

    poller = SnapshotPoller(SimpleNamespace(), gateway_url="http://gateway")
    quotes = await poller._fetch_quotes(["AAPL260220C00100000"])

    assert quotes["AAPL260220C00100000"]["bp"] == 1.5
    assert _StubAsyncClient.calls[0][0] == "http://gateway/api/v1/alpaca/options/quotes"
    assert _StubAsyncClient.calls[1][0] == "http://gateway/alpaca/options/quotes"


@pytest.mark.asyncio
async def test_poller_fallback_handles_epoch_millisecond_quote_timestamps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stale_ms = int((datetime.now(UTC) - timedelta(minutes=15)).timestamp() * 1000)
    fresh_ms = int(datetime.now(UTC).timestamp() * 1000)
    _StubAsyncClient.calls = []
    _StubAsyncClient.responses = {
        "http://gateway/api/v1/alpaca/options/quotes": _StubResponse(
            200,
            {"data": {"quotes": {"AAPL260220C00100000": {"bp": 1.0, "ap": 1.2, "t": stale_ms}}}},
        ),
        "http://gateway/alpaca/options/quotes": _StubResponse(
            200,
            {"data": {"quotes": {"AAPL260220C00100000": {"bp": 2.0, "ap": 2.4, "t": fresh_ms}}}},
        ),
    }
    monkeypatch.setattr(poller_module.httpx, "AsyncClient", _StubAsyncClient)

    poller = SnapshotPoller(SimpleNamespace(), gateway_url="http://gateway")
    quotes = await poller._fetch_quotes(["AAPL260220C00100000"])

    assert quotes["AAPL260220C00100000"]["bp"] == 2.0
    assert _StubAsyncClient.calls[0][0] == "http://gateway/api/v1/alpaca/options/quotes"
    assert _StubAsyncClient.calls[1][0] == "http://gateway/alpaca/options/quotes"


@pytest.mark.asyncio
async def test_poller_fetch_quotes_emits_standardized_failure_taxonomy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _StubAsyncClient.calls = []
    _StubAsyncClient.responses = {
        "http://gateway/api/v1/alpaca/options/quotes": httpx.ReadTimeout("timeout"),
        "http://gateway/alpaca/options/quotes": httpx.ConnectError("connect"),
    }
    captured_warnings: list[tuple[str, dict[str, Any]]] = []

    def _capture_warning(event: str, **kwargs: Any) -> None:
        captured_warnings.append((event, kwargs))

    monkeypatch.setattr(poller_module.httpx, "AsyncClient", _StubAsyncClient)
    monkeypatch.setattr(poller_module.logger, "warning", _capture_warning)

    poller = SnapshotPoller(SimpleNamespace(), gateway_url="http://gateway")
    quotes = await poller._fetch_quotes(["AAPL260220C00100000"])

    assert quotes == {}
    summary = next(kwargs for event, kwargs in captured_warnings if event == "Quote fetch failed across routes")
    assert summary["failures"][0]["failure"] == "timeout"
    assert summary["failures"][0]["error_type"] == "ReadTimeout"
    assert summary["failures"][1]["failure"] == "transport_error"
    assert summary["failures"][1]["error_type"] == "ConnectError"


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
async def test_consumer_entry_price_falls_back_when_prefixed_route_times_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _StubAsyncClient.calls = []
    _StubAsyncClient.responses = {
        "http://gateway/api/v1/alpaca/options/quotes": httpx.ReadTimeout("timeout"),
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
async def test_consumer_entry_price_falls_back_when_prefixed_symbol_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _StubAsyncClient.calls = []
    _StubAsyncClient.responses = {
        "http://gateway/api/v1/alpaca/options/quotes": _StubResponse(
            200,
            {"data": {"quotes": {"OTHER": {"bp": 0.9, "ap": 1.1}}}},
        ),
        "http://gateway/alpaca/options/quotes": _StubResponse(
            200,
            {"data": {"quotes": {"AAPL260220C00100000": {"bp": 1.0, "ap": 1.2}}}},
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
async def test_consumer_entry_price_falls_back_when_prefixed_symbol_shape_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _StubAsyncClient.calls = []
    _StubAsyncClient.responses = {
        "http://gateway/api/v1/alpaca/options/quotes": _StubResponse(
            200,
            {"data": {"quotes": {"AAPL260220C00100000": ["invalid"]}}},
        ),
        "http://gateway/alpaca/options/quotes": _StubResponse(
            200,
            {"data": {"quotes": {"AAPL260220C00100000": {"bp": 1.0, "ap": 1.2}}}},
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
async def test_consumer_entry_price_falls_back_when_prefixed_quote_is_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stale_ts = (datetime.now(UTC) - timedelta(minutes=15)).isoformat()
    fresh_ts = datetime.now(UTC).isoformat()
    _StubAsyncClient.calls = []
    _StubAsyncClient.responses = {
        "http://gateway/api/v1/alpaca/options/quotes": _StubResponse(
            200,
            {"data": {"quotes": {"AAPL260220C00100000": {"bp": 1.0, "ap": 1.2, "timestamp": stale_ts}}}},
        ),
        "http://gateway/alpaca/options/quotes": _StubResponse(
            200,
            {"data": {"quotes": {"AAPL260220C00100000": {"bp": 2.0, "ap": 2.4, "timestamp": fresh_ts}}}},
        ),
    }
    monkeypatch.setattr(consumer_module.httpx, "AsyncClient", _StubAsyncClient)

    consumer = AlertWatchConsumer(
        redis_client=SimpleNamespace(),
        watch_manager=SimpleNamespace(),
        gateway_url="http://gateway",
    )

    price = await consumer._get_entry_price("AAPL260220C00100000")

    assert price == 2.2
    assert _StubAsyncClient.calls[0][0] == "http://gateway/api/v1/alpaca/options/quotes"
    assert _StubAsyncClient.calls[1][0] == "http://gateway/alpaca/options/quotes"


@pytest.mark.asyncio
async def test_consumer_entry_price_uses_freshest_stale_fallback_when_all_routes_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    older_ts = (datetime.now(UTC) - timedelta(minutes=30)).isoformat()
    newer_stale_ts = (datetime.now(UTC) - timedelta(minutes=10)).isoformat()
    _StubAsyncClient.calls = []
    _StubAsyncClient.responses = {
        "http://gateway/api/v1/alpaca/options/quotes": _StubResponse(
            200,
            {"data": {"quotes": {"AAPL260220C00100000": {"bp": 1.0, "ap": 1.2, "timestamp": older_ts}}}},
        ),
        "http://gateway/alpaca/options/quotes": _StubResponse(
            200,
            {"data": {"quotes": {"AAPL260220C00100000": {"bp": 1.5, "ap": 1.9, "timestamp": newer_stale_ts}}}},
        ),
    }
    monkeypatch.setattr(consumer_module.httpx, "AsyncClient", _StubAsyncClient)

    consumer = AlertWatchConsumer(
        redis_client=SimpleNamespace(),
        watch_manager=SimpleNamespace(),
        gateway_url="http://gateway",
    )

    price = await consumer._get_entry_price("AAPL260220C00100000")

    assert price == 1.7
    assert _StubAsyncClient.calls[0][0] == "http://gateway/api/v1/alpaca/options/quotes"
    assert _StubAsyncClient.calls[1][0] == "http://gateway/alpaca/options/quotes"


@pytest.mark.asyncio
async def test_consumer_entry_price_emits_standardized_failure_taxonomy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _StubAsyncClient.calls = []
    _StubAsyncClient.responses = {
        "http://gateway/api/v1/alpaca/options/quotes": httpx.ReadTimeout("timeout"),
        "http://gateway/alpaca/options/quotes": httpx.ConnectError("connect"),
    }
    captured_warnings: list[tuple[str, dict[str, Any]]] = []

    def _capture_warning(event: str, **kwargs: Any) -> None:
        captured_warnings.append((event, kwargs))

    monkeypatch.setattr(consumer_module.httpx, "AsyncClient", _StubAsyncClient)
    monkeypatch.setattr(consumer_module.logger, "warning", _capture_warning)

    consumer = AlertWatchConsumer(
        redis_client=SimpleNamespace(),
        watch_manager=SimpleNamespace(),
        gateway_url="http://gateway",
    )

    price = await consumer._get_entry_price("AAPL260220C00100000")

    assert price is None
    summary = next(kwargs for event, kwargs in captured_warnings if event == "Entry price fetch failed across routes")
    assert summary["failures"][0]["failure"] == "timeout"
    assert summary["failures"][0]["error_type"] == "ReadTimeout"
    assert summary["failures"][1]["failure"] == "transport_error"
    assert summary["failures"][1]["error_type"] == "ConnectError"


@pytest.mark.asyncio
async def test_consumer_entry_price_failure_metadata_includes_expected_payload_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _StubAsyncClient.calls = []
    _StubAsyncClient.responses = {
        "http://gateway/api/v1/alpaca/options/quotes": _StubResponse(200, {"data": []}),
        "http://gateway/alpaca/options/quotes": _StubResponse(200, {"data": {"quotes": []}}),
    }
    captured_warnings: list[tuple[str, dict[str, Any]]] = []

    def _capture_warning(event: str, **kwargs: Any) -> None:
        captured_warnings.append((event, kwargs))

    monkeypatch.setattr(consumer_module.httpx, "AsyncClient", _StubAsyncClient)
    monkeypatch.setattr(consumer_module.logger, "warning", _capture_warning)

    consumer = AlertWatchConsumer(
        redis_client=SimpleNamespace(),
        watch_manager=SimpleNamespace(),
        gateway_url="http://gateway",
    )

    price = await consumer._get_entry_price("AAPL260220C00100000")

    assert price is None
    summary = next(kwargs for event, kwargs in captured_warnings if event == "Entry price fetch failed across routes")
    assert summary["failures"][0]["failure"] == "data_payload_shape"
    assert summary["failures"][0]["expected_type"] == "dict"
    assert summary["failures"][0]["payload_type"] == "list"
    assert summary["failures"][1]["failure"] == "quotes_payload_shape"
    assert summary["failures"][1]["expected_type"] == "dict"
    assert summary["failures"][1]["payload_type"] == "list"


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
