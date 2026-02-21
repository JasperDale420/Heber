from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from heber.watch import consumer as consumer_module
from heber.watch import poller as poller_module
from heber.watch.consumer import AlertWatchConsumer
from heber.watch.features import AlertFeatureExtractor
from heber.watch.poller import SnapshotPoller

TEST_GATEWAY_KEY = "gw_test_dev_key_67890"  # pragma: allowlist secret


class _Response:
    def __init__(self, status_code: int, payload: dict[str, Any]):
        self.status_code = status_code
        self._payload = payload

    def json(self) -> dict[str, Any]:
        return self._payload


class _CapturingAsyncClient:
    responses: dict[str, _Response] = {}
    calls: list[tuple[str, dict[str, Any] | None, dict[str, str] | None]] = []

    def __init__(self, *args, **kwargs):  # noqa: ANN002, ANN003
        pass

    async def __aenter__(self) -> _CapturingAsyncClient:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False

    async def get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> _Response:
        self.calls.append((url, params, headers))
        return self.responses[url]


@pytest.mark.asyncio
async def test_feature_extractor_sends_gateway_auth_header(monkeypatch: pytest.MonkeyPatch) -> None:
    _CapturingAsyncClient.calls = []
    _CapturingAsyncClient.responses = {
        "http://gateway/api/v1/uw/options/AAPL/iv-rank": _Response(200, {"data": {"iv_rank": 66.0}}),
    }
    monkeypatch.setattr("httpx.AsyncClient", _CapturingAsyncClient)

    extractor = AlertFeatureExtractor(
        gateway_url="http://gateway",
        gateway_api_key=TEST_GATEWAY_KEY,
        request_max_attempts=1,
    )
    payload = await extractor._request_json_with_retry(
        endpoint="uw_iv_rank",
        routes=["http://gateway/api/v1/uw/options/AAPL/iv-rank"],
        symbol="AAPL",
    )

    assert payload == {"data": {"iv_rank": 66.0}}
    assert _CapturingAsyncClient.calls
    assert _CapturingAsyncClient.calls[0][2] == {"X-Gateway-Key": TEST_GATEWAY_KEY}


@pytest.mark.asyncio
async def test_poller_sends_gateway_auth_header(monkeypatch: pytest.MonkeyPatch) -> None:
    _CapturingAsyncClient.calls = []
    _CapturingAsyncClient.responses = {
        "http://gateway/api/v1/alpaca/options/quotes": _Response(
            200,
            {"data": {"quotes": {"AAPL260220C00100000": {"bp": 1.0, "ap": 1.2}}}},
        ),
    }
    monkeypatch.setattr(poller_module.httpx, "AsyncClient", _CapturingAsyncClient)

    poller = SnapshotPoller(
        SimpleNamespace(),
        gateway_url="http://gateway",
        gateway_api_key=TEST_GATEWAY_KEY,
    )
    quotes = await poller._fetch_quotes(["AAPL260220C00100000"])

    assert "AAPL260220C00100000" in quotes
    assert _CapturingAsyncClient.calls
    assert _CapturingAsyncClient.calls[0][2] == {"X-Gateway-Key": TEST_GATEWAY_KEY}


@pytest.mark.asyncio
async def test_consumer_sends_gateway_auth_header(monkeypatch: pytest.MonkeyPatch) -> None:
    _CapturingAsyncClient.calls = []
    _CapturingAsyncClient.responses = {
        "http://gateway/api/v1/alpaca/options/quotes": _Response(
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
    monkeypatch.setattr(consumer_module.httpx, "AsyncClient", _CapturingAsyncClient)

    consumer = AlertWatchConsumer(
        redis_client=SimpleNamespace(),
        watch_manager=SimpleNamespace(),
        gateway_url="http://gateway",
        gateway_api_key=TEST_GATEWAY_KEY,
    )
    price = await consumer._get_entry_price("AAPL260220C00100000")

    assert price == pytest.approx(1.1)
    assert _CapturingAsyncClient.calls
    assert _CapturingAsyncClient.calls[0][2] == {"X-Gateway-Key": TEST_GATEWAY_KEY}
