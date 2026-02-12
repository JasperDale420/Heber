from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime

import pytest

from heber.watch import features as features_module
from heber.watch.features import AlertFeatureExtractor, AlertFeatures, EnrichmentAuthFailure


class _Response:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload

    def json(self) -> dict:
        return self._payload


class _SequencedClient:
    responses: list[_Response] = []
    calls: int = 0

    def __init__(self, *args, **kwargs):  # noqa: ANN002, ANN003
        pass

    async def __aenter__(self) -> _SequencedClient:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False

    async def get(self, url: str, params: dict | None = None) -> _Response:  # noqa: ARG002
        idx = min(type(self).calls, len(type(self).responses) - 1)
        response = type(self).responses[idx]
        type(self).calls += 1
        return response


class _ThrottleClient:
    active_calls = 0
    max_active_calls = 0

    def __init__(self, *args, **kwargs):  # noqa: ANN002, ANN003
        pass

    async def __aenter__(self) -> _ThrottleClient:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False

    async def get(self, url: str, params: dict | None = None) -> _Response:  # noqa: ARG002
        _ThrottleClient.active_calls += 1
        _ThrottleClient.max_active_calls = max(_ThrottleClient.max_active_calls, _ThrottleClient.active_calls)
        await asyncio.sleep(0.05)
        _ThrottleClient.active_calls -= 1
        return _Response(200, {"data": {"iv_rank": 42.0}})


def _base_features(symbol: str = "AAPL") -> AlertFeatures:
    return AlertFeatures(
        alert_id="a1",
        alert_time=datetime(2026, 2, 7, 15, 0, tzinfo=UTC),
        symbol=symbol,
        occ_symbol=f"{symbol}260220C00100000",
        underlying=symbol,
        strike=100.0,
        expiry=date(2026, 2, 20),
        put_call="C",
        days_to_expiry=13,
        premium=12500.0,
        volume=100.0,
        open_interest=200.0,
        volume_oi_ratio=0.5,
        alert_type="SWEEP",
        side="ask",
        aggressor="ask",
        spot_price=195.0,
        contract_price=1.25,
    )


@pytest.mark.asyncio
async def test_enrich_iv_rank_retries_on_429_until_success(monkeypatch: pytest.MonkeyPatch) -> None:
    _SequencedClient.calls = 0
    _SequencedClient.responses = [
        _Response(429, {"error": "rate limited"}),
        _Response(200, {"data": {"iv_rank": 77.0}}),
    ]
    gateway_calls: list[dict] = []
    gateway_success_calls: list[dict] = []
    monkeypatch.setattr("httpx.AsyncClient", _SequencedClient)
    monkeypatch.setattr(
        features_module,
        "record_watch_gateway_request",
        lambda **kwargs: gateway_calls.append(kwargs),
    )
    monkeypatch.setattr(
        features_module,
        "record_watch_gateway_success",
        lambda **kwargs: gateway_success_calls.append(kwargs),
    )

    extractor = AlertFeatureExtractor(
        gateway_url="http://gateway:8000",
        request_max_attempts=2,
        retry_base_delay_seconds=0.0,
        retry_jitter_seconds=0.0,
    )

    enriched = await extractor._enrich_iv_rank(_base_features())

    assert enriched.iv_rank == 77.0
    assert _SequencedClient.calls == 2
    assert any(call["outcome"] == "http_error" and call["status_code"] == 429 for call in gateway_calls)
    assert any(call["outcome"] == "success" and call["status_code"] == 200 for call in gateway_calls)
    assert gateway_success_calls


@pytest.mark.asyncio
async def test_enrich_iv_rank_logs_when_429_retries_exhausted(monkeypatch: pytest.MonkeyPatch) -> None:
    _SequencedClient.calls = 0
    _SequencedClient.responses = [_Response(429, {"error": "rate limited"})]
    warning_calls: list[tuple[str, dict]] = []
    gateway_calls: list[dict] = []

    def _capture_warning(message: str, **kwargs) -> None:  # noqa: ANN003
        warning_calls.append((message, kwargs))

    monkeypatch.setattr("httpx.AsyncClient", _SequencedClient)
    monkeypatch.setattr(features_module.logger, "warning", _capture_warning)
    monkeypatch.setattr(
        features_module,
        "record_watch_gateway_request",
        lambda **kwargs: gateway_calls.append(kwargs),
    )

    extractor = AlertFeatureExtractor(
        gateway_url="http://gateway:8000",
        request_max_attempts=2,
        retry_base_delay_seconds=0.0,
        retry_jitter_seconds=0.0,
    )

    enriched = await extractor._enrich_iv_rank(_base_features())

    assert enriched.iv_rank is None
    failure_logs = [entry for entry in warning_calls if entry[0] == "Feature enrichment request failed"]
    assert failure_logs
    assert all(log[1]["status_code"] == 429 for log in failure_logs)
    assert all(log[1]["retryable"] is True for log in failure_logs)
    assert gateway_calls
    assert all(call["outcome"] == "http_error" for call in gateway_calls)
    assert all(call["status_code"] == 429 for call in gateway_calls)


@pytest.mark.asyncio
async def test_enrichment_raises_fatal_when_401_threshold_crossed(monkeypatch: pytest.MonkeyPatch) -> None:
    _SequencedClient.calls = 0
    _SequencedClient.responses = [_Response(401, {"error": "unauthorized"})]
    monkeypatch.setattr("httpx.AsyncClient", _SequencedClient)

    extractor = AlertFeatureExtractor(
        gateway_url="http://gateway:8000",
        request_max_attempts=1,
        auth_failure_threshold=3,
        auth_failure_window_seconds=300,
    )

    await extractor._enrich_iv_rank(_base_features("AAPL"))

    with pytest.raises(EnrichmentAuthFailure):
        await extractor._enrich_iv_rank(_base_features("MSFT"))


@pytest.mark.asyncio
async def test_request_throttle_limits_concurrency(monkeypatch: pytest.MonkeyPatch) -> None:
    _ThrottleClient.active_calls = 0
    _ThrottleClient.max_active_calls = 0
    monkeypatch.setattr("httpx.AsyncClient", _ThrottleClient)

    extractor = AlertFeatureExtractor(
        gateway_url="http://gateway:8000",
        request_max_attempts=1,
        max_concurrent_requests=1,
        retry_base_delay_seconds=0.0,
        retry_jitter_seconds=0.0,
    )

    async def _run(symbol: str) -> None:
        await extractor._request_json_with_retry(
            endpoint="iv_rank",
            routes=["http://gateway/api/v1/uw/options/AAPL/iv-rank"],
            params={"symbol": symbol},
            symbol=symbol,
            alert_id="a1",
        )

    await asyncio.gather(_run("AAPL"), _run("MSFT"))

    assert _ThrottleClient.max_active_calls == 1
