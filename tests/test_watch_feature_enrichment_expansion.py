"""Tests for GEX, Max Pain, and Market Tide per-alert enrichment."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from heber.watch.features import AlertFeatureExtractor, AlertFeatures


class _Response:
    def __init__(self, status_code: int, payload: dict, headers: dict | None = None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

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

    async def get(
        self,
        url: str,
        params: dict | None = None,  # noqa: ARG002
        headers: dict[str, str] | None = None,  # noqa: ARG002
    ) -> _Response:
        idx = min(type(self).calls, len(type(self).responses) - 1)
        response = type(self).responses[idx]
        type(self).calls += 1
        return response


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


def _make_extractor() -> AlertFeatureExtractor:
    return AlertFeatureExtractor(
        gateway_url="http://gateway:8000",
        request_max_attempts=1,
        retry_base_delay_seconds=0.0,
        retry_jitter_seconds=0.0,
    )


# --- GEX enrichment tests ---


@pytest.mark.asyncio
async def test_enrich_gex_populates_gex_and_vex(monkeypatch: pytest.MonkeyPatch) -> None:
    _SequencedClient.calls = 0
    _SequencedClient.responses = [
        _Response(200, {"data": {"gamma_exposure": 1_500_000.0, "vanna_exposure": -320_000.0}}),
    ]
    monkeypatch.setattr("httpx.AsyncClient", _SequencedClient)

    extractor = _make_extractor()
    enriched = await extractor._enrich_gex(_base_features())

    assert enriched.gex == 1_500_000.0
    assert enriched.vex == -320_000.0


@pytest.mark.asyncio
async def test_enrich_gex_handles_list_response(monkeypatch: pytest.MonkeyPatch) -> None:
    _SequencedClient.calls = 0
    _SequencedClient.responses = [
        _Response(200, {"data": [{"gex_oi": 750_000.0, "vex_oi": -100_000.0}]}),
    ]
    monkeypatch.setattr("httpx.AsyncClient", _SequencedClient)

    extractor = _make_extractor()
    enriched = await extractor._enrich_gex(_base_features())

    assert enriched.gex == 750_000.0
    assert enriched.vex == -100_000.0


@pytest.mark.asyncio
async def test_enrich_gex_no_gateway_url_skips() -> None:
    extractor = AlertFeatureExtractor(gateway_url=None)
    features = _base_features()
    enriched = await extractor._enrich_gex(features)

    assert enriched.gex is None
    assert enriched.vex is None


@pytest.mark.asyncio
async def test_enrich_gex_gateway_failure_leaves_none(monkeypatch: pytest.MonkeyPatch) -> None:
    _SequencedClient.calls = 0
    _SequencedClient.responses = [_Response(500, {"error": "internal"})]
    monkeypatch.setattr("httpx.AsyncClient", _SequencedClient)

    extractor = _make_extractor()
    enriched = await extractor._enrich_gex(_base_features())

    assert enriched.gex is None
    assert enriched.vex is None


# --- Max Pain enrichment tests ---


@pytest.mark.asyncio
async def test_enrich_max_pain_populates_strike_and_distance(monkeypatch: pytest.MonkeyPatch) -> None:
    _SequencedClient.calls = 0
    _SequencedClient.responses = [
        _Response(200, {"data": {"max_pain_strike": 190.0}}),
    ]
    monkeypatch.setattr("httpx.AsyncClient", _SequencedClient)

    extractor = _make_extractor()
    features = _base_features()
    features.spot_price = 200.0
    enriched = await extractor._enrich_max_pain(features)

    assert enriched.max_pain_strike == 190.0
    assert enriched.max_pain_distance_pct == pytest.approx((200.0 - 190.0) / 200.0)


@pytest.mark.asyncio
async def test_enrich_max_pain_handles_list_response(monkeypatch: pytest.MonkeyPatch) -> None:
    _SequencedClient.calls = 0
    _SequencedClient.responses = [
        _Response(200, {"data": [{"max_pain": 185.0}]}),
    ]
    monkeypatch.setattr("httpx.AsyncClient", _SequencedClient)

    extractor = _make_extractor()
    features = _base_features()
    features.spot_price = 200.0
    enriched = await extractor._enrich_max_pain(features)

    assert enriched.max_pain_strike == 185.0
    assert enriched.max_pain_distance_pct == pytest.approx((200.0 - 185.0) / 200.0)


@pytest.mark.asyncio
async def test_enrich_max_pain_no_spot_price_skips_distance(monkeypatch: pytest.MonkeyPatch) -> None:
    _SequencedClient.calls = 0
    _SequencedClient.responses = [
        _Response(200, {"data": {"max_pain_strike": 190.0}}),
    ]
    monkeypatch.setattr("httpx.AsyncClient", _SequencedClient)

    extractor = _make_extractor()
    features = _base_features()
    features.spot_price = None
    enriched = await extractor._enrich_max_pain(features)

    assert enriched.max_pain_strike == 190.0
    assert enriched.max_pain_distance_pct is None


@pytest.mark.asyncio
async def test_enrich_max_pain_no_gateway_url_skips() -> None:
    extractor = AlertFeatureExtractor(gateway_url=None)
    enriched = await extractor._enrich_max_pain(_base_features())

    assert enriched.max_pain_strike is None


# --- Market Tide enrichment tests ---


@pytest.mark.asyncio
async def test_enrich_market_tide_bullish(monkeypatch: pytest.MonkeyPatch) -> None:
    _SequencedClient.calls = 0
    _SequencedClient.responses = [
        _Response(200, {"data": {"net_premium": 5_000_000.0}}),
    ]
    monkeypatch.setattr("httpx.AsyncClient", _SequencedClient)

    extractor = _make_extractor()
    enriched = await extractor._enrich_market_tide(_base_features())

    assert enriched.market_tide_net_premium == 5_000_000.0
    assert enriched.market_tide_direction == "bullish"


@pytest.mark.asyncio
async def test_enrich_market_tide_bearish(monkeypatch: pytest.MonkeyPatch) -> None:
    _SequencedClient.calls = 0
    _SequencedClient.responses = [
        _Response(200, {"data": {"net_call_premium": -3_000_000.0}}),
    ]
    monkeypatch.setattr("httpx.AsyncClient", _SequencedClient)

    extractor = _make_extractor()
    enriched = await extractor._enrich_market_tide(_base_features())

    assert enriched.market_tide_net_premium == -3_000_000.0
    assert enriched.market_tide_direction == "bearish"


@pytest.mark.asyncio
async def test_enrich_market_tide_neutral(monkeypatch: pytest.MonkeyPatch) -> None:
    _SequencedClient.calls = 0
    _SequencedClient.responses = [
        _Response(200, {"data": {"net_premium": 0.0}}),
    ]
    monkeypatch.setattr("httpx.AsyncClient", _SequencedClient)

    extractor = _make_extractor()
    enriched = await extractor._enrich_market_tide(_base_features())

    assert enriched.market_tide_net_premium == 0.0
    assert enriched.market_tide_direction == "neutral"


@pytest.mark.asyncio
async def test_enrich_market_tide_handles_list_response(monkeypatch: pytest.MonkeyPatch) -> None:
    _SequencedClient.calls = 0
    _SequencedClient.responses = [
        _Response(200, {"data": [{"net_premium": 1_000_000.0}]}),
    ]
    monkeypatch.setattr("httpx.AsyncClient", _SequencedClient)

    extractor = _make_extractor()
    enriched = await extractor._enrich_market_tide(_base_features())

    assert enriched.market_tide_net_premium == 1_000_000.0
    assert enriched.market_tide_direction == "bullish"


@pytest.mark.asyncio
async def test_enrich_market_tide_no_gateway_url_skips() -> None:
    extractor = AlertFeatureExtractor(gateway_url=None)
    enriched = await extractor._enrich_market_tide(_base_features())

    assert enriched.market_tide_net_premium is None
    assert enriched.market_tide_direction is None


# --- AlertFeatures dataclass tests ---


def test_alert_features_new_fields_default_to_none() -> None:
    features = _base_features()
    assert features.gex is None
    assert features.vex is None
    assert features.max_pain_strike is None
    assert features.max_pain_distance_pct is None
    assert features.market_tide_net_premium is None
    assert features.market_tide_direction is None


def test_alert_features_to_dict_includes_new_fields() -> None:
    features = _base_features()
    features.gex = 500_000.0
    features.max_pain_strike = 190.0
    features.market_tide_net_premium = 1_000_000.0
    features.market_tide_direction = "bullish"

    d = features.to_dict()
    assert d["gex"] == 500_000.0
    assert d["max_pain_strike"] == 190.0
    assert d["market_tide_net_premium"] == 1_000_000.0
    assert d["market_tide_direction"] == "bullish"


def test_alert_features_from_dict_roundtrip() -> None:
    features = _base_features()
    features.gex = 500_000.0
    features.vex = -100_000.0
    features.max_pain_strike = 190.0
    features.max_pain_distance_pct = 0.025
    features.market_tide_net_premium = 1_000_000.0
    features.market_tide_direction = "bullish"

    d = features.to_dict()
    restored = AlertFeatures.from_dict(d)

    assert restored.gex == 500_000.0
    assert restored.vex == -100_000.0
    assert restored.max_pain_strike == 190.0
    assert restored.max_pain_distance_pct == pytest.approx(0.025)
    assert restored.market_tide_net_premium == 1_000_000.0
    assert restored.market_tide_direction == "bullish"
