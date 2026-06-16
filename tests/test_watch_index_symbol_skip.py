"""Tests for index symbol skipping in market context enrichment."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from heber.watch.features import INDEX_SYMBOLS, AlertFeatureExtractor, AlertFeatures


def _base_features(symbol: str = "AAPL") -> AlertFeatures:
    return AlertFeatures(
        alert_id="test-alert-001",
        alert_time=datetime(2026, 3, 9, 15, 0, tzinfo=UTC),
        symbol=symbol,
        occ_symbol=None,
        underlying=symbol,
        strike=150.0,
        expiry=date(2026, 3, 21),
        put_call="C",
        days_to_expiry=12,
        premium=50000.0,
        volume=100,
        open_interest=500,
        volume_oi_ratio=0.2,
        alert_type="SWEEP",
        side="ask",
        aggressor="buyer",
        spot_price=148.0,
        contract_price=3.50,
    )


class _Response:
    def __init__(self, status_code: int, payload: dict, headers: dict | None = None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def json(self):
        return self._payload


class _TrackingClient:
    """HTTP client that tracks whether any requests were made."""

    calls: list[str] = []

    def __init__(self, *args, **kwargs):  # noqa: ANN002, ANN003
        pass

    async def __aenter__(self):
        self.calls = []
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def get(
        self,
        url: str,
        params: dict | None = None,  # noqa: ARG002
        headers: dict[str, str] | None = None,  # noqa: ARG002
    ):
        self.calls.append(url)
        return _Response(200, {"data": {"bars": []}})


def _make_extractor() -> AlertFeatureExtractor:
    return AlertFeatureExtractor(
        gateway_url="http://localhost:8080",
        legacy_route_fallback_enabled=False,
        request_max_attempts=1,
        retry_base_delay_seconds=0,
        retry_jitter_seconds=0,
    )


@pytest.mark.parametrize("symbol", sorted(INDEX_SYMBOLS))
@pytest.mark.asyncio
async def test_enrich_market_context_skips_index_symbols(
    symbol: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Index symbols like SPX should skip the Alpaca stock bars call entirely."""
    client = _TrackingClient()
    from heber.watch import features as features_module

    monkeypatch.setattr(features_module, "create_async_http_client", lambda **kw: client)

    extractor = _make_extractor()
    features = _base_features(symbol)
    result = await extractor._enrich_market_context(features)

    # No HTTP call should have been made
    assert client.calls == [], f"Expected no HTTP calls for index symbol {symbol}, got {client.calls}"

    # Market context fields should remain None
    assert result.underlying_1d_return is None
    assert result.underlying_5d_return is None
    assert result.underlying_30d_return is None
    assert result.realized_vol_20d is None


@pytest.mark.asyncio
async def test_enrich_market_context_calls_alpaca_for_stock_symbols(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Normal stock symbols (AAPL, SPY) should still call Alpaca stock bars."""
    client = _TrackingClient()
    from heber.watch import features as features_module

    monkeypatch.setattr(features_module, "create_async_http_client", lambda **kw: client)

    extractor = _make_extractor()
    features = _base_features("AAPL")
    await extractor._enrich_market_context(features)

    # Should have made at least one HTTP call
    assert len(client.calls) > 0, "Expected HTTP call for stock symbol AAPL"
    assert any("AAPL" in url for url in client.calls)


def test_index_symbols_constant_is_frozen() -> None:
    """INDEX_SYMBOLS should be a frozenset to prevent accidental mutation."""
    assert isinstance(INDEX_SYMBOLS, frozenset)
    with pytest.raises(AttributeError):
        INDEX_SYMBOLS.add("TEST")  # type: ignore[attr-defined]
