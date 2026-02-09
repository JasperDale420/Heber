from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from heber.watch.features import AlertFeatureExtractor, AlertFeatures


def _base_features() -> AlertFeatures:
    return AlertFeatures(
        alert_id="a1",
        alert_time=datetime(2026, 2, 7, 15, 0, tzinfo=UTC),
        symbol="AAPL",
        occ_symbol="AAPL260220C00100000",
        underlying="AAPL",
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
async def test_enrich_greeks_preserves_zero_values(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Response:
        status_code = 200

        @staticmethod
        def json() -> dict:
            return {
                "data": {
                    "contracts": [
                        {
                            "strike_price": 100.0,
                            "delta": 0.0,
                            "gamma": 0.0,
                            "theta": 0.0,
                            "vega": 0.0,
                            "implied_volatility": 0.0,
                        }
                    ]
                }
            }

    class _Client:
        async def __aenter__(self):  # noqa: ANN204
            return self

        async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
            return False

        async def get(self, route: str, params: dict | None = None) -> _Response:  # noqa: ARG002
            return _Response()

    monkeypatch.setattr("httpx.AsyncClient", lambda *args, **kwargs: _Client())  # noqa: ARG005

    extractor = AlertFeatureExtractor(gateway_url="http://gateway:8000")
    enriched = await extractor._enrich_greeks(_base_features())

    assert enriched.delta == 0.0
    assert enriched.gamma == 0.0
    assert enriched.theta == 0.0
    assert enriched.vega == 0.0
    assert enriched.iv == 0.0


@pytest.mark.asyncio
async def test_market_context_does_not_skip_zero_close_day(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Response:
        status_code = 200

        @staticmethod
        def json() -> dict:
            return {
                "data": {
                    "bars": [
                        {"t": "2026-02-07T00:00:00Z", "c": 100.0},
                        {"t": "2026-02-06T00:00:00Z", "c": 0.0},
                        {"t": "2026-02-05T00:00:00Z", "c": 50.0},
                    ]
                }
            }

    class _Client:
        async def __aenter__(self):  # noqa: ANN204
            return self

        async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
            return False

        async def get(self, route: str, params: dict | None = None) -> _Response:  # noqa: ARG002
            return _Response()

    monkeypatch.setattr("httpx.AsyncClient", lambda *args, **kwargs: _Client())  # noqa: ARG005

    extractor = AlertFeatureExtractor(gateway_url="http://gateway:8000")
    enriched = await extractor._enrich_market_context(_base_features())

    assert enriched.underlying_1d_return is None


@pytest.mark.asyncio
async def test_enrich_greeks_skips_malformed_contract_strike_and_uses_valid_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Response:
        status_code = 200

        @staticmethod
        def json() -> dict:
            return {
                "data": {
                    "contracts": [
                        {
                            "strike_price": "bad-strike",
                            "delta": 0.99,
                            "gamma": 0.99,
                            "theta": 0.99,
                            "vega": 0.99,
                            "implied_volatility": 0.99,
                        },
                        {
                            "strike_price": 100.0,
                            "delta": 0.11,
                            "gamma": 0.22,
                            "theta": -0.03,
                            "vega": 0.44,
                            "implied_volatility": 0.55,
                        },
                    ]
                }
            }

    class _Client:
        async def __aenter__(self):  # noqa: ANN204
            return self

        async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
            return False

        async def get(self, route: str, params: dict | None = None) -> _Response:  # noqa: ARG002
            return _Response()

    monkeypatch.setattr("httpx.AsyncClient", lambda *args, **kwargs: _Client())  # noqa: ARG005

    extractor = AlertFeatureExtractor(gateway_url="http://gateway:8000")
    enriched = await extractor._enrich_greeks(_base_features())

    assert enriched.delta == 0.11
    assert enriched.gamma == 0.22
    assert enriched.theta == -0.03
    assert enriched.vega == 0.44
    assert enriched.iv == 0.55


@pytest.mark.asyncio
async def test_enrich_greeks_treats_non_finite_values_as_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Response:
        status_code = 200

        @staticmethod
        def json() -> dict:
            return {
                "data": {
                    "contracts": [
                        {
                            "strike_price": 100.0,
                            "delta": "NaN",
                            "gamma": "inf",
                            "theta": "-inf",
                            "vega": 0.44,
                            "implied_volatility": "NaN",
                        },
                    ]
                }
            }

    class _Client:
        async def __aenter__(self):  # noqa: ANN204
            return self

        async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
            return False

        async def get(self, route: str, params: dict | None = None) -> _Response:  # noqa: ARG002
            return _Response()

    monkeypatch.setattr("httpx.AsyncClient", lambda *args, **kwargs: _Client())  # noqa: ARG005

    extractor = AlertFeatureExtractor(gateway_url="http://gateway:8000")
    enriched = await extractor._enrich_greeks(_base_features())

    assert enriched.delta is None
    assert enriched.gamma is None
    assert enriched.theta is None
    assert enriched.vega == 0.44
    assert enriched.iv is None


@pytest.mark.asyncio
async def test_enrich_greeks_treats_boolean_values_as_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Response:
        status_code = 200

        @staticmethod
        def json() -> dict:
            return {
                "data": {
                    "contracts": [
                        {
                            "strike_price": 100.0,
                            "delta": True,
                            "gamma": False,
                            "theta": True,
                            "vega": 0.44,
                            "implied_volatility": False,
                        },
                    ]
                }
            }

    class _Client:
        async def __aenter__(self):  # noqa: ANN204
            return self

        async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
            return False

        async def get(self, route: str, params: dict | None = None) -> _Response:  # noqa: ARG002
            return _Response()

    monkeypatch.setattr("httpx.AsyncClient", lambda *args, **kwargs: _Client())  # noqa: ARG005

    extractor = AlertFeatureExtractor(gateway_url="http://gateway:8000")
    enriched = await extractor._enrich_greeks(_base_features())

    assert enriched.delta is None
    assert enriched.gamma is None
    assert enriched.theta is None
    assert enriched.vega == 0.44
    assert enriched.iv is None


@pytest.mark.asyncio
async def test_enrich_iv_rank_treats_non_finite_values_as_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Response:
        status_code = 200

        @staticmethod
        def json() -> dict:
            return {"data": {"iv_rank": "NaN"}}

    class _Client:
        async def __aenter__(self):  # noqa: ANN204
            return self

        async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
            return False

        async def get(self, route: str, params: dict | None = None) -> _Response:  # noqa: ARG002
            return _Response()

    monkeypatch.setattr("httpx.AsyncClient", lambda *args, **kwargs: _Client())  # noqa: ARG005

    extractor = AlertFeatureExtractor(gateway_url="http://gateway:8000")
    enriched = await extractor._enrich_iv_rank(_base_features())

    assert enriched.iv_rank is None


@pytest.mark.asyncio
async def test_market_context_treats_non_finite_close_as_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Response:
        status_code = 200

        @staticmethod
        def json() -> dict:
            return {
                "data": {
                    "bars": [
                        {"t": "2026-02-07T00:00:00Z", "c": "NaN"},
                        {"t": "2026-02-06T00:00:00Z", "c": 100.0},
                        {"t": "2026-02-05T00:00:00Z", "c": 99.0},
                    ]
                }
            }

    class _Client:
        async def __aenter__(self):  # noqa: ANN204
            return self

        async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
            return False

        async def get(self, route: str, params: dict | None = None) -> _Response:  # noqa: ARG002
            return _Response()

    monkeypatch.setattr("httpx.AsyncClient", lambda *args, **kwargs: _Client())  # noqa: ARG005

    extractor = AlertFeatureExtractor(gateway_url="http://gateway:8000")
    enriched = await extractor._enrich_market_context(_base_features())

    assert enriched.underlying_1d_return is None
    assert enriched.underlying_5d_return is None
    assert enriched.underlying_30d_return is None
