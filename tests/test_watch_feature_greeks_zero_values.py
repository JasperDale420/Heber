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
