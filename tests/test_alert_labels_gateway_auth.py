from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import pandas as pd
import pytest

from heber.features.pipelines.alert_labels import AlertLabelsPipeline


class _StubResponse:
    def __init__(self, status_code: int, payload: dict[str, Any]) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> dict[str, Any]:
        return self._payload


class _CapturingAsyncClient:
    calls: list[dict[str, Any]] = []

    def __init__(self, **kwargs: Any) -> None:
        pass

    async def __aenter__(self) -> _CapturingAsyncClient:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None

    async def get(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
    ) -> _StubResponse:
        self.calls.append({"url": url, "headers": headers or {}, "params": params or {}})
        return _StubResponse(
            200,
            {
                "data": {
                    "bars": [
                        {
                            "symbol": "SPY260320C00700000",
                            "timestamp": "2026-02-13T14:30:00Z",
                            "close": 1.25,
                        }
                    ]
                }
            },
        )


def test_alert_labels_pipeline_requires_gateway_key_for_contract_labels() -> None:
    pipeline = AlertLabelsPipeline(gateway_url="http://gateway:8080", gateway_api_key=None)

    with pytest.raises(ValueError, match="HEBER_WATCH_GATEWAY_API_KEY"):
        pipeline._require_gateway_api_key()


def test_fetch_option_bars_sends_gateway_auth_header(monkeypatch: pytest.MonkeyPatch) -> None:
    _CapturingAsyncClient.calls = []
    monkeypatch.setattr("httpx.AsyncClient", _CapturingAsyncClient)

    pipeline = AlertLabelsPipeline(gateway_url="http://gateway:8080", gateway_api_key="gw_test")

    bars = asyncio.run(
        pipeline._fetch_option_bars(
            ["SPY260320C00700000"],
            datetime(2026, 2, 13, tzinfo=UTC),
            datetime(2026, 2, 14, tzinfo=UTC),
        )
    )

    assert not bars.empty
    assert isinstance(bars, pd.DataFrame)
    assert _CapturingAsyncClient.calls[0]["headers"] == {"X-Gateway-Key": "gw_test"}
