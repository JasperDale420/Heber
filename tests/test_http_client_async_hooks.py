from __future__ import annotations

import httpx
import pytest

from heber.core.http_client import create_async_http_client


@pytest.mark.asyncio
async def test_async_http_client_default_event_hooks_are_awaitable() -> None:
    async def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code=200, json={"ok": True}, request=request)

    transport = httpx.MockTransport(_handler)
    async with create_async_http_client(timeout=2.0, transport=transport) as client:
        response = await client.get("https://example.test/health")

    assert response.status_code == 200
