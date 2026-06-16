from __future__ import annotations

import pytest

from heber.watch import writer as writer_module


def test_resolve_gateway_api_key_prefers_explicit_value() -> None:
    resolved = writer_module._resolve_gateway_api_key("  gw_explicit_key  ", "gw_settings_key")

    assert resolved == "gw_explicit_key"


def test_resolve_gateway_api_key_falls_back_to_settings_value() -> None:
    resolved = writer_module._resolve_gateway_api_key(None, "  gw_settings_key  ")

    assert resolved == "gw_settings_key"


def test_resolve_gateway_api_key_allows_missing_key_in_dev() -> None:
    resolved = writer_module._resolve_gateway_api_key(None, "   ")

    assert resolved == ""


def test_resolve_gateway_api_key_raises_when_missing_in_prod(monkeypatch: pytest.MonkeyPatch) -> None:
    from heber.config import get_settings

    get_settings.cache_clear()
    monkeypatch.setenv("HEBER_ENVIRONMENT", "prod")
    with pytest.raises(ValueError, match="HEBER_WATCH_GATEWAY_API_KEY"):
        writer_module._resolve_gateway_api_key(None, "   ")
    get_settings.cache_clear()
