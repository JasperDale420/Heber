"""Tests for credential-safe gateway auth diagnostics surfaced on 401."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pytest

from heber.watch import features as features_module
from heber.watch.features import AlertFeatureExtractor, AlertFeatures
from heber.watch.gateway import (
    GATEWAY_API_KEY_ENV_VARS,
    GATEWAY_AUTH_HEADER_NAME,
    describe_gateway_auth_env,
)


class _Response:
    def __init__(self, status_code: int, payload: dict[str, Any]):
        self.status_code = status_code
        self._payload = payload
        self.headers: dict[str, str] = {}

    def json(self) -> dict[str, Any]:
        return self._payload


class _AlwaysUnauthorizedClient:
    def __init__(self, *args, **kwargs):  # noqa: ANN002, ANN003
        pass

    async def __aenter__(self) -> _AlwaysUnauthorizedClient:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False

    async def get(
        self,
        url: str,  # noqa: ARG002
        params: dict[str, Any] | None = None,  # noqa: ARG002
        headers: dict[str, str] | None = None,  # noqa: ARG002
    ) -> _Response:
        return _Response(401, {"error": "unauthorized"})


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


class TestDescribeGatewayAuthEnv:
    """The diagnostic helper must never leak credential values."""

    def test_reports_header_name_used(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for var in GATEWAY_API_KEY_ENV_VARS:
            monkeypatch.delenv(var, raising=False)
        env = describe_gateway_auth_env()
        assert env["auth_header_names"] == [GATEWAY_AUTH_HEADER_NAME]
        assert env["any_env_var_set"] is False
        assert env["env_var_used"] is None
        assert env["key_length"] is None

    def test_reports_which_env_var_supplied_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for var in GATEWAY_API_KEY_ENV_VARS:
            monkeypatch.delenv(var, raising=False)
        monkeypatch.setenv("DATA_GATEWAY_API_KEY", "the-actual-secret-value")
        env = describe_gateway_auth_env()
        assert env["any_env_var_set"] is True
        assert env["env_var_used"] == "DATA_GATEWAY_API_KEY"
        assert env["key_length"] == len("the-actual-secret-value")
        # The actual value MUST NOT appear anywhere in the diagnostics.
        serialized = repr(env)
        assert "the-actual-secret-value" not in serialized

    def test_first_set_env_var_wins(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for var in GATEWAY_API_KEY_ENV_VARS:
            monkeypatch.delenv(var, raising=False)
        # HEBER_WATCH_GATEWAY_API_KEY is first in precedence
        monkeypatch.setenv("HEBER_WATCH_GATEWAY_API_KEY", "primary")
        monkeypatch.setenv("DATA_GATEWAY_API_KEY", "secondary")
        env = describe_gateway_auth_env()
        assert env["env_var_used"] == "HEBER_WATCH_GATEWAY_API_KEY"

    def test_blank_env_var_treated_as_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for var in GATEWAY_API_KEY_ENV_VARS:
            monkeypatch.delenv(var, raising=False)
        monkeypatch.setenv("HEBER_WATCH_GATEWAY_API_KEY", "   ")
        env = describe_gateway_auth_env()
        assert env["any_env_var_set"] is False
        status_for_var = {entry["name"]: entry["set"] for entry in env["env_vars_checked"]}
        assert status_for_var["HEBER_WATCH_GATEWAY_API_KEY"] is False


@pytest.mark.asyncio
async def test_401_logs_include_auth_diagnostics(monkeypatch: pytest.MonkeyPatch) -> None:
    """On 401, the warning log must include header names sent + env state."""
    # Pretend the operator deployed without setting any gateway env var.
    for var in GATEWAY_API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr("httpx.AsyncClient", _AlwaysUnauthorizedClient)

    warning_calls: list[tuple[str, dict[str, Any]]] = []

    def _capture_warning(message: str, **kwargs: Any) -> None:
        warning_calls.append((message, kwargs))

    monkeypatch.setattr(features_module.logger, "warning", _capture_warning)

    extractor = AlertFeatureExtractor(
        gateway_url="http://gateway:8000",
        gateway_api_key="actual-key-value-do-not-log",  # pragma: allowlist secret
        request_max_attempts=1,
        auth_failure_threshold=10,  # high enough to NOT raise on first 401
        auth_failure_window_seconds=300,
    )

    await extractor._enrich_iv_rank(_base_features())

    auth_logs = [entry for entry in warning_calls if entry[1].get("status_code") == 401]
    assert auth_logs, "Expected at least one 401 warning"
    fields = auth_logs[0][1]
    assert fields["auth_header_sent"] is True
    assert fields["auth_header_names_sent"] == [GATEWAY_AUTH_HEADER_NAME]
    # Env diagnostics surfaced.
    assert "auth_env" in fields
    auth_env = fields["auth_env"]
    assert auth_env["any_env_var_set"] is False
    assert auth_env["auth_header_names"] == [GATEWAY_AUTH_HEADER_NAME]
    # CRITICAL: no credential values leak into the log payload.
    serialized = repr(fields)
    assert "actual-key-value-do-not-log" not in serialized


def test_module_constants_match_runtime_header() -> None:
    """The diagnostic constants must match the header the code actually sends."""
    from heber.watch.gateway import gateway_auth_headers

    headers = gateway_auth_headers("some-key")
    assert list(headers.keys()) == [GATEWAY_AUTH_HEADER_NAME]
