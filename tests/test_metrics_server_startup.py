"""Tests for ``start_metrics_server`` failure handling.

The Prometheus metrics HTTP server is a non-critical sidecar — a port-bind
failure (e.g. the port is already in use because another Heber service is
running on the same host) must NOT take down the calling service. These
tests pin that contract.
"""

from __future__ import annotations

import errno

import pytest

from heber.ops import metrics as metrics_module


@pytest.mark.unit
def test_start_metrics_server_swallows_address_in_use(monkeypatch: pytest.MonkeyPatch) -> None:
    """A bound-port ``OSError`` must be logged and swallowed, not re-raised."""

    def _raise_addr_in_use(_port: int) -> None:
        raise OSError(errno.EADDRINUSE, "Address already in use")

    monkeypatch.setattr(metrics_module, "start_http_server", _raise_addr_in_use)

    # Must not raise.
    metrics_module.start_metrics_server(port=9090)


@pytest.mark.unit
def test_start_metrics_server_swallows_generic_oserror(monkeypatch: pytest.MonkeyPatch) -> None:
    """Any ``OSError`` from the bind call (permission denied, etc.) is swallowed."""

    def _raise_permission(_port: int) -> None:
        raise PermissionError("permission denied binding low port")

    monkeypatch.setattr(metrics_module, "start_http_server", _raise_permission)

    metrics_module.start_metrics_server(port=80)


@pytest.mark.unit
def test_start_metrics_server_propagates_unexpected_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-``OSError`` exceptions are unusual and must surface to the caller."""

    def _raise_unexpected(_port: int) -> None:
        raise RuntimeError("prometheus client internal bug")

    monkeypatch.setattr(metrics_module, "start_http_server", _raise_unexpected)

    with pytest.raises(RuntimeError, match="prometheus client internal bug"):
        metrics_module.start_metrics_server(port=9090)


@pytest.mark.unit
def test_start_metrics_server_success_logs_started(monkeypatch: pytest.MonkeyPatch) -> None:
    """Happy path still calls ``start_http_server`` with the requested port."""

    calls: list[int] = []

    def _record(port: int) -> None:
        calls.append(port)

    monkeypatch.setattr(metrics_module, "start_http_server", _record)

    metrics_module.start_metrics_server(port=9099)

    assert calls == [9099]
