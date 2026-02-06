"""Regression tests for tracing behavior when OpenTelemetry is unavailable."""

from __future__ import annotations

from heber.ops import tracing


def test_traced_decorator_no_otel_does_not_require_spankind(monkeypatch) -> None:
    monkeypatch.setattr(tracing, "OTEL_AVAILABLE", False)
    monkeypatch.delattr(tracing, "SpanKind", raising=False)

    @tracing.traced(span_name="unit-test-no-otel")
    def _sample(value: int) -> int:
        return value + 1

    assert _sample(41) == 42
