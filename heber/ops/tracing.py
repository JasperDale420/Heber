"""Distributed tracing for Heber per PRD §12.5.6.

Provides:
- OpenTelemetry (OTLP) integration
- Trace context propagation from Gateway lineage.trace_id
- Key spans for consumer, writer, and catalog services
- Head-based sampling: 1% prod, 100% dev
"""

from collections.abc import Callable
from contextlib import contextmanager
from functools import wraps
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


# Check if OpenTelemetry is available
try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.trace.sampling import (
        ParentBased,
        TraceIdRatioBased,
    )
    from opentelemetry.trace import SpanKind, Status, StatusCode
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False
    logger.warning("opentelemetry not installed, tracing disabled")


# Environment-based sampling rates (PRD §12.5.6)
SAMPLING_RATES = {
    "prod": 0.01,  # 1% in production
    "production": 0.01,
    "staging": 1.0,  # 100% in staging
    "dev": 1.0,  # 100% in development
    "development": 1.0,
    "test": 1.0,
}


def get_sampling_rate() -> float:
    """Get sampling rate based on environment."""
    from heber.config import get_settings

    env = get_settings().environment
    return SAMPLING_RATES.get(env, 0.01)


def configure_tracing(
    service_name: str = "heber",
    endpoint: str | None = None,
    sampling_rate: float | None = None,
) -> None:
    """Configure OpenTelemetry tracing per PRD §12.5.6.

    Args:
        service_name: Service name for spans (e.g., "heber-consumer")
        endpoint: OTLP collector endpoint (default: from env OTEL_EXPORTER_OTLP_ENDPOINT)
        sampling_rate: Override sampling rate (default: based on environment)
    """
    if not OTEL_AVAILABLE:
        logger.warning("tracing_disabled", reason="opentelemetry not installed")
        return

    from heber.config import get_settings

    settings = get_settings()

    # Get endpoint from parameter or settings
    endpoint = endpoint or settings.otel_endpoint

    # Get sampling rate
    rate = sampling_rate if sampling_rate is not None else get_sampling_rate()

    # Create sampler
    sampler = ParentBased(root=TraceIdRatioBased(rate))

    # Create resource with service info
    resource = Resource.create(
        {
            "service.name": service_name,
            "service.version": settings.service_version,
            "deployment.environment": settings.environment,
        }
    )

    # Create tracer provider
    provider = TracerProvider(resource=resource, sampler=sampler)

    # Add OTLP exporter
    try:
        exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
        processor = BatchSpanProcessor(exporter)
        provider.add_span_processor(processor)

        # Set as global provider
        trace.set_tracer_provider(provider)

        logger.info(
            "tracing_configured",
            service=service_name,
            endpoint=endpoint,
            sampling_rate=rate,
        )
    except Exception as e:
        logger.error("tracing_setup_failed", error=str(e))


def get_tracer(name: str = "heber") -> Any:
    """Get a tracer instance."""
    if not OTEL_AVAILABLE:
        return _NoopTracer()
    return trace.get_tracer(name)


class _NoopTracer:
    """No-op tracer when OpenTelemetry is not available."""

    @contextmanager
    def start_as_current_span(self, name: str, **kwargs):
        yield _NoopSpan()

    def start_span(self, name: str, **kwargs):
        return _NoopSpan()


class _NoopSpan:
    """No-op span."""

    def set_attribute(self, key: str, value: Any) -> None:
        pass

    def set_status(self, status: Any) -> None:
        pass

    def record_exception(self, exception: Exception) -> None:
        pass

    def end(self) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


def extract_trace_context(lineage: dict | None) -> Any:
    """Extract trace context from EventEnvelope lineage (PRD §12.5.6).

    Args:
        lineage: lineage dict from EventEnvelope containing trace_id

    Returns:
        OpenTelemetry Context or None
    """
    if not OTEL_AVAILABLE or not lineage:
        return None

    trace_id = lineage.get("trace_id")
    if not trace_id:
        return None

    # Reconstruct W3C traceparent format
    # Format: {version}-{trace_id}-{span_id}-{flags}
    # We use trace_id as both trace_id and generate a new span_id
    propagator = TraceContextTextMapPropagator()
    carrier = {"traceparent": f"00-{trace_id}-0000000000000001-01"}

    return propagator.extract(carrier)


# Span decorators for key operations (PRD §12.5.6)


def traced(
    span_name: str | None = None,
    kind: str = "internal",
    extract_lineage: bool = False,
):
    """Decorator to trace a function.

    Args:
        span_name: Span name (default: function name)
        kind: Span kind (internal, server, client, producer, consumer)
        extract_lineage: If True, extract trace context from 'lineage' kwarg
    """

    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(*args, **kwargs):
            tracer = get_tracer()
            name = span_name or fn.__name__

            # Extract context from lineage if requested
            context = None
            if extract_lineage and "lineage" in kwargs:
                context = extract_trace_context(kwargs["lineage"])

            span_kind = None
            if OTEL_AVAILABLE:
                kind_map = {
                    "internal": SpanKind.INTERNAL,
                    "server": SpanKind.SERVER,
                    "client": SpanKind.CLIENT,
                    "producer": SpanKind.PRODUCER,
                    "consumer": SpanKind.CONSUMER,
                }
                span_kind = kind_map.get(kind, SpanKind.INTERNAL)

            with tracer.start_as_current_span(name, kind=span_kind, context=context) as span:
                try:
                    result = fn(*args, **kwargs)
                    if OTEL_AVAILABLE:
                        span.set_status(Status(StatusCode.OK))
                    return result
                except Exception as e:
                    if OTEL_AVAILABLE:
                        span.record_exception(e)
                        span.set_status(Status(StatusCode.ERROR, str(e)))
                    raise

        return wrapper

    return decorator


# Pre-defined span helpers for PRD §12.5.6 key spans


@contextmanager
def span_process_batch(feed: str, batch_size: int, lineage: dict | None = None):
    """Span for consumer batch processing."""
    tracer = get_tracer()
    context = extract_trace_context(lineage)

    with tracer.start_as_current_span(
        "process_batch",
        kind=SpanKind.CONSUMER if OTEL_AVAILABLE else None,
        context=context,
    ) as span:
        span.set_attribute("feed", feed)
        span.set_attribute("batch_size", batch_size)
        yield span


@contextmanager
def span_dedupe_check(bloom_size: int, drops: int = 0):
    """Span for deduplication check."""
    tracer = get_tracer()

    with tracer.start_as_current_span("dedupe_check") as span:
        span.set_attribute("bloom_size", bloom_size)
        span.set_attribute("drops", drops)
        yield span


@contextmanager
def span_write_bronze(rows: int, bytes_written: int):
    """Span for Bronze layer write."""
    tracer = get_tracer()

    with tracer.start_as_current_span("write_bronze") as span:
        span.set_attribute("rows", rows)
        span.set_attribute("bytes", bytes_written)
        yield span


@contextmanager
def span_write_silver(rows: int, bytes_written: int, partition: str):
    """Span for Silver layer write."""
    tracer = get_tracer()

    with tracer.start_as_current_span("write_silver") as span:
        span.set_attribute("rows", rows)
        span.set_attribute("bytes", bytes_written)
        span.set_attribute("partition", partition)
        yield span


@contextmanager
def span_api_request(endpoint: str, status: int | None = None):
    """Span for Catalog API request."""
    tracer = get_tracer()

    with tracer.start_as_current_span(
        "api_request",
        kind=SpanKind.SERVER if OTEL_AVAILABLE else None,
    ) as span:
        span.set_attribute("endpoint", endpoint)
        yield span
        if status:
            span.set_attribute("status", status)


def inject_trace_context(carrier: dict) -> dict:
    """Inject current trace context into a carrier dict for propagation.

    Useful for outgoing HTTP requests or message publishing.
    """
    if not OTEL_AVAILABLE:
        return carrier

    propagator = TraceContextTextMapPropagator()
    propagator.inject(carrier)
    return carrier


def get_current_trace_id() -> str | None:
    """Get the current trace ID as a hex string."""
    if not OTEL_AVAILABLE:
        return None

    span = trace.get_current_span()
    if span:
        ctx = span.get_span_context()
        if ctx.is_valid:
            return format(ctx.trace_id, "032x")
    return None
