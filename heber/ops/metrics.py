"""Prometheus metrics for Heber per PRD §12.5.1-12.5.3.

Provides all required metrics for consumer, writer, compactor, catalog,
and anti-leakage latency monitoring.

Naming convention: heber_<service>_<metric_name>{<labels>}
"""

import time

import structlog
from prometheus_client import REGISTRY, Counter, Gauge, Histogram, Info, start_http_server

logger = structlog.get_logger(__name__)


def _get_or_create(metric_cls, name, *args, **kwargs):
    """Return an existing metric or create a new one.

    Prevents ``ValueError: Duplicated timeseries`` when the module is
    re-imported (e.g. across test files that transitively import ops.metrics).
    """
    try:
        return metric_cls(name, *args, **kwargs)
    except ValueError:
        for collector in list(REGISTRY._collectors):
            if hasattr(collector, "_name") and collector._name == name:
                return collector
        raise


# Default metrics port (PRD §12.5.1)
METRICS_PORT = 9100


# Service info
heber_info = _get_or_create(Info, "heber", "Heber Data Lakehouse service info")


# =============================================================================
# Consumer Metrics (PRD §12.5.2)
# =============================================================================

consumer_events_received_total = _get_or_create(
    Counter,
    "heber_consumer_events_received_total",
    "Total events received from event bus",
    ["feed", "provider"],
)

consumer_events_processed_total = _get_or_create(
    Counter,
    "heber_consumer_events_processed_total",
    "Total events processed",
    ["feed", "provider", "status"],  # status: success, error, dropped
)

consumer_batch_size = _get_or_create(
    Histogram,
    "heber_consumer_batch_size",
    "Batch sizes for processing",
    ["feed"],
    buckets=[10, 50, 100, 250, 500, 1000, 2500, 5000, 10000],
)

consumer_lag_seconds = _get_or_create(
    Gauge,
    "heber_consumer_lag_seconds",
    "Consumer lag behind stream head in seconds",
    ["stream"],
)

consumer_dedupe_drops_total = _get_or_create(
    Counter,
    "heber_consumer_dedupe_drops_total",
    "Events dropped by deduplication (Bloom filter)",
    ["feed"],
)


# =============================================================================
# Writer Metrics (PRD §12.5.2)
# =============================================================================

writer_rows_written_total = _get_or_create(
    Counter,
    "heber_writer_rows_written_total",
    "Total rows written",
    ["layer", "dataset"],
)

writer_bytes_written_total = _get_or_create(
    Counter,
    "heber_writer_bytes_written_total",
    "Total bytes written",
    ["layer", "dataset"],
)

writer_files_written_total = _get_or_create(
    Counter,
    "heber_writer_files_written_total",
    "Total files created",
    ["layer", "dataset"],
)

writer_flush_duration_seconds = _get_or_create(
    Histogram,
    "heber_writer_flush_duration_seconds",
    "Time to flush a batch to storage",
    ["layer"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

writer_errors_total = _get_or_create(
    Counter,
    "heber_writer_errors_total",
    "Write failures by type",
    ["layer", "error_type"],
)

writer_last_write_unixtime = _get_or_create(
    Gauge,
    "heber_writer_last_write_unixtime",
    "Unix timestamp of most recent successful write",
    ["layer", "dataset"],
)


# =============================================================================
# Watch Metrics
# =============================================================================

watch_gateway_requests_total = _get_or_create(
    Counter,
    "heber_watch_gateway_requests_total",
    "Gateway request outcomes from watch-service components",
    ["component", "endpoint", "outcome", "status_class"],
)

watch_gateway_request_duration_seconds = _get_or_create(
    Histogram,
    "heber_watch_gateway_request_duration_seconds",
    "Gateway request latency from watch-service components",
    ["component", "endpoint", "outcome"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

watch_gateway_last_success_unixtime = _get_or_create(
    Gauge,
    "heber_watch_gateway_last_success_unixtime",
    "Unix timestamp of most recent successful gateway response by component/endpoint",
    ["component", "endpoint"],
)

watch_watches_created_total = _get_or_create(
    Counter,
    "heber_watch_watches_created_total",
    "Total watches created by watch consumer",
)

watch_last_watch_created_unixtime = _get_or_create(
    Gauge,
    "heber_watch_last_watch_created_unixtime",
    "Unix timestamp of most recent watch creation",
)

watch_poll_cycles_total = _get_or_create(
    Counter,
    "heber_watch_poll_cycles_total",
    "Total poll cycles by status",
    ["status"],
)

watch_last_poll_unixtime = _get_or_create(
    Gauge,
    "heber_watch_last_poll_unixtime",
    "Unix timestamp of most recent poll cycle completion",
)

watch_alert_parse_total = _get_or_create(
    Counter,
    "heber_watch_alert_parse_total",
    "Alert parse outcomes in watch consumer",
    ["status"],
)


# =============================================================================
# Compactor Metrics (PRD §12.5.2)
# =============================================================================

compactor_runs_total = _get_or_create(
    Counter,
    "heber_compactor_runs_total",
    "Total compaction runs",
    ["dataset", "status"],  # status: success, error
)

compactor_files_merged_total = _get_or_create(
    Counter,
    "heber_compactor_files_merged_total",
    "Total files merged during compaction",
    ["dataset"],
)

compactor_bytes_reclaimed_total = _get_or_create(
    Counter,
    "heber_compactor_bytes_reclaimed_total",
    "Bytes reclaimed (space saved) by compaction",
    ["dataset"],
)

compactor_duration_seconds = _get_or_create(
    Histogram,
    "heber_compactor_duration_seconds",
    "Compaction duration",
    ["dataset"],
    buckets=[1, 5, 10, 30, 60, 120, 300, 600],
)

compactor_dedupe_drops_total = _get_or_create(
    Counter,
    "heber_compactor_dedupe_drops_total",
    "Events dropped by deduplication during compaction",
    ["dataset"],
)


# =============================================================================
# Catalog Metrics (PRD §12.5.2)
# =============================================================================

catalog_requests_total = _get_or_create(
    Counter,
    "heber_catalog_requests_total",
    "Total API requests",
    ["endpoint", "status_class"],
)

catalog_request_duration_seconds = _get_or_create(
    Histogram,
    "heber_catalog_request_duration_seconds",
    "API request latency",
    ["endpoint"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
)

catalog_db_connections_active = _get_or_create(
    Gauge,
    "heber_catalog_db_connections_active",
    "Active database connections",
)


# =============================================================================
# Anti-Leakage Latency Metrics (PRD §12.5.3)
# =============================================================================

ingest_lag_seconds = _get_or_create(
    Histogram,
    "heber_ingest_lag_seconds",
    "Lag from ts_event to ts_ingest (ts_ingest - ts_event)",
    ["feed", "provider"],
    buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0],
)

availability_lag_seconds = _get_or_create(
    Histogram,
    "heber_availability_lag_seconds",
    "Lag from ts_event to ts_available (ts_available - ts_event)",
    ["feed", "provider"],
    buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0],
)

commit_lag_seconds = _get_or_create(
    Histogram,
    "heber_commit_lag_seconds",
    "Lag from ts_ingest to file commit (ts_commit - ts_ingest)",
    ["feed"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)


# =============================================================================
# DLQ Metrics
# =============================================================================

dlq_events_total = _get_or_create(
    Counter,
    "heber_dlq_events_total",
    "Events sent to dead-letter queue",
    ["feed", "error_type"],
)

dlq_size = _get_or_create(
    Gauge,
    "heber_dlq_size",
    "Current dead-letter queue size",
    ["feed"],
)


# =============================================================================
# Metric Recording Helpers
# =============================================================================


def record_event_received(feed: str, provider: str) -> None:
    """Record an event received from the bus."""
    consumer_events_received_total.labels(feed=feed, provider=provider).inc()


def record_event_processed(
    feed: str,
    provider: str,
    status: str = "success",
) -> None:
    """Record an event processed (success/error/dropped)."""
    consumer_events_processed_total.labels(feed=feed, provider=provider, status=status).inc()


def record_batch_processed(feed: str, batch_size: int) -> None:
    """Record a batch processing."""
    consumer_batch_size.labels(feed=feed).observe(batch_size)


def record_dedupe_drop(feed: str) -> None:
    """Record a deduplication drop."""
    consumer_dedupe_drops_total.labels(feed=feed).inc()


def record_write(
    layer: str,
    dataset: str,
    rows: int,
    bytes_written: int,
    duration_seconds: float,
) -> None:
    """Record a file write."""
    writer_rows_written_total.labels(layer=layer, dataset=dataset).inc(rows)
    writer_bytes_written_total.labels(layer=layer, dataset=dataset).inc(bytes_written)
    writer_files_written_total.labels(layer=layer, dataset=dataset).inc()
    writer_flush_duration_seconds.labels(layer=layer).observe(duration_seconds)
    writer_last_write_unixtime.labels(layer=layer, dataset=dataset).set(time.time())


def record_write_error(layer: str, error_type: str) -> None:
    """Record a write error."""
    writer_errors_total.labels(layer=layer, error_type=error_type).inc()


def record_watch_gateway_success(
    component: str,
    endpoint: str,
    timestamp_unixtime: float | None = None,
) -> None:
    """Record last successful watch gateway response timestamp."""
    watch_gateway_last_success_unixtime.labels(component=component, endpoint=endpoint).set(
        timestamp_unixtime if timestamp_unixtime is not None else time.time()
    )


def _status_class(status_code: int | None) -> str:
    """Bucket an HTTP status code into a low-cardinality label."""
    if status_code is None:
        return "error"
    if 200 <= status_code < 300:
        return "2xx"
    if 300 <= status_code < 400:
        return "3xx"
    if 400 <= status_code < 500:
        return "4xx"
    if 500 <= status_code < 600:
        return "5xx"
    return f"{status_code // 100}xx"


def record_watch_gateway_request(
    component: str,
    endpoint: str,
    outcome: str,
    status_code: int | None,
    duration_seconds: float,
) -> None:
    """Record a watch-service gateway request outcome and latency."""
    watch_gateway_requests_total.labels(
        component=component,
        endpoint=endpoint,
        outcome=outcome,
        status_class=_status_class(status_code),
    ).inc()
    watch_gateway_request_duration_seconds.labels(
        component=component,
        endpoint=endpoint,
        outcome=outcome,
    ).observe(max(0.0, duration_seconds))
    if outcome == "success":
        record_watch_gateway_success(component=component, endpoint=endpoint)


def record_watch_watch_created(timestamp_unixtime: float | None = None) -> None:
    """Record watch creation activity."""
    watch_watches_created_total.inc()
    watch_last_watch_created_unixtime.set(timestamp_unixtime if timestamp_unixtime is not None else time.time())


def record_watch_poll_cycle(status: str, timestamp_unixtime: float | None = None) -> None:
    """Record watch poll cycle status and last poll timestamp."""
    watch_poll_cycles_total.labels(status=status).inc()
    watch_last_poll_unixtime.set(timestamp_unixtime if timestamp_unixtime is not None else time.time())


def record_watch_alert_parse(status: str, count: int = 1) -> None:
    """Record watch alert parse outcomes."""
    if count <= 0:
        return
    watch_alert_parse_total.labels(status=status).inc(count)


def record_ingest_latency(
    feed: str,
    provider: str,
    ingest_lag: float,
    availability_lag: float,
) -> None:
    """Record latency metrics for anti-leakage monitoring."""
    ingest_lag_seconds.labels(feed=feed, provider=provider).observe(ingest_lag)
    availability_lag_seconds.labels(feed=feed, provider=provider).observe(availability_lag)


def record_compaction(
    dataset: str,
    status: str,
    files_merged: int,
    bytes_reclaimed: int,
    duration: float,
) -> None:
    """Record compaction run."""
    compactor_runs_total.labels(dataset=dataset, status=status).inc()
    if status == "success":
        compactor_files_merged_total.labels(dataset=dataset).inc(files_merged)
        compactor_bytes_reclaimed_total.labels(dataset=dataset).inc(bytes_reclaimed)
    compactor_duration_seconds.labels(dataset=dataset).observe(duration)


def record_api_request(endpoint: str, status_code: int, duration: float) -> None:
    """Record a catalog API request."""
    catalog_requests_total.labels(endpoint=endpoint, status_class=_status_class(status_code)).inc()
    catalog_request_duration_seconds.labels(endpoint=endpoint).observe(duration)


def record_dlq_event(feed: str, error_type: str) -> None:
    """Record an event sent to DLQ."""
    dlq_events_total.labels(feed=feed, error_type=error_type).inc()


def set_consumer_lag(stream: str, lag_seconds: float) -> None:
    """Set consumer lag gauge."""
    consumer_lag_seconds.labels(stream=stream).set(lag_seconds)


# =============================================================================
# Server Setup
# =============================================================================


def start_metrics_server(port: int = METRICS_PORT) -> None:
    """Start the Prometheus metrics HTTP server.

    Port-bind failures (``OSError``, e.g. ``EADDRINUSE``) are logged as a
    WARNING and swallowed so the caller's service can continue running
    without metrics. Other exception classes are unexpected and re-raised.

    Args:
        port: Port to serve metrics on (default: 9100)
    """
    try:
        start_http_server(port)
        logger.info("metrics_server_started", port=port)
    except OSError as e:
        logger.warning(
            "metrics_server_bind_failed",
            port=port,
            error=str(e),
            errno=getattr(e, "errno", None),
        )
    except Exception as e:
        logger.error("metrics_server_failed", port=port, error=str(e), exc_info=True)
        raise


def start_metrics_server_from_env(
    default_port: int | None = None,
) -> int | None:
    """Start metrics server from settings or environment configuration.

    Returns:
        The bound port when started, otherwise None.
    """
    from heber.config import get_settings

    settings_port = get_settings().metrics_port
    if settings_port is not None:
        start_metrics_server(settings_port)
        return settings_port
    elif default_port is not None:
        start_metrics_server(default_port)
        return default_port
    else:
        logger.debug("metrics_server_skipped", reason="port_not_configured")
        return None


def set_service_info(
    version: str,
    service: str,
    instance_id: str,
) -> None:
    """Set service info label for all metrics."""
    heber_info.info(
        {
            "version": version,
            "service": service,
            "instance_id": instance_id,
        }
    )
