"""Prometheus metrics for Heber per PRD §12.5.1-12.5.3.

Provides all required metrics for consumer, writer, compactor, catalog,
and anti-leakage latency monitoring.

Naming convention: heber_<service>_<metric_name>{<labels>}
"""

from prometheus_client import Counter, Gauge, Histogram, Info, start_http_server
import structlog

logger = structlog.get_logger(__name__)


# Default metrics port (PRD §12.5.1)
METRICS_PORT = 9100


# Service info
heber_info = Info("heber", "Heber Data Lakehouse service info")


# =============================================================================
# Consumer Metrics (PRD §12.5.2)
# =============================================================================

consumer_events_received_total = Counter(
    "heber_consumer_events_received_total",
    "Total events received from event bus",
    ["feed", "provider"],
)

consumer_events_processed_total = Counter(
    "heber_consumer_events_processed_total",
    "Total events processed",
    ["feed", "provider", "status"],  # status: success, error, dropped
)

consumer_batch_size = Histogram(
    "heber_consumer_batch_size",
    "Batch sizes for processing",
    ["feed"],
    buckets=[10, 50, 100, 250, 500, 1000, 2500, 5000, 10000],
)

consumer_lag_seconds = Gauge(
    "heber_consumer_lag_seconds",
    "Consumer lag behind stream head in seconds",
    ["stream"],
)

consumer_dedupe_drops_total = Counter(
    "heber_consumer_dedupe_drops_total",
    "Events dropped by deduplication (Bloom filter)",
    ["feed"],
)


# =============================================================================
# Writer Metrics (PRD §12.5.2)
# =============================================================================

writer_rows_written_total = Counter(
    "heber_writer_rows_written_total",
    "Total rows written",
    ["layer", "dataset"],
)

writer_bytes_written_total = Counter(
    "heber_writer_bytes_written_total",
    "Total bytes written",
    ["layer", "dataset"],
)

writer_files_written_total = Counter(
    "heber_writer_files_written_total",
    "Total files created",
    ["layer", "dataset"],
)

writer_flush_duration_seconds = Histogram(
    "heber_writer_flush_duration_seconds",
    "Time to flush a batch to storage",
    ["layer"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

writer_errors_total = Counter(
    "heber_writer_errors_total",
    "Write failures by type",
    ["layer", "error_type"],
)


# =============================================================================
# Compactor Metrics (PRD §12.5.2)
# =============================================================================

compactor_runs_total = Counter(
    "heber_compactor_runs_total",
    "Total compaction runs",
    ["dataset", "status"],  # status: success, error
)

compactor_files_merged_total = Counter(
    "heber_compactor_files_merged_total",
    "Total files merged during compaction",
    ["dataset"],
)

compactor_bytes_reclaimed_total = Counter(
    "heber_compactor_bytes_reclaimed_total",
    "Bytes reclaimed (space saved) by compaction",
    ["dataset"],
)

compactor_duration_seconds = Histogram(
    "heber_compactor_duration_seconds",
    "Compaction duration",
    ["dataset"],
    buckets=[1, 5, 10, 30, 60, 120, 300, 600],
)


# =============================================================================
# Catalog Metrics (PRD §12.5.2)
# =============================================================================

catalog_requests_total = Counter(
    "heber_catalog_requests_total",
    "Total API requests",
    ["endpoint", "status_code"],
)

catalog_request_duration_seconds = Histogram(
    "heber_catalog_request_duration_seconds",
    "API request latency",
    ["endpoint"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
)

catalog_db_connections_active = Gauge(
    "heber_catalog_db_connections_active",
    "Active database connections",
)


# =============================================================================
# Hot Store Metrics (PRD §12.5.2)
# =============================================================================

hotstore_rows_synced_total = Counter(
    "heber_hotstore_rows_synced_total",
    "Rows synced to Hot Store",
    ["dataset"],
)

hotstore_lag_seconds = Gauge(
    "heber_hotstore_lag_seconds",
    "Sync lag behind Silver in seconds",
    ["dataset"],
)

hotstore_sync_errors_total = Counter(
    "heber_hotstore_sync_errors_total",
    "Sync failures",
    ["dataset", "error_type"],
)


# =============================================================================
# Anti-Leakage Latency Metrics (PRD §12.5.3)
# =============================================================================

ingest_lag_seconds = Histogram(
    "heber_ingest_lag_seconds",
    "Lag from ts_event to ts_ingest (ts_ingest - ts_event)",
    ["feed", "provider"],
    buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0],
)

availability_lag_seconds = Histogram(
    "heber_availability_lag_seconds",
    "Lag from ts_event to ts_available (ts_available - ts_event)",
    ["feed", "provider"],
    buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0],
)

commit_lag_seconds = Histogram(
    "heber_commit_lag_seconds",
    "Lag from ts_ingest to file commit (ts_commit - ts_ingest)",
    ["feed"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)


# =============================================================================
# DLQ Metrics
# =============================================================================

dlq_events_total = Counter(
    "heber_dlq_events_total",
    "Events sent to dead-letter queue",
    ["feed", "error_type"],
)

dlq_size = Gauge(
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
    consumer_events_processed_total.labels(
        feed=feed, provider=provider, status=status
    ).inc()


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


def record_write_error(layer: str, error_type: str) -> None:
    """Record a write error."""
    writer_errors_total.labels(layer=layer, error_type=error_type).inc()


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
    catalog_requests_total.labels(endpoint=endpoint, status_code=str(status_code)).inc()
    catalog_request_duration_seconds.labels(endpoint=endpoint).observe(duration)


def record_dlq_event(feed: str, error_type: str) -> None:
    """Record an event sent to DLQ."""
    dlq_events_total.labels(feed=feed, error_type=error_type).inc()


def set_consumer_lag(stream: str, lag_seconds: float) -> None:
    """Set consumer lag gauge."""
    consumer_lag_seconds.labels(stream=stream).set(lag_seconds)


def set_hotstore_lag(dataset: str, lag_seconds: float) -> None:
    """Set Hot Store sync lag."""
    hotstore_lag_seconds.labels(dataset=dataset).set(lag_seconds)


# =============================================================================
# Server Setup
# =============================================================================

def start_metrics_server(port: int = METRICS_PORT) -> None:
    """Start the Prometheus metrics HTTP server.
    
    Args:
        port: Port to serve metrics on (default: 9100)
    """
    try:
        start_http_server(port)
        logger.info("metrics_server_started", port=port)
    except Exception as e:
        logger.error("metrics_server_failed", port=port, error=str(e))
        raise


def set_service_info(
    version: str,
    service: str,
    instance_id: str,
) -> None:
    """Set service info label for all metrics."""
    heber_info.info({
        "version": version,
        "service": service,
        "instance_id": instance_id,
    })
