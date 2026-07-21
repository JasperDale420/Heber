"""Dataflow health verification for Gateway -> Ingest -> Storage.

Provides JSON-only health reports for proving that data is flowing through
Heber and landing in storage.

NOTE: Stream reachability, consumer group, feed freshness, and DLQ checks
are also performed by heber.health_monitor (with richer alerting and persistence).
This module remains as the /health endpoint for container orchestration probes.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import redis
import structlog

from heber.calendar import MarketCalendar
from heber.config import Settings, get_settings
from heber.core.http_client import create_http_client, raise_for_status
from heber.health_monitor.models import CheckResult, Severity, Status
from heber.ops.notifier import DiscordNotifier

logger = structlog.get_logger(__name__)

# Sentinel file created once on the real data volume. If the bind mount breaks
# or resolves to an empty placeholder directory, the sentinel disappears — the
# only reliable mount-liveness signal (the 2026-07-11 volume drop surfaced only
# as scattered per-service EPERM symptoms with no dedicated check).
_MOUNT_SENTINEL_NAME = ".heber-sentinel"

_PROM_METRIC_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?\s+(?P<value>[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)$"
)
_DT_PARTITION_RE = re.compile(r"^dt=(?P<date>\d{4}-\d{2}-\d{2})$")
_MAX_RECENT_FEED_PARTITIONS = 3

_FEED_SEVERITY: dict[str, str] = {
    "bars": "critical",
    "trades": "warning",
    "flow_alerts": "warning",
}


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _normalize_utc(ts: datetime | None) -> datetime:
    if ts is None:
        return _utc_now()
    if ts.tzinfo is None:
        return ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC)


def _is_market_open(ts_utc: datetime) -> bool:
    return MarketCalendar().is_market_open(ts_utc)


def _parse_prometheus_labels(raw_labels: str | None) -> dict[str, str]:
    if not raw_labels:
        return {}

    labels: dict[str, str] = {}
    parts = re.split(r""",(?![^\"]*\"(?:,|$))""", raw_labels)
    for part in parts:
        token = part.strip()
        if not token or "=" not in token:
            continue
        key, value = token.split("=", 1)
        labels[key.strip()] = value.strip().strip('"')
    return labels


def _parse_prometheus_text(payload: str) -> list[tuple[str, dict[str, str], float]]:
    samples: list[tuple[str, dict[str, str], float]] = []
    for raw_line in payload.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = _PROM_METRIC_RE.match(line)
        if match is None:
            continue

        try:
            value = float(match.group("value"))
        except ValueError:
            continue

        name = match.group("name")
        labels = _parse_prometheus_labels(match.group("labels"))
        samples.append((name, labels, value))
    return samples


def _fetch_metrics_samples(
    metrics_url: str, *, attempts: int = 2, retry_delay: float = 0.5
) -> tuple[list[tuple[str, dict[str, str], float]] | None, str | None]:
    """Fetch and parse Prometheus metrics, retrying once on a transient blip.

    The co-located metrics endpoint occasionally takes longer than the client
    timeout to respond when the consumer event loop is briefly busy (a large
    flush or GC pause), yielding a single ReadTimeout that recovers on the very
    next request. One short retry absorbs that transient without masking a
    genuinely-unreachable endpoint — both attempts must fail before it is logged
    and reported down. Mirrors _collect_redis_signals.

    The failure is logged without a traceback: the stack is the identical httpx
    timeout every time and adds only noise; the url + error string is the signal.
    """
    last_error: str | None = None
    for attempt in range(1, attempts + 1):
        try:
            with create_http_client(timeout=5.0) as client:
                response = client.get(metrics_url)
            raise_for_status(response)
            return _parse_prometheus_text(response.text), None
        except Exception as exc:  # noqa: BLE001 — any fetch failure degrades to filesystem fallback
            last_error = str(exc)
            if attempt < attempts:
                time.sleep(retry_delay)
    logger.warning("dataflow_metrics_fetch_failed", url=metrics_url, error=last_error, attempts=attempts)
    return None, last_error


def _metric_max(
    samples: list[tuple[str, dict[str, str], float]] | None,
    metric_name: str,
    labels: dict[str, str] | None = None,
) -> float | None:
    if samples is None:
        return None
    expected_labels = labels or {}
    values: list[float] = []
    for name, sample_labels, value in samples:
        if name != metric_name:
            continue
        if any(sample_labels.get(k) != v for k, v in expected_labels.items()):
            continue
        values.append(value)
    if not values:
        return None
    return max(values)


def _safe_stat_mtime(path: Path) -> float | None:
    """Return st_mtime for a file, or None on error."""
    try:
        return path.stat().st_mtime
    except FileNotFoundError:
        return None
    except OSError as exc:
        logger.warning(
            "dataflow_health_filesystem_stat_failed",
            path=str(path),
            error=str(exc),
        )
        return None


def _safe_iterdir(path: Path) -> list[Path]:
    try:
        return list(path.iterdir())
    except FileNotFoundError:
        return []
    except OSError as exc:
        logger.warning(
            "dataflow_health_filesystem_list_failed",
            path=str(path),
            error=str(exc),
        )
        return []


def _dt_partition_sort_key(path: Path) -> tuple[str, str]:
    match = _DT_PARTITION_RE.match(path.name)
    if match is None:
        return ("", str(path))
    return (match.group("date"), str(path))


def _recent_dt_partitions(root: Path, max_partitions: int = _MAX_RECENT_FEED_PARTITIONS) -> list[Path]:
    partitions: list[Path] = []

    for child in _safe_iterdir(root):
        if child.name.startswith(".") or not child.is_dir():
            continue
        if _DT_PARTITION_RE.match(child.name):
            partitions.append(child)
            continue
        for grandchild in _safe_iterdir(child):
            if grandchild.name.startswith(".") or not grandchild.is_dir():
                continue
            if _DT_PARTITION_RE.match(grandchild.name):
                partitions.append(grandchild)

    return sorted(partitions, key=_dt_partition_sort_key, reverse=True)[:max_partitions]


def _latest_file_mtime(root: Path) -> float | None:
    if not root.exists():
        return None

    if root.is_file():
        if root.name.startswith("."):
            return None
        return _safe_stat_mtime(root)

    latest: float | None = None

    for item in root.rglob("*"):
        if item.name.startswith(".") or not item.is_file():
            continue
        mtime = _safe_stat_mtime(item)
        if mtime is not None and (latest is None or mtime > latest):
            latest = mtime
    return latest


def _latest_partition_activity_mtime(partition: Path) -> float | None:
    latest = _safe_stat_mtime(partition)
    for child in _safe_iterdir(partition):
        if child.name.startswith("."):
            continue
        mtime = _safe_stat_mtime(child)
        if mtime is not None and (latest is None or mtime > latest):
            latest = mtime
    return latest


def _latest_feed_file_mtime(root: Path) -> float | None:
    if not root.exists():
        return None

    if root.is_file():
        return _latest_file_mtime(root)

    recent_partitions = _recent_dt_partitions(root)
    if not recent_partitions:
        return _latest_file_mtime(root)

    latest: float | None = None
    for partition in recent_partitions:
        mtime = _latest_partition_activity_mtime(partition)
        if mtime is not None and (latest is None or mtime > latest):
            latest = mtime
    return latest


def _collect_filesystem_feed_timestamps(settings: Settings) -> dict[str, float | None]:
    feed_paths = {
        "bars": settings.silver_path / "feed=bars",
        "trades": settings.silver_path / "feed=trades",
        "flow_alerts": settings.silver_path / "feed=flow_alerts",
    }
    return {feed: _latest_feed_file_mtime(path) for feed, path in feed_paths.items()}


def _decode_redis_field(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _collect_redis_signals(settings: Settings, *, attempts: int = 2, retry_delay: float = 0.5) -> dict[str, Any]:
    """Collect Redis stream signals, retrying once on failure.

    A single transient blip (e.g. a 2s connect timeout during a Redis restart)
    otherwise flips both redis_connection and redis_consumer_group to fail for a
    whole report cycle. One short retry absorbs the blip without masking a
    genuinely-down Redis.
    """
    result = _collect_redis_signals_once(settings)
    tries = 1
    while not result["ok"] and tries < attempts:
        time.sleep(retry_delay)
        result = _collect_redis_signals_once(settings)
        tries += 1
    return result


def _collect_redis_signals_once(settings: Settings) -> dict[str, Any]:
    client: redis.Redis | None = None
    try:
        client = redis.Redis.from_url(
            settings.redis_url,
            socket_connect_timeout=2,
            socket_timeout=2,
            decode_responses=False,
        )
        client.ping()

        group_exists = False
        lag: int | None = None
        pending: int | None = None
        stream_len: int | None = None
        groups: list[dict[Any, Any]] = []

        try:
            stream_len = int(client.xlen(settings.redis_stream_name))
        except redis.ResponseError:
            stream_len = None

        try:
            groups = cast(list[dict[Any, Any]], client.xinfo_groups(settings.redis_stream_name))
        except redis.ResponseError as exc:
            stream_missing = "no such key" in str(exc).lower()
            if not stream_missing:
                raise

        target_group = settings.redis_consumer_group
        for group in groups:
            name = _decode_redis_field(group.get("name") or group.get(b"name"))
            if name != target_group:
                continue
            group_exists = True
            lag_value = group.get("lag", group.get(b"lag"))
            pending_value = group.get("pending", group.get(b"pending"))
            lag = int(lag_value) if lag_value is not None else None
            pending = int(pending_value) if pending_value is not None else None
            break

        return {
            "ok": True,
            "group_exists": group_exists,
            "lag": lag,
            "pending": pending,
            "stream_len": stream_len,
            "error": None,
        }
    except Exception as exc:
        return {
            "ok": False,
            "group_exists": False,
            "lag": None,
            "pending": None,
            "stream_len": None,
            "error": str(exc),
        }
    finally:
        if client is not None:
            client.close()


def _collect_runtime_signals(
    *,
    consumer_metrics_url: str,
    watch_metrics_url: str,
    settings: Settings,
    collect_metrics: bool = True,
) -> dict[str, Any]:
    if collect_metrics:
        consumer_samples, consumer_error = _fetch_metrics_samples(consumer_metrics_url)
        watch_samples, watch_error = _fetch_metrics_samples(watch_metrics_url)
    else:
        consumer_samples = None
        watch_samples = None
        consumer_error = None
        watch_error = None

    feed_metric_sources = {
        "bars": _metric_max(
            consumer_samples,
            "heber_writer_last_write_unixtime",
            labels={"layer": "silver", "dataset": "bars"},
        ),
        "trades": _metric_max(
            consumer_samples,
            "heber_writer_last_write_unixtime",
            labels={"layer": "silver", "dataset": "trades"},
        ),
        "flow_alerts": _metric_max(
            consumer_samples,
            "heber_writer_last_write_unixtime",
            labels={"layer": "silver", "dataset": "flow_alerts"},
        ),
    }

    gateway_last_success = _metric_max(watch_samples, "heber_watch_gateway_last_success_unixtime")

    return {
        "redis": _collect_redis_signals(settings),
        "feeds": feed_metric_sources,
        "gateway_last_success": gateway_last_success,
        "metrics": {
            "consumer_url": consumer_metrics_url,
            "watch_url": watch_metrics_url,
            "consumer_error": consumer_error,
            "watch_error": watch_error,
        },
    }


def _freshness_check(
    *,
    feed: str,
    timestamp_unixtime: float | None,
    filesystem_unixtime: float | None,
    market_open: bool,
    window_seconds: int,
    now_unixtime: float,
) -> dict[str, Any]:
    severity = _FEED_SEVERITY[feed]
    check_id = f"feed_freshness_{feed}"

    if not market_open:
        return {
            "id": check_id,
            "status": "skipped",
            "severity": severity,
            "observed": {"source": "n/a", "age_seconds": None},
            "threshold": {"max_age_seconds": window_seconds},
            "message": "Market closed; freshness check skipped.",
        }

    source = "metrics"
    observed_ts = timestamp_unixtime
    if observed_ts is None and filesystem_unixtime is not None:
        source = "filesystem"
        observed_ts = filesystem_unixtime

    if observed_ts is None:
        status = "fail" if severity == "critical" else "warn"
        return {
            "id": check_id,
            "status": status,
            "severity": severity,
            "observed": {"source": "none", "age_seconds": None},
            "threshold": {"max_age_seconds": window_seconds},
            "message": f"No recent {feed} write evidence found.",
        }

    age_seconds = max(0.0, now_unixtime - observed_ts)
    stale = age_seconds > float(window_seconds)
    if stale:
        status = "fail" if severity == "critical" else "warn"
        message = f"{feed} evidence is stale."
    else:
        status = "ok"
        message = f"{feed} evidence is fresh."

    return {
        "id": check_id,
        "status": status,
        "severity": severity,
        "observed": {
            "source": source,
            "last_success_unixtime": observed_ts,
            "age_seconds": age_seconds,
        },
        "threshold": {"max_age_seconds": window_seconds},
        "message": message,
    }


def _overall_status(summary: dict[str, int]) -> str:
    if summary["fail"] > 0:
        return "fail"
    if summary["warn"] > 0:
        return "warn"
    return "ok"


def _build_gateway_check(
    gateway_last_success: float | None,
    now_unixtime: float,
    market_open: bool,
    window_seconds: int,
) -> dict[str, Any]:
    """Build the gateway passive activity health check dict."""
    if gateway_last_success is None:
        gateway_age = None
        if market_open:
            status, message = "warn", "No passive gateway success evidence from watch metrics."
        else:
            status, message = "skipped", "Market closed; passive gateway activity check skipped."
    else:
        gateway_age = max(0.0, now_unixtime - float(gateway_last_success))
        if market_open and gateway_age > float(window_seconds):
            status, message = "warn", "Gateway passive evidence is stale."
        else:
            status, message = "ok", "Gateway passive evidence is fresh."

    return {
        "id": "gateway_passive_activity",
        "status": status,
        "severity": "warning",
        "observed": {
            "last_success_unixtime": gateway_last_success,
            "age_seconds": gateway_age,
        },
        "threshold": {"max_age_seconds": window_seconds},
        "message": message,
    }


# Consumer-lag-vs-stream-length thresholds. When the group's lag is a large
# fraction of the (MAXLEN-capped) stream length, un-consumed entries are being
# evicted before the writer reads them — silent cross-feed data loss. This is the
# failure mode behind the 2026-06-09 option-quote flood (lag pinned at the 300K
# cap) that the reachability/freshness/DLQ checks all missed.
_CONSUMER_LAG_WARN_RATIO = 0.5
_CONSUMER_LAG_FAIL_RATIO = 0.8
_CONSUMER_LAG_MIN_FLOOR = 5_000

# A DLQ in the hundreds of thousands is a rejection flood, not steady-state
# trickle — escalate from warning to a critical page.
# DLQ health is measured from the durable on-disk audit files (the dedup-by-event-id
# record of truth), counted per day. The real failure rate is a handful/day, so a
# spike past these thresholds signals a genuine rejection flood — unlike the Redis
# DLQ xlen, which oscillates into the millions purely from cache-mode eviction.
_DLQ_DAILY_WARN_THRESHOLD = 100
_DLQ_DAILY_CRITICAL_THRESHOLD = 1_000


def _build_consumer_lag_check(redis_signal: dict[str, Any]) -> dict[str, Any]:
    """Alert when consumer lag approaches the stream length (MAXLEN cap)."""
    lag = redis_signal.get("lag")
    stream_len = redis_signal.get("stream_len")
    threshold = {
        "warn_ratio": _CONSUMER_LAG_WARN_RATIO,
        "fail_ratio": _CONSUMER_LAG_FAIL_RATIO,
        "min_lag_floor": _CONSUMER_LAG_MIN_FLOOR,
    }

    if not redis_signal.get("group_exists") or lag is None or not stream_len or stream_len <= 0:
        return {
            "id": "consumer_lag",
            "status": "skipped",
            "severity": "critical",
            "observed": {"lag": lag, "stream_len": stream_len, "lag_ratio": None},
            "threshold": threshold,
            "message": "Consumer lag not evaluable (group missing or stream length unknown).",
        }

    ratio = lag / stream_len
    if lag >= _CONSUMER_LAG_MIN_FLOOR and ratio >= _CONSUMER_LAG_FAIL_RATIO:
        status = "fail"
        message = (
            f"Consumer lag {lag} is {ratio:.0%} of stream length {stream_len} — "
            "un-consumed events are being evicted (MAXLEN cap)."
        )
    elif lag >= _CONSUMER_LAG_MIN_FLOOR and ratio >= _CONSUMER_LAG_WARN_RATIO:
        status = "warn"
        message = f"Consumer lag {lag} is {ratio:.0%} of stream length {stream_len} — approaching eviction."
    else:
        status = "ok"
        message = f"Consumer lag {lag} ({ratio:.0%} of stream length {stream_len})."

    return {
        "id": "consumer_lag",
        "status": status,
        "severity": "critical",
        "observed": {"lag": lag, "stream_len": stream_len, "lag_ratio": round(ratio, 3)},
        "threshold": threshold,
        "message": message,
    }


def _collect_dlq_size_check(active_settings: Settings) -> dict[str, Any]:
    """Report DLQ health from the durable audit record, not the Redis xlen.

    The Redis DLQ stream lives on a cache-mode Redis (LRU/TTL eviction) and is
    now bounded by an approximate MAXLEN, so its length reflects reprocessing-queue
    depth, not real failures (historically it oscillated into the millions purely
    from eviction churn). The dedup-by-event-id durable files are the audit record
    of truth; today's file count is the genuine failure signal.
    """
    from heber.writer.dlq_fallback import count_fallback_files_for_today

    threshold = {"warn": _DLQ_DAILY_WARN_THRESHOLD, "critical": _DLQ_DAILY_CRITICAL_THRESHOLD}
    try:
        daily_count = count_fallback_files_for_today(active_settings.dlq_fallback_path)
    except OSError as exc:
        return {
            "id": "dlq_queue_size",
            "status": "warn",
            "severity": "warning",
            "observed": {"durable_files_today": None},
            "threshold": threshold,
            "message": f"Could not count durable DLQ files: {exc}",
        }

    if daily_count > _DLQ_DAILY_CRITICAL_THRESHOLD:
        status, severity = "fail", "critical"
        message = (
            f"{daily_count} DLQ failures today (exceeds critical "
            f"{_DLQ_DAILY_CRITICAL_THRESHOLD}) — likely a rejection flood."
        )
    elif daily_count > _DLQ_DAILY_WARN_THRESHOLD:
        status, severity = "warn", "warning"
        message = f"{daily_count} DLQ failures today (exceeds {_DLQ_DAILY_WARN_THRESHOLD})."
    else:
        status, severity = "ok", "info"
        message = f"{daily_count} DLQ failures today."

    if status != "ok":
        logger.warning(
            "dlq_size_exceeded",
            durable_files_today=daily_count,
            warn_threshold=_DLQ_DAILY_WARN_THRESHOLD,
            critical_threshold=_DLQ_DAILY_CRITICAL_THRESHOLD,
        )

    return {
        "id": "dlq_queue_size",
        "status": status,
        "severity": severity,
        "observed": {"durable_files_today": daily_count},
        "threshold": threshold,
        "message": message,
    }


def _mount_liveness_check(settings: Settings) -> dict[str, Any]:
    sentinel = Path(settings.data_root) / _MOUNT_SENTINEL_NAME
    try:
        present = sentinel.exists()
        error = None
    except OSError as exc:  # zombie mount: stat itself can fail with EPERM
        present, error = False, str(exc)
    return {
        "id": "mount_liveness",
        "status": "ok" if present else "fail",
        "severity": "critical",
        "observed": {"sentinel": str(sentinel), "present": present, "error": error},
        "threshold": {"required": True},
        "message": (
            "Data volume sentinel present."
            if present
            else "Data volume missing or unmounted: sentinel not found."
        ),
    }


def _backup_freshness_check(settings: Settings) -> dict[str, Any]:
    """The lakehouse backup marker must be recent (twin marker written by
    scripts/backup_lakehouse.sh onto the data volume)."""
    marker = Path(settings.data_root) / "ops" / "backup-last-ok"
    max_age_s = settings.backup_freshness_hours * 3600
    try:
        age_s = time.time() - marker.stat().st_mtime
        fresh = age_s <= max_age_s
        observed: dict[str, Any] = {"marker": str(marker), "age_hours": round(age_s / 3600, 1)}
    except FileNotFoundError:
        fresh, observed = False, {"marker": str(marker), "age_hours": None}
    except OSError as exc:
        fresh, observed = False, {"marker": str(marker), "error": str(exc)}
    return {
        "id": "backup_freshness",
        "status": "ok" if fresh else "fail",
        "severity": "warning",
        "observed": observed,
        "threshold": {"max_age_hours": settings.backup_freshness_hours},
        "message": (
            "Lakehouse backup is fresh."
            if fresh
            else "Lakehouse backup marker missing or stale — the second copy is not current."
        ),
    }


def _catalog_health_check(settings: Settings) -> dict[str, Any]:
    url = settings.health_catalog_url
    try:
        with create_http_client(timeout=5.0) as client:
            resp = client.get(url)
        ok = resp.status_code == 200
        detail = None if ok else f"HTTP {resp.status_code}"
    except Exception as exc:  # noqa: BLE001 — any failure means the catalog is unreachable
        ok, detail = False, str(exc)[:200]
    return {
        "id": "catalog_health",
        "status": "ok" if ok else "fail",
        "severity": "critical",
        "observed": {"url": url, "error": detail},
        "threshold": {"required": True},
        "message": "Catalog healthy (DB exercised)." if ok else f"Catalog unhealthy: {detail}",
    }


def generate_dataflow_report(
    *,
    window_seconds: int,
    mode: str,
    now: datetime | None = None,
    consumer_metrics_url: str | None = None,
    watch_metrics_url: str | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    active_settings = settings or get_settings()
    now_utc = _normalize_utc(now)
    now_unixtime = now_utc.timestamp()
    market_open = _is_market_open(now_utc)

    consumer_url = consumer_metrics_url or active_settings.health_consumer_metrics_url
    watch_url = watch_metrics_url or active_settings.health_watch_metrics_url
    signals = _collect_runtime_signals(
        consumer_metrics_url=consumer_url,
        watch_metrics_url=watch_url,
        settings=active_settings,
        collect_metrics=market_open,
    )

    checks: list[dict[str, Any]] = []

    feed_signals = signals.get("feeds", {})
    fs_fallback = signals.get("filesystem_feeds")
    if fs_fallback is None:
        fs_fallback = {"bars": None, "trades": None, "flow_alerts": None}
        if market_open and any(feed_signals.get(feed) is None for feed in ("bars", "trades", "flow_alerts")):
            fs_fallback = _collect_filesystem_feed_timestamps(active_settings)

    redis_signal = signals.get("redis", {})
    redis_ok = bool(redis_signal.get("ok"))
    checks.append(
        {
            "id": "redis_connection",
            "status": "ok" if redis_ok else "fail",
            "severity": "critical",
            "observed": {
                "ok": redis_ok,
                "lag": redis_signal.get("lag"),
                "pending": redis_signal.get("pending"),
            },
            "threshold": {"required": True},
            "message": "Redis stream reachable." if redis_ok else f"Redis unavailable: {redis_signal.get('error')}",
        }
    )

    group_exists = bool(redis_signal.get("group_exists"))
    if not redis_ok:
        # A failed/timed-out connection cannot observe the group. Reporting
        # "missing" here fabricates a phantom critical every time the 2s probe
        # times out (docker-proxy jitter) — the group is not actually gone
        # (Redis uptime and the group's monotonic entries-read confirm it).
        # redis_connection already carries the unreachable signal, so skip this
        # check instead of doubling the alert.
        group_status, group_message = "skipped", "Consumer group check skipped: Redis unreachable."
    elif group_exists:
        group_status, group_message = "ok", "Consumer group exists."
    else:
        group_status, group_message = "fail", "Consumer group is missing."
    checks.append(
        {
            "id": "redis_consumer_group",
            "status": group_status,
            "severity": "critical",
            "observed": {"group_exists": group_exists, "redis_ok": redis_ok},
            "threshold": {"required": True},
            "message": group_message,
        }
    )

    checks.append(_build_consumer_lag_check(redis_signal))

    checks.append(
        _build_gateway_check(
            signals.get("gateway_last_success"),
            now_unixtime,
            market_open,
            window_seconds,
        )
    )

    for feed in ("bars", "trades", "flow_alerts"):
        checks.append(
            _freshness_check(
                feed=feed,
                timestamp_unixtime=feed_signals.get(feed),
                filesystem_unixtime=fs_fallback.get(feed),
                market_open=market_open,
                window_seconds=window_seconds,
                now_unixtime=now_unixtime,
            )
        )

    # DLQ size check
    dlq_check = _collect_dlq_size_check(active_settings)
    checks.append(dlq_check)

    # Run in ALL modes, never behind the market_open gate: the Jul 10-13 incident
    # (dead postgres + dropped volume) went unreported for the whole weekend
    # because every existing check was either skipped when the market was closed
    # or never looked at the catalog/volume at all.
    checks.append(_mount_liveness_check(active_settings))
    checks.append(_catalog_health_check(active_settings))
    checks.append(_backup_freshness_check(active_settings))

    summary = {"ok": 0, "warn": 0, "fail": 0, "skipped": 0}
    for check in checks:
        summary[check["status"]] += 1

    # Info-only deployment provenance (no alarm): what deploy.sh last baked.
    deployed_sha = None
    try:
        sha_file = Path(active_settings.data_root) / "ops" / "deployed_sha"
        deployed_sha = sha_file.read_text().split()[0][:12]
    except (OSError, IndexError):
        pass

    return {
        "ts_utc": now_utc.isoformat(),
        "mode": mode,
        "market_open": market_open,
        "overall_status": _overall_status(summary),
        "window_seconds": window_seconds,
        "deployed_sha": deployed_sha,
        "checks": checks,
        "summary": summary,
    }


def _write_report(report: dict[str, Any], report_dir: Path) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)
    ts_compact = report["ts_utc"].replace(":", "").replace("+00:00", "Z")
    dated_path = report_dir / f"report-{ts_compact}.json"
    latest_path = report_dir / "latest.json"
    payload = json.dumps(report, separators=(",", ":"))
    dated_path.write_text(payload + "\n", encoding="utf-8")
    latest_path.write_text(payload + "\n", encoding="utf-8")


_notifier: DiscordNotifier | None = None


def _dispatch_alerts(report: dict[str, Any], settings: Settings | None = None) -> None:
    """Send failing checks to Discord; debounce/cooldown/recovery live in the notifier."""
    global _notifier
    try:
        if _notifier is None:
            _notifier = DiscordNotifier(settings or get_settings())
        now = datetime.now(UTC)
        results = [
            CheckResult(
                check_name=f"dataflow:{check['id']}",
                feed=None,
                severity=(
                    Severity.P0_CRITICAL
                    if check.get("severity") == "critical"
                    else Severity.P1_WARNING
                ),
                status=Status.FAIL if check["status"] == "fail" else Status.PASS,
                message=f"dataflow {check['id']}: {check.get('message', '')}",
                details=check.get("observed") or {},
                ts_checked=now,
            )
            for check in report.get("checks", [])
            if check["status"] in ("fail", "ok")
        ]
        if results:
            _notifier.dispatch(results)
    except Exception:  # noqa: BLE001 — alerting must never break the health loop
        logger.warning("dataflow_alert_dispatch_failed", exc_info=True)


def _ping_heartbeat(report: dict[str, Any], settings: Settings | None = None) -> None:
    """Off-machine dead-man heartbeat: ping after every cycle; append /fail when
    the report is failing. If pings stop entirely (machine dead, mount gone, loop
    wedged) the external service alerts — the only signal that survives host death."""
    url = (settings or get_settings()).heartbeat_url.strip()
    if not url:
        return
    target = url if report.get("overall_status") != "fail" else f"{url}/fail"
    try:
        with create_http_client(timeout=10.0) as client:
            client.get(target)
    except Exception:  # noqa: BLE001 — heartbeat must never break the health loop
        logger.warning("heartbeat_ping_failed", url=target)


def run_dataflow_health_once(
    *,
    window_seconds: int,
    mode: str,
    consumer_metrics_url: str | None = None,
    watch_metrics_url: str | None = None,
    report_dir: str | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    report = generate_dataflow_report(
        window_seconds=window_seconds,
        mode=mode,
        consumer_metrics_url=consumer_metrics_url,
        watch_metrics_url=watch_metrics_url,
        settings=settings,
    )
    _dispatch_alerts(report, settings)
    _ping_heartbeat(report, settings)
    if report_dir:
        try:
            _write_report(report, Path(report_dir))
        except OSError as exc:
            logger.warning(
                "dataflow_health_report_write_failed",
                report_dir=report_dir,
                error=str(exc),
            )
    logger.debug(
        "dataflow_health_report_generated",
        overall_status=report["overall_status"],
        mode=mode,
    )
    return report


def run_dataflow_health_loop(
    *,
    window_seconds: int,
    mode: str,
    interval_seconds: int,
    consumer_metrics_url: str | None = None,
    watch_metrics_url: str | None = None,
    report_dir: str | None = None,
    settings: Settings | None = None,
) -> None:
    while True:
        report = run_dataflow_health_once(
            window_seconds=window_seconds,
            mode=mode,
            consumer_metrics_url=consumer_metrics_url,
            watch_metrics_url=watch_metrics_url,
            report_dir=report_dir,
            settings=settings,
        )
        print(json.dumps(report, separators=(",", ":")))
        time.sleep(max(0, interval_seconds))


def _build_parser() -> argparse.ArgumentParser:
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Heber dataflow health check")
    parser.add_argument("--window-seconds", type=int, default=settings.health_freshness_seconds)
    parser.add_argument("--consumer-metrics-url", default=settings.health_consumer_metrics_url)
    parser.add_argument("--watch-metrics-url", default=settings.health_watch_metrics_url)
    parser.add_argument("--report-dir", default=str(settings.health_report_dir))
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval-seconds", type=int, default=settings.health_interval_seconds)
    parser.add_argument("--mode", choices=["manual", "scheduled"], default="manual")
    return parser


def main() -> int:
    args = _build_parser().parse_args()

    # Configure logging based on mode
    # For manual runs (CLI), we might want human-readable logs, but adherence to PRD says JSON.
    # However, this tool prints JSON report to stdout, so logs should go to stderr or be suppressed.
    # The logging module handles this via structlog configuration.
    from heber.config import get_settings
    from heber.ops.logging import configure_logging

    s = get_settings()
    # Ensure logs don't interfere with JSON report on stdout if running manually
    configure_logging(service_name="heber-dataflow-health", log_level=s.log_level, json_output=True)

    try:
        if args.loop:
            run_dataflow_health_loop(
                window_seconds=args.window_seconds,
                mode=args.mode,
                interval_seconds=args.interval_seconds,
                consumer_metrics_url=args.consumer_metrics_url,
                watch_metrics_url=args.watch_metrics_url,
                report_dir=args.report_dir,
            )
            return 0

        report = run_dataflow_health_once(
            window_seconds=args.window_seconds,
            mode=args.mode,
            consumer_metrics_url=args.consumer_metrics_url,
            watch_metrics_url=args.watch_metrics_url,
            report_dir=args.report_dir,
        )
        print(json.dumps(report, separators=(",", ":")))
        return 0
    except KeyboardInterrupt:
        return 0
    except Exception as e:
        print(f"Error: {e}", file=__import__("sys").stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
