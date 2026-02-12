"""Dataflow health verification for Gateway -> Ingest -> Storage.

Provides JSON-only health reports for proving that data is flowing through
Heber and landing in storage.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import redis
import structlog

from heber.calendar import MarketCalendar
from heber.config import Settings, get_settings

logger = structlog.get_logger(__name__)

_PROM_METRIC_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?\s+(?P<value>[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)$"
)

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


def _fetch_metrics_samples(metrics_url: str) -> tuple[list[tuple[str, dict[str, str], float]] | None, str | None]:
    try:
        response = httpx.get(metrics_url, timeout=5.0)
        response.raise_for_status()
        return _parse_prometheus_text(response.text), None
    except Exception as exc:
        return None, str(exc)


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


def _latest_file_mtime(root: Path) -> float | None:
    if not root.exists():
        return None

    latest = root.stat().st_mtime if root.is_file() else None
    if root.is_file():
        return latest

    for item in root.rglob("*"):
        if not item.is_file():
            continue
        mtime = item.stat().st_mtime
        if latest is None or mtime > latest:
            latest = mtime
    return latest


def _collect_filesystem_feed_timestamps(settings: Settings) -> dict[str, float | None]:
    feed_paths = {
        "bars": settings.silver_path / "feed=bars",
        "trades": settings.silver_path / "feed=trades",
        "flow_alerts": settings.silver_path / "feed=flow_alerts",
    }
    return {feed: _latest_file_mtime(path) for feed, path in feed_paths.items()}


def _decode_redis_field(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _collect_redis_signals(settings: Settings) -> dict[str, Any]:
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
        groups: list[dict[Any, Any]] = []

        try:
            groups = client.xinfo_groups(settings.redis_stream_name)
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
            "error": None,
        }
    except Exception as exc:
        return {
            "ok": False,
            "group_exists": False,
            "lag": None,
            "pending": None,
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
) -> dict[str, Any]:
    consumer_samples, consumer_error = _fetch_metrics_samples(consumer_metrics_url)
    watch_samples, watch_error = _fetch_metrics_samples(watch_metrics_url)

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
        "filesystem_feeds": _collect_filesystem_feed_timestamps(settings),
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

    consumer_url = consumer_metrics_url or active_settings.health_consumer_metrics_url
    watch_url = watch_metrics_url or active_settings.health_watch_metrics_url
    signals = _collect_runtime_signals(
        consumer_metrics_url=consumer_url,
        watch_metrics_url=watch_url,
        settings=active_settings,
    )

    checks: list[dict[str, Any]] = []
    market_open = _is_market_open(now_utc)

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
    checks.append(
        {
            "id": "redis_consumer_group",
            "status": "ok" if group_exists else "fail",
            "severity": "critical",
            "observed": {"group_exists": group_exists},
            "threshold": {"required": True},
            "message": "Consumer group exists." if group_exists else "Consumer group is missing.",
        }
    )

    gateway_last_success = signals.get("gateway_last_success")
    if gateway_last_success is None:
        gateway_status = "warn"
        gateway_message = "No passive gateway success evidence from watch metrics."
        gateway_age = None
    else:
        gateway_age = max(0.0, now_unixtime - float(gateway_last_success))
        if market_open and gateway_age > float(window_seconds):
            gateway_status = "warn"
            gateway_message = "Gateway passive evidence is stale."
        else:
            gateway_status = "ok"
            gateway_message = "Gateway passive evidence is fresh."
    checks.append(
        {
            "id": "gateway_passive_activity",
            "status": gateway_status,
            "severity": "warning",
            "observed": {
                "last_success_unixtime": gateway_last_success,
                "age_seconds": gateway_age,
            },
            "threshold": {"max_age_seconds": window_seconds},
            "message": gateway_message,
        }
    )

    feed_signals = signals.get("feeds", {})
    fs_fallback = signals.get("filesystem_feeds", {})
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

    summary = {"ok": 0, "warn": 0, "fail": 0, "skipped": 0}
    for check in checks:
        status = check["status"]
        summary[status] += 1

    return {
        "ts_utc": now_utc.isoformat(),
        "mode": mode,
        "market_open": market_open,
        "overall_status": _overall_status(summary),
        "window_seconds": window_seconds,
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


if __name__ == "__main__":
    raise SystemExit(main())
