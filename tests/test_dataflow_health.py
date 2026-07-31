from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

from heber.config import Settings
from heber.ops import dataflow_health as dataflow_health_module


def test_dataflow_alerts_use_an_isolated_notifier_state_file(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        data_root=tmp_path,
        alert_discord_enabled=True,
        alert_discord_webhook_url="https://discord.invalid/test",
    )
    factory = MagicMock()

    with (
        patch.object(dataflow_health_module, "DiscordNotifier", factory),
        patch.object(dataflow_health_module, "_notifier", None),
    ):
        dataflow_health_module._dispatch_alerts({"checks": []}, settings)

    factory.assert_called_once_with(
        settings,
        state_path=tmp_path / "ops" / "alerts" / "dataflow-health-state.json",
    )


def _signals(now: datetime) -> dict:
    fresh = now.timestamp() - 60
    return {
        "redis": {
            "ok": True,
            "group_exists": True,
            "lag": 0,
            "pending": 0,
            "stream_len": 100_000,
            "error": None,
        },
        "feeds": {
            "bars": fresh,
            "trades": fresh,
            "flow_alerts": fresh,
        },
        "gateway_last_success": fresh,
    }


def _ok_dlq_check(*_args, **_kwargs) -> dict:
    return {
        "id": "dlq_queue_size",
        "status": "ok",
        "severity": "warning",
        "observed": {"dlq_stream": "heber:dlq", "length": 0},
        "threshold": {"max_length": 10_000},
        "message": "DLQ has 0 entries.",
    }


def _ok_backup_check(*_args, **_kwargs) -> dict:
    return {
        "id": "backup_freshness",
        "status": "ok",
        "severity": "warning",
        "observed": {"marker": "/tmp/backup-last-ok", "age_hours": 1.0},
        "threshold": {"max_age_hours": 26},
        "message": "Lakehouse backup is fresh.",
    }


def test_after_hours_collects_metrics_when_backfill_or_watch_uses_jetstream(monkeypatch, tmp_path) -> None:
    now = datetime(2026, 3, 28, 12, tzinfo=UTC)  # Saturday
    captured: list[bool] = []

    def collect(**kwargs):
        captured.append(kwargs["collect_metrics"])
        signals = _signals(now)
        signals.update(
            {
                "jetstream": {},
                "jetstream_backfill": {},
                "jetstream_watch": {},
                "metrics": {},
            }
        )
        return signals

    monkeypatch.setattr(dataflow_health_module, "_collect_runtime_signals", collect)
    settings = Settings(
        _env_file=None,
        data_root=tmp_path,
        ingest_transport="redis",
        health_backfill_ingest_transport="jetstream",
    )

    dataflow_health_module.generate_dataflow_report(now=now, window_seconds=900, mode="manual", settings=settings)

    assert captured == [True]


def test_selected_jetstream_lanes_get_labeled_checks_and_redis_lanes_skip(monkeypatch, tmp_path) -> None:
    now = datetime(2026, 3, 25, 16, tzinfo=UTC)
    fresh_state = {"bound": 1, "pending": 0, "ack_pending": 0, "redelivered": 0, "heartbeat": now.timestamp()}
    signals = _signals(now)
    signals.update(
        {"jetstream": fresh_state, "jetstream_backfill": fresh_state, "jetstream_watch": fresh_state, "metrics": {}}
    )
    monkeypatch.setattr(dataflow_health_module, "_collect_runtime_signals", lambda **_kwargs: signals)
    settings = Settings(
        _env_file=None,
        data_root=tmp_path,
        ingest_transport="jetstream",
        health_backfill_ingest_transport="jetstream",
        watch_ingest_transport="jetstream",
        nats_username="heber",
        nats_password="test-only-password",  # pragma: allowlist secret
    )

    report = dataflow_health_module.generate_dataflow_report(
        now=now, window_seconds=900, mode="manual", settings=settings
    )
    ids = {check["id"] for check in report["checks"]}

    assert {"jetstream_consumer", "jetstream_backfill_consumer", "jetstream_watch_consumer"} <= ids
    assert "redis_consumer_group" in ids  # control-plane skip is explicit


def test_jetstream_redis_control_plane_outage_reports_warning_without_crashing(monkeypatch, tmp_path) -> None:
    now = datetime(2026, 3, 25, 16, tzinfo=UTC)
    fresh_state = {"bound": 1, "pending": 0, "ack_pending": 0, "redelivered": 0, "heartbeat": now.timestamp()}
    signals = _signals(now)
    signals["redis"] = {
        "ok": False,
        "group_exists": False,
        "lag": None,
        "pending": None,
        "stream_len": None,
        "error": "connection refused",
    }
    signals.update(
        {"jetstream": fresh_state, "jetstream_backfill": fresh_state, "jetstream_watch": fresh_state, "metrics": {}}
    )
    monkeypatch.setattr(dataflow_health_module, "_collect_runtime_signals", lambda **_kwargs: signals)
    settings = Settings(
        _env_file=None,
        data_root=tmp_path,
        ingest_transport="jetstream",
        nats_username="heber",
        nats_password="test-only-password",  # pragma: allowlist secret
    )

    report = dataflow_health_module.generate_dataflow_report(
        now=now, window_seconds=900, mode="manual", settings=settings
    )

    control_plane = _find_check(report, "redis_control_plane")
    assert control_plane["status"] == "warn"
    assert report["summary"]["warn"] >= 1


def _find_check(report: dict, check_id: str) -> dict:
    return next(c for c in report["checks"] if c["id"] == check_id)


def test_consumer_lag_near_stream_cap_is_critical(monkeypatch) -> None:
    """When consumer lag approaches the stream length (MAXLEN cap), un-consumed
    events are being evicted — the silent-data-loss condition from the 2026-06-09
    option-quote flood. This must surface as a critical failure, not pass silently.
    """
    now = datetime(2026, 2, 12, 15, 0, tzinfo=UTC)
    signals = _signals(now)
    signals["redis"]["lag"] = 300_000
    signals["redis"]["stream_len"] = 300_000
    monkeypatch.setattr(dataflow_health_module, "_collect_runtime_signals", lambda **_kwargs: signals)
    monkeypatch.setattr(dataflow_health_module, "_is_market_open", lambda _ts: True)
    monkeypatch.setattr(dataflow_health_module, "_collect_dlq_size_check", _ok_dlq_check)

    report = dataflow_health_module.generate_dataflow_report(window_seconds=900, mode="manual", now=now)

    check = _find_check(report, "consumer_lag")
    assert check["status"] == "fail"
    assert check["severity"] == "critical"
    assert report["overall_status"] == "fail"


def test_consumer_lag_low_is_ok(monkeypatch) -> None:
    now = datetime(2026, 2, 12, 15, 0, tzinfo=UTC)
    monkeypatch.setattr(dataflow_health_module, "_collect_runtime_signals", lambda **_kwargs: _signals(now))
    monkeypatch.setattr(dataflow_health_module, "_is_market_open", lambda _ts: True)
    monkeypatch.setattr(dataflow_health_module, "_collect_dlq_size_check", _ok_dlq_check)

    report = dataflow_health_module.generate_dataflow_report(window_seconds=900, mode="manual", now=now)

    assert _find_check(report, "consumer_lag")["status"] == "ok"


def test_consumer_lag_mid_band_is_warning(monkeypatch) -> None:
    now = datetime(2026, 2, 12, 15, 0, tzinfo=UTC)
    signals = _signals(now)
    signals["redis"]["lag"] = 180_000
    signals["redis"]["stream_len"] = 300_000  # ratio 0.6 -> warn band (>=0.5, <0.8)
    monkeypatch.setattr(dataflow_health_module, "_collect_runtime_signals", lambda **_kwargs: signals)
    monkeypatch.setattr(dataflow_health_module, "_is_market_open", lambda _ts: True)
    monkeypatch.setattr(dataflow_health_module, "_collect_dlq_size_check", _ok_dlq_check)

    report = dataflow_health_module.generate_dataflow_report(window_seconds=900, mode="manual", now=now)

    assert _find_check(report, "consumer_lag")["status"] == "warn"


def test_consumer_lag_high_ratio_below_floor_stays_ok(monkeypatch) -> None:
    """A small stream fully behind (ratio ~1.0) but with lag under the absolute
    floor must NOT alarm — guards against noise on tiny/overnight streams.
    """
    now = datetime(2026, 2, 12, 15, 0, tzinfo=UTC)
    signals = _signals(now)
    signals["redis"]["lag"] = 200
    signals["redis"]["stream_len"] = 210  # ratio 0.95 but lag < 5000 floor
    monkeypatch.setattr(dataflow_health_module, "_collect_runtime_signals", lambda **_kwargs: signals)
    monkeypatch.setattr(dataflow_health_module, "_is_market_open", lambda _ts: True)
    monkeypatch.setattr(dataflow_health_module, "_collect_dlq_size_check", _ok_dlq_check)

    report = dataflow_health_module.generate_dataflow_report(window_seconds=900, mode="manual", now=now)

    assert _find_check(report, "consumer_lag")["status"] == "ok"


def test_consumer_lag_unknown_stream_len_is_skipped(monkeypatch) -> None:
    now = datetime(2026, 2, 12, 15, 0, tzinfo=UTC)
    signals = _signals(now)
    signals["redis"]["lag"] = 50_000
    signals["redis"]["stream_len"] = None  # xlen unavailable -> not evaluable
    monkeypatch.setattr(dataflow_health_module, "_collect_runtime_signals", lambda **_kwargs: signals)
    monkeypatch.setattr(dataflow_health_module, "_is_market_open", lambda _ts: True)
    monkeypatch.setattr(dataflow_health_module, "_collect_dlq_size_check", _ok_dlq_check)

    report = dataflow_health_module.generate_dataflow_report(window_seconds=900, mode="manual", now=now)

    assert _find_check(report, "consumer_lag")["status"] == "skipped"


def test_dlq_flood_escalates_to_critical(monkeypatch) -> None:
    """A DLQ that has grown into the hundreds of thousands (a rejection flood) must
    page as critical, not sit at warning like a steady-state trickle.
    """
    import heber.writer.dlq_fallback as dlq_fallback_mod

    # The signal is the durable audit-file count for today, not the Redis xlen.
    monkeypatch.setattr(dlq_fallback_mod, "count_fallback_files_for_today", lambda *_a, **_kw: 5_000)

    check = dataflow_health_module._collect_dlq_size_check(Settings())

    assert check["status"] == "fail"
    assert check["severity"] == "critical"
    assert check["observed"]["durable_files_today"] == 5_000


def test_dlq_steady_trickle_stays_ok(monkeypatch) -> None:
    """A handful of durable DLQ files (the real steady-state rate) must stay OK —
    the historical multi-million Redis xlen was eviction churn, not real failures.
    """
    import heber.writer.dlq_fallback as dlq_fallback_mod

    monkeypatch.setattr(dlq_fallback_mod, "count_fallback_files_for_today", lambda *_a, **_kw: 7)

    check = dataflow_health_module._collect_dlq_size_check(Settings())

    assert check["status"] == "ok"
    assert check["observed"]["durable_files_today"] == 7


def test_redis_signals_retry_absorbs_transient_blip(monkeypatch) -> None:
    """A single transient Redis failure must not flip the redis checks to fail —
    _collect_redis_signals retries once and returns the recovered signal.
    """
    calls = {"n": 0}

    def _fake_once(_settings):
        calls["n"] += 1
        if calls["n"] == 1:
            return {
                "ok": False,
                "group_exists": False,
                "lag": None,
                "pending": None,
                "stream_len": None,
                "error": "timed out",
            }
        return {"ok": True, "group_exists": True, "lag": 0, "pending": 0, "stream_len": 100, "error": None}

    monkeypatch.setattr(dataflow_health_module, "_collect_redis_signals_once", _fake_once)

    result = dataflow_health_module._collect_redis_signals(Settings(), retry_delay=0.0)

    assert result["ok"] is True
    assert calls["n"] == 2


def test_redis_signals_persistent_failure_stops_after_attempts(monkeypatch) -> None:
    """A genuinely-down Redis must still fail (not loop forever) after the bounded retries."""
    calls = {"n": 0}

    def _fake_once(_settings):
        calls["n"] += 1
        return {"ok": False, "group_exists": False, "lag": None, "pending": None, "stream_len": None, "error": "down"}

    monkeypatch.setattr(dataflow_health_module, "_collect_redis_signals_once", _fake_once)

    result = dataflow_health_module._collect_redis_signals(Settings(), attempts=2, retry_delay=0.0)

    assert result["ok"] is False
    assert calls["n"] == 2


class _FakeClient:
    def __init__(self, behaviour) -> None:
        self._behaviour = behaviour

    def __enter__(self):
        return self

    def __exit__(self, *_exc) -> None:
        return None

    def get(self, _url):
        result = self._behaviour()
        if isinstance(result, Exception):
            raise result
        return result


def test_metrics_fetch_retry_absorbs_transient_timeout(monkeypatch) -> None:
    """A single slow-scrape ReadTimeout must not be reported as down —
    _fetch_metrics_samples retries once and parses the recovered response.
    """
    calls = {"n": 0}

    class _Resp:
        status_code = 200
        text = 'heber_writer_last_write_unixtime{layer="silver",dataset="bars"} 1.0\n'

    def _behaviour():
        calls["n"] += 1
        if calls["n"] == 1:
            return TimeoutError("timed out")
        return _Resp()

    monkeypatch.setattr(dataflow_health_module, "create_http_client", lambda **_kw: _FakeClient(_behaviour))
    monkeypatch.setattr(dataflow_health_module, "raise_for_status", lambda _resp: None)

    samples, error = dataflow_health_module._fetch_metrics_samples("http://x/metrics", retry_delay=0.0)

    assert error is None
    assert calls["n"] == 2
    assert dataflow_health_module._metric_max(samples, "heber_writer_last_write_unixtime") == 1.0


def test_metrics_fetch_persistent_failure_reports_error(monkeypatch) -> None:
    """A genuinely-unreachable endpoint must fail after the bounded retries and
    surface the error string (filesystem fallback handles freshness)."""
    calls = {"n": 0}

    def _behaviour():
        calls["n"] += 1
        return TimeoutError("timed out")

    monkeypatch.setattr(dataflow_health_module, "create_http_client", lambda **_kw: _FakeClient(_behaviour))

    samples, error = dataflow_health_module._fetch_metrics_samples("http://x/metrics", attempts=2, retry_delay=0.0)

    assert samples is None
    assert error == "timed out"
    assert calls["n"] == 2


def test_dataflow_health_all_checks_pass_during_market_open(monkeypatch) -> None:
    now = datetime(2026, 2, 12, 15, 0, tzinfo=UTC)
    monkeypatch.setattr(dataflow_health_module, "_collect_runtime_signals", lambda **_kwargs: _signals(now))
    monkeypatch.setattr(dataflow_health_module, "_is_market_open", lambda _ts: True)
    monkeypatch.setattr(dataflow_health_module, "_backup_freshness_check", _ok_backup_check)
    monkeypatch.setattr(dataflow_health_module, "_collect_dlq_size_check", _ok_dlq_check)

    report = dataflow_health_module.generate_dataflow_report(window_seconds=900, mode="manual", now=now)

    assert report["overall_status"] == "ok"
    assert report["summary"]["fail"] == 0
    assert report["summary"]["warn"] == 0


def test_dataflow_health_bars_stale_is_fail(monkeypatch) -> None:
    now = datetime(2026, 2, 12, 15, 0, tzinfo=UTC)
    signals = _signals(now)
    signals["feeds"]["bars"] = (now - timedelta(minutes=30)).timestamp()
    monkeypatch.setattr(dataflow_health_module, "_collect_runtime_signals", lambda **_kwargs: signals)
    monkeypatch.setattr(dataflow_health_module, "_is_market_open", lambda _ts: True)

    report = dataflow_health_module.generate_dataflow_report(window_seconds=900, mode="manual", now=now)

    assert report["overall_status"] == "fail"
    bars_check = next(check for check in report["checks"] if check["id"] == "feed_freshness_bars")
    assert bars_check["status"] == "fail"


def test_dataflow_health_trades_and_flow_alerts_stale_are_warning(monkeypatch) -> None:
    now = datetime(2026, 2, 12, 15, 0, tzinfo=UTC)
    signals = _signals(now)
    stale = (now - timedelta(minutes=30)).timestamp()
    signals["feeds"]["trades"] = stale
    signals["feeds"]["flow_alerts"] = stale
    monkeypatch.setattr(dataflow_health_module, "_collect_runtime_signals", lambda **_kwargs: signals)
    monkeypatch.setattr(dataflow_health_module, "_is_market_open", lambda _ts: True)
    monkeypatch.setattr(dataflow_health_module, "_backup_freshness_check", _ok_backup_check)

    report = dataflow_health_module.generate_dataflow_report(window_seconds=900, mode="manual", now=now)

    assert report["overall_status"] == "warn"
    trades_check = next(check for check in report["checks"] if check["id"] == "feed_freshness_trades")
    flow_check = next(check for check in report["checks"] if check["id"] == "feed_freshness_flow_alerts")
    assert trades_check["status"] == "warn"
    assert flow_check["status"] == "warn"


def test_redis_unreachable_skips_group_check_instead_of_reporting_missing(monkeypatch) -> None:
    """A timed-out Redis probe must NOT fabricate a 'Consumer group is missing' critical.

    The group is not actually gone; redis_connection already carries the
    unreachable signal, so redis_consumer_group is reported skipped, not fail.
    """
    now = datetime(2026, 2, 12, 15, 0, tzinfo=UTC)
    signals = _signals(now)
    signals["redis"] = {
        "ok": False,
        "group_exists": False,
        "lag": None,
        "pending": None,
        "stream_len": None,
        "error": "Timeout reading from socket",
    }
    monkeypatch.setattr(dataflow_health_module, "_collect_runtime_signals", lambda **_kwargs: signals)
    monkeypatch.setattr(dataflow_health_module, "_is_market_open", lambda _ts: True)
    monkeypatch.setattr(dataflow_health_module, "_collect_dlq_size_check", _ok_dlq_check)

    report = dataflow_health_module.generate_dataflow_report(window_seconds=900, mode="manual", now=now)

    conn = _find_check(report, "redis_connection")
    group = _find_check(report, "redis_consumer_group")
    assert conn["status"] == "fail"  # the real unreachable signal is still reported
    assert group["status"] == "skipped"  # but no phantom "missing" critical
    assert "skipped" in group["message"].lower()


def test_redis_reachable_but_group_absent_still_fails(monkeypatch) -> None:
    """When Redis IS reachable and the group genuinely does not exist, still fail."""
    now = datetime(2026, 2, 12, 15, 0, tzinfo=UTC)
    signals = _signals(now)
    signals["redis"] = {
        "ok": True,
        "group_exists": False,
        "lag": None,
        "pending": None,
        "stream_len": 100_000,
        "error": None,
    }
    monkeypatch.setattr(dataflow_health_module, "_collect_runtime_signals", lambda **_kwargs: signals)
    monkeypatch.setattr(dataflow_health_module, "_is_market_open", lambda _ts: True)
    monkeypatch.setattr(dataflow_health_module, "_collect_dlq_size_check", _ok_dlq_check)

    report = dataflow_health_module.generate_dataflow_report(window_seconds=900, mode="manual", now=now)

    group = _find_check(report, "redis_consumer_group")
    assert group["status"] == "fail"
    assert group["message"] == "Consumer group is missing."


def test_dataflow_health_market_closed_skips_freshness_checks(monkeypatch) -> None:
    now = datetime(2026, 2, 12, 2, 0, tzinfo=UTC)
    monkeypatch.setattr(dataflow_health_module, "_collect_runtime_signals", lambda **_kwargs: _signals(now))
    monkeypatch.setattr(dataflow_health_module, "_is_market_open", lambda _ts: False)

    report = dataflow_health_module.generate_dataflow_report(window_seconds=900, mode="manual", now=now)

    assert report["market_open"] is False
    bars_check = next(check for check in report["checks"] if check["id"] == "feed_freshness_bars")
    assert bars_check["status"] == "skipped"


def test_dataflow_health_market_closed_skips_gateway_passive_check_when_metric_missing(monkeypatch) -> None:
    now = datetime(2026, 2, 12, 2, 0, tzinfo=UTC)
    signals = _signals(now)
    signals["gateway_last_success"] = None
    monkeypatch.setattr(dataflow_health_module, "_collect_runtime_signals", lambda **_kwargs: signals)
    monkeypatch.setattr(dataflow_health_module, "_is_market_open", lambda _ts: False)
    monkeypatch.setattr(dataflow_health_module, "_backup_freshness_check", _ok_backup_check)
    monkeypatch.setattr(dataflow_health_module, "_collect_dlq_size_check", _ok_dlq_check)

    report = dataflow_health_module.generate_dataflow_report(window_seconds=900, mode="manual", now=now)

    gateway_check = next(check for check in report["checks"] if check["id"] == "gateway_passive_activity")
    assert gateway_check["status"] == "skipped"
    assert report["overall_status"] == "ok"


def test_dataflow_health_market_closed_does_not_collect_filesystem_fallback(monkeypatch) -> None:
    now = datetime(2026, 2, 12, 2, 0, tzinfo=UTC)
    monkeypatch.setattr(dataflow_health_module, "_collect_redis_signals", lambda _settings: _signals(now)["redis"])
    monkeypatch.setattr(dataflow_health_module, "_collect_dlq_size_check", _ok_dlq_check)
    monkeypatch.setattr(dataflow_health_module, "_is_market_open", lambda _ts: False)

    def fail_metrics_collection(_url: str) -> tuple[list[tuple[str, dict[str, str], float]] | None, str | None]:
        raise AssertionError("metrics collection should not run while freshness checks are skipped")

    def fail_filesystem_collection(_settings: Settings) -> dict[str, float | None]:
        raise AssertionError("filesystem fallback should not run while freshness checks are skipped")

    monkeypatch.setattr(dataflow_health_module, "_fetch_metrics_samples", fail_metrics_collection)
    monkeypatch.setattr(dataflow_health_module, "_collect_filesystem_feed_timestamps", fail_filesystem_collection)

    report = dataflow_health_module.generate_dataflow_report(window_seconds=900, mode="manual", now=now)

    assert report["market_open"] is False
    bars_check = next(check for check in report["checks"] if check["id"] == "feed_freshness_bars")
    assert bars_check["status"] == "skipped"


def test_dataflow_health_market_open_warns_when_gateway_passive_metric_missing(monkeypatch) -> None:
    now = datetime(2026, 2, 12, 15, 0, tzinfo=UTC)
    signals = _signals(now)
    signals["gateway_last_success"] = None
    monkeypatch.setattr(dataflow_health_module, "_collect_runtime_signals", lambda **_kwargs: signals)
    monkeypatch.setattr(dataflow_health_module, "_is_market_open", lambda _ts: True)
    monkeypatch.setattr(dataflow_health_module, "_backup_freshness_check", _ok_backup_check)

    report = dataflow_health_module.generate_dataflow_report(window_seconds=900, mode="manual", now=now)

    gateway_check = next(check for check in report["checks"] if check["id"] == "gateway_passive_activity")
    assert gateway_check["status"] == "warn"
    assert report["overall_status"] == "warn"


def test_dataflow_health_uses_filesystem_fallback_when_metrics_unavailable(monkeypatch) -> None:
    now = datetime(2026, 2, 12, 15, 0, tzinfo=UTC)
    signals = _signals(now)
    fresh = now.timestamp() - 30
    signals["feeds"] = {"bars": None, "trades": None, "flow_alerts": None}
    signals["filesystem_feeds"] = {
        "bars": fresh,
        "trades": fresh,
        "flow_alerts": fresh,
    }
    monkeypatch.setattr(dataflow_health_module, "_collect_runtime_signals", lambda **_kwargs: signals)
    monkeypatch.setattr(dataflow_health_module, "_is_market_open", lambda _ts: True)
    monkeypatch.setattr(dataflow_health_module, "_backup_freshness_check", _ok_backup_check)
    monkeypatch.setattr(dataflow_health_module, "_collect_dlq_size_check", _ok_dlq_check)

    report = dataflow_health_module.generate_dataflow_report(window_seconds=900, mode="manual", now=now)

    assert report["overall_status"] == "ok"
    bars_check = next(check for check in report["checks"] if check["id"] == "feed_freshness_bars")
    assert bars_check["observed"]["source"] == "filesystem"


def test_dataflow_health_redis_unavailable_is_critical_fail(monkeypatch) -> None:
    now = datetime(2026, 2, 12, 15, 0, tzinfo=UTC)
    signals = _signals(now)
    signals["redis"] = {
        "ok": False,
        "group_exists": False,
        "lag": None,
        "pending": None,
        "error": "redis unavailable",
    }
    monkeypatch.setattr(dataflow_health_module, "_collect_runtime_signals", lambda **_kwargs: signals)
    monkeypatch.setattr(dataflow_health_module, "_is_market_open", lambda _ts: True)

    report = dataflow_health_module.generate_dataflow_report(window_seconds=900, mode="manual", now=now)

    assert report["overall_status"] == "fail"
    redis_check = next(check for check in report["checks"] if check["id"] == "redis_connection")
    assert redis_check["status"] == "fail"


def test_dataflow_health_report_write_failure_is_warn_only(monkeypatch) -> None:
    now = datetime(2026, 2, 12, 15, 0, tzinfo=UTC)
    monkeypatch.setattr(dataflow_health_module, "_collect_runtime_signals", lambda **_kwargs: _signals(now))
    monkeypatch.setattr(dataflow_health_module, "_is_market_open", lambda _ts: True)
    monkeypatch.setattr(dataflow_health_module, "_backup_freshness_check", _ok_backup_check)
    monkeypatch.setattr(dataflow_health_module, "_utc_now", lambda: now)
    monkeypatch.setattr(dataflow_health_module, "_collect_dlq_size_check", _ok_dlq_check)

    def _raise_write_error(*_args, **_kwargs) -> None:  # noqa: ANN002, ANN003
        raise OSError("read-only path")

    monkeypatch.setattr(dataflow_health_module, "_write_report", _raise_write_error)

    report = dataflow_health_module.run_dataflow_health_once(
        window_seconds=900,
        mode="manual",
        report_dir="/data/ops/dataflow-health",
    )

    assert report["overall_status"] == "ok"


def test_latest_file_mtime_ignores_hidden_files(tmp_path) -> None:
    root = tmp_path / "feed=bars"
    root.mkdir(parents=True)
    visible = root / "part-1.parquet"
    hidden = root / ".compaction.lock"
    visible.write_text("visible", encoding="utf-8")
    hidden.write_text("hidden", encoding="utf-8")
    os.utime(visible, (1_000_000, 1_000_000))
    os.utime(hidden, (2_000_000, 2_000_000))

    latest = dataflow_health_module._latest_file_mtime(root)

    assert latest == visible.stat().st_mtime


def test_latest_file_mtime_handles_file_disappearing_during_scan(tmp_path, monkeypatch) -> None:
    root = tmp_path / "feed=bars"
    root.mkdir(parents=True)
    stable = root / "part-stable.parquet"
    volatile = root / "part-volatile.parquet"
    stable.write_text("stable", encoding="utf-8")
    os.utime(stable, (1_000_000, 1_000_000))

    original_rglob = Path.rglob
    original_is_file = Path.is_file

    def fake_rglob(path: Path, pattern: str):  # noqa: ANN202
        if path == root and pattern == "*":
            return iter([stable, volatile])
        return original_rglob(path, pattern)

    def fake_is_file(path: Path) -> bool:
        if path == volatile:
            return True
        return original_is_file(path)

    monkeypatch.setattr(dataflow_health_module.Path, "rglob", fake_rglob)
    monkeypatch.setattr(dataflow_health_module.Path, "is_file", fake_is_file)

    latest = dataflow_health_module._latest_file_mtime(root)

    assert latest == stable.stat().st_mtime


def test_collect_filesystem_feed_timestamps_avoids_full_feed_scan(tmp_path, monkeypatch) -> None:
    settings = Settings(data_root=tmp_path)
    feed_root = settings.silver_path / "feed=bars" / "instrument_type=equity"
    cold_partition = feed_root / "dt=2026-05-20"
    old_partition = feed_root / "dt=2026-05-27"
    mid_partition = feed_root / "dt=2026-05-28"
    new_partition = feed_root / "dt=2026-05-29"
    cold_partition.mkdir(parents=True)
    old_partition.mkdir(parents=True)
    mid_partition.mkdir(parents=True)
    new_partition.mkdir(parents=True)

    cold_file = cold_partition / "part-cold.parquet"
    old_file = old_partition / "part-old.parquet"
    mid_file = mid_partition / "part-mid.parquet"
    new_file = new_partition / "part-new.parquet"
    cold_file.write_text("cold", encoding="utf-8")
    old_file.write_text("old", encoding="utf-8")
    mid_file.write_text("mid", encoding="utf-8")
    new_file.write_text("new", encoding="utf-8")
    os.utime(cold_file, (3_000_000, 3_000_000))
    os.utime(old_file, (1_000_000, 1_000_000))
    os.utime(mid_file, (1_500_000, 1_500_000))
    os.utime(new_file, (2_000_000, 2_000_000))
    os.utime(cold_partition, (3_000_000, 3_000_000))
    os.utime(old_partition, (1_000_000, 1_000_000))
    os.utime(mid_partition, (1_500_000, 1_500_000))
    os.utime(new_partition, (2_000_000, 2_000_000))

    def guarded_rglob(path: Path, pattern: str):  # noqa: ANN202
        raise AssertionError(f"dataflow health must not recursively scan partitioned feeds: {path}:{pattern}")

    monkeypatch.setattr(Path, "rglob", guarded_rglob)

    timestamps = dataflow_health_module._collect_filesystem_feed_timestamps(settings)

    assert timestamps["bars"] == new_file.stat().st_mtime


def test_backup_freshness_fresh_marker_is_ok(tmp_path) -> None:
    marker = tmp_path / "ops" / "backup-last-ok"
    marker.parent.mkdir(parents=True)
    marker.touch()

    check = dataflow_health_module._backup_freshness_check(Settings(data_root=tmp_path))

    assert check["id"] == "backup_freshness"
    assert check["status"] == "ok"


def test_backup_freshness_missing_marker_is_fail(tmp_path) -> None:
    check = dataflow_health_module._backup_freshness_check(Settings(data_root=tmp_path))

    assert check["status"] == "fail"
    assert check["observed"]["age_hours"] is None
