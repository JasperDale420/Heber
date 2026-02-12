from __future__ import annotations

from datetime import UTC, datetime, timedelta

from heber.ops import dataflow_health as dataflow_health_module


def _signals(now: datetime) -> dict:
    fresh = now.timestamp() - 60
    return {
        "redis": {
            "ok": True,
            "group_exists": True,
            "lag": 0,
            "pending": 0,
            "error": None,
        },
        "feeds": {
            "bars": fresh,
            "trades": fresh,
            "flow_alerts": fresh,
        },
        "gateway_last_success": fresh,
    }


def test_dataflow_health_all_checks_pass_during_market_open(monkeypatch) -> None:
    now = datetime(2026, 2, 12, 15, 0, tzinfo=UTC)
    monkeypatch.setattr(dataflow_health_module, "_collect_runtime_signals", lambda **_kwargs: _signals(now))
    monkeypatch.setattr(dataflow_health_module, "_is_market_open", lambda _ts: True)

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

    report = dataflow_health_module.generate_dataflow_report(window_seconds=900, mode="manual", now=now)

    assert report["overall_status"] == "warn"
    trades_check = next(check for check in report["checks"] if check["id"] == "feed_freshness_trades")
    flow_check = next(check for check in report["checks"] if check["id"] == "feed_freshness_flow_alerts")
    assert trades_check["status"] == "warn"
    assert flow_check["status"] == "warn"


def test_dataflow_health_market_closed_skips_freshness_checks(monkeypatch) -> None:
    now = datetime(2026, 2, 12, 2, 0, tzinfo=UTC)
    monkeypatch.setattr(dataflow_health_module, "_collect_runtime_signals", lambda **_kwargs: _signals(now))
    monkeypatch.setattr(dataflow_health_module, "_is_market_open", lambda _ts: False)

    report = dataflow_health_module.generate_dataflow_report(window_seconds=900, mode="manual", now=now)

    assert report["market_open"] is False
    bars_check = next(check for check in report["checks"] if check["id"] == "feed_freshness_bars")
    assert bars_check["status"] == "skipped"


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

    def _raise_write_error(*_args, **_kwargs) -> None:  # noqa: ANN002, ANN003
        raise OSError("read-only path")

    monkeypatch.setattr(dataflow_health_module, "_write_report", _raise_write_error)

    report = dataflow_health_module.run_dataflow_health_once(
        window_seconds=900,
        mode="manual",
        report_dir="/data/ops/dataflow-health",
    )

    assert report["overall_status"] == "ok"
