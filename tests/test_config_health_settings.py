from __future__ import annotations

from pathlib import Path

from heber.config import Settings


def test_health_settings_defaults(monkeypatch) -> None:
    monkeypatch.delenv("HEBER_HEALTH_CONSUMER_METRICS_URL", raising=False)
    monkeypatch.delenv("HEBER_HEALTH_WATCH_METRICS_URL", raising=False)
    monkeypatch.delenv("HEBER_HEALTH_FRESHNESS_SECONDS", raising=False)
    monkeypatch.delenv("HEBER_HEALTH_REPORT_DIR", raising=False)
    monkeypatch.delenv("HEBER_HEALTH_INTERVAL_SECONDS", raising=False)

    settings = Settings(_env_file=None)

    assert settings.health_consumer_metrics_url == "http://localhost:9090/metrics"
    assert settings.health_watch_metrics_url == "http://localhost:9091/metrics"
    assert settings.health_freshness_seconds == 900
    assert settings.health_report_dir == Path("/data/ops/dataflow-health")
    assert settings.health_interval_seconds == 300


def test_health_settings_honor_env_overrides(monkeypatch) -> None:
    monkeypatch.setenv("HEBER_HEALTH_CONSUMER_METRICS_URL", "http://heber-consumer:9090/metrics")
    monkeypatch.setenv("HEBER_HEALTH_WATCH_METRICS_URL", "http://heber-watch:9090/metrics")
    monkeypatch.setenv("HEBER_HEALTH_FRESHNESS_SECONDS", "1200")
    monkeypatch.setenv("HEBER_HEALTH_REPORT_DIR", "/tmp/dataflow-health")
    monkeypatch.setenv("HEBER_HEALTH_INTERVAL_SECONDS", "600")

    settings = Settings(_env_file=None)

    assert settings.health_consumer_metrics_url == "http://heber-consumer:9090/metrics"
    assert settings.health_watch_metrics_url == "http://heber-watch:9090/metrics"
    assert settings.health_freshness_seconds == 1200
    assert settings.health_report_dir == Path("/tmp/dataflow-health")
    assert settings.health_interval_seconds == 600
