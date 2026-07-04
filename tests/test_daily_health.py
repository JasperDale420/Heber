"""Tests for heber.ops.daily_health — daily end-of-day health report."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from heber.config import Settings
from heber.ops.daily_health import (
    _check_cross_feed_completeness,
    _check_gold_freshness,
    generate_daily_health_report,
    write_daily_report,
)


@pytest.fixture()
def settings(tmp_path: Path) -> Settings:
    """Build Settings pointing at tmp_path for data_root."""
    return Settings(
        data_root=tmp_path,
        daily_health_report_dir=tmp_path / "ops" / "daily-health",
        daily_health_expected_symbol_count=3,
        daily_health_expected_feeds=["bars", "quotes"],
    )


def _write_silver_partition(
    data_root: Path,
    feed: str,
    dt: date,
    df: pd.DataFrame,
    instrument_type: str = "equity",
) -> Path:
    # Mirror the writer's real layout: feed={feed}/instrument_type={type}/dt={dt}.
    partition = data_root / "silver" / f"feed={feed}" / f"instrument_type={instrument_type}" / f"dt={dt.isoformat()}"
    partition.mkdir(parents=True, exist_ok=True)
    out = partition / "data.parquet"
    df.to_parquet(out, index=False)
    return out


def _write_gold_partition(
    data_root: Path,
    dataset: str,
    dt: date,
    df: pd.DataFrame,
) -> Path:
    partition = data_root / "gold" / f"dataset={dataset}" / "project=watch" / "version=v1" / f"dt={dt.isoformat()}"
    partition.mkdir(parents=True, exist_ok=True)
    out = partition / "data.parquet"
    df.to_parquet(out, index=False)
    return out


# ---------------------------------------------------------------------------
# Cross-Feed Completeness
# ---------------------------------------------------------------------------


class TestCrossFeedCompleteness:
    def test_good_overlap(self, settings: Settings) -> None:
        dt = date(2026, 3, 9)
        for feed in ("bars", "quotes"):
            _write_silver_partition(
                settings.data_root,
                feed,
                dt,
                pd.DataFrame({"instrument_key": ["equity:AAPL", "equity:MSFT"]}),
            )
        result = _check_cross_feed_completeness(dt, settings)
        assert result["status"] == "ok"
        assert result["observed"]["min_coverage"] == 1.0

    def test_partial_overlap(self, settings: Settings) -> None:
        dt = date(2026, 3, 9)
        _write_silver_partition(
            settings.data_root,
            "bars",
            dt,
            pd.DataFrame({"instrument_key": ["equity:AAPL", "equity:MSFT"]}),
        )
        _write_silver_partition(
            settings.data_root,
            "quotes",
            dt,
            pd.DataFrame({"instrument_key": ["equity:AAPL"]}),
        )
        result = _check_cross_feed_completeness(dt, settings)
        assert result["observed"]["min_coverage"] == 0.5

    def test_single_feed_warns(self, settings: Settings) -> None:
        dt = date(2026, 3, 9)
        _write_silver_partition(
            settings.data_root,
            "bars",
            dt,
            pd.DataFrame({"instrument_key": ["equity:AAPL"]}),
        )
        result = _check_cross_feed_completeness(dt, settings)
        assert result["status"] == "warn"


# ---------------------------------------------------------------------------
# Gold Freshness
# ---------------------------------------------------------------------------


class TestGoldFreshness:
    def test_gold_partitions_exist(self, settings: Settings) -> None:
        dt = date(2026, 3, 9)
        for ds in ("meta_label_features", "labels_alert_barriers"):
            _write_gold_partition(
                settings.data_root,
                ds,
                dt,
                pd.DataFrame({"alert_id": ["a1"]}),
            )
        result = _check_gold_freshness(dt, settings)
        assert result["status"] == "ok"
        assert len(result["observed"]["missing"]) == 0

    def test_gold_partitions_missing(self, settings: Settings) -> None:
        result = _check_gold_freshness(date(2026, 3, 9), settings)
        assert result["status"] == "warn"
        assert len(result["observed"]["missing"]) == 2


# ---------------------------------------------------------------------------
# Full Report
# ---------------------------------------------------------------------------


class TestGenerateReport:
    def test_non_trading_day_skipped(self, settings: Settings) -> None:
        # Saturday 2026-03-07
        with patch("heber.ops.daily_health.MarketCalendar") as MockCal:
            MockCal.return_value.is_trading_day.return_value = False
            report = generate_daily_health_report(
                report_date=date(2026, 3, 7),
                settings=settings,
            )
        assert report["overall_status"] == "skipped"
        assert report["checks"] == []

    def test_non_trading_day_with_force(self, settings: Settings) -> None:
        with patch("heber.ops.daily_health.MarketCalendar") as MockCal:
            MockCal.return_value.is_trading_day.return_value = False
            report = generate_daily_health_report(
                report_date=date(2026, 3, 7),
                settings=settings,
                force=True,
                skip_soda=True,
            )
        assert report["overall_status"] != "skipped"
        assert len(report["checks"]) > 0

    def test_full_report_all_ok(self, settings: Settings) -> None:
        dt = date(2026, 3, 9)
        now = datetime(2026, 3, 9, 20, 0, tzinfo=UTC)

        for feed in ("bars", "quotes"):
            _write_silver_partition(
                settings.data_root,
                feed,
                dt,
                pd.DataFrame(
                    {
                        "instrument_key": ["equity:AAPL", "equity:MSFT", "equity:GOOG"],
                        "ts_event": [now, now, now],
                        "ts_available": [now, now, now],
                    }
                ),
            )

        for ds in ("meta_label_features", "labels_alert_barriers"):
            _write_gold_partition(settings.data_root, ds, dt, pd.DataFrame({"alert_id": ["a1"]}))

        with patch("heber.ops.daily_health.MarketCalendar") as MockCal:
            MockCal.return_value.is_trading_day.return_value = True
            report = generate_daily_health_report(
                report_date=dt,
                settings=settings,
                skip_soda=True,
            )

        assert report["overall_status"] == "ok"
        assert report["summary"]["fail"] == 0

    def test_report_has_required_fields(self, settings: Settings) -> None:
        with patch("heber.ops.daily_health.MarketCalendar") as MockCal:
            MockCal.return_value.is_trading_day.return_value = True
            report = generate_daily_health_report(
                report_date=date(2026, 3, 9),
                settings=settings,
                skip_soda=True,
            )
        assert "ts_utc" in report
        assert "report_date" in report
        assert "overall_status" in report
        assert "checks" in report
        assert "summary" in report


# ---------------------------------------------------------------------------
# Write Report
# ---------------------------------------------------------------------------


class TestWriteReport:
    def test_writes_json(self, tmp_path: Path) -> None:
        report = {
            "ts_utc": "2026-03-09T20:00:00+00:00",
            "report_date": "2026-03-09",
            "overall_status": "ok",
            "checks": [],
            "summary": {"ok": 0, "warn": 0, "fail": 0, "skipped": 0},
        }
        path = write_daily_report(report, tmp_path / "reports")
        assert path.exists()
        assert path.name == "2026-03-09.json"

    def test_creates_directory(self, tmp_path: Path) -> None:
        report_dir = tmp_path / "nested" / "reports"
        report = {"report_date": "2026-03-09"}
        write_daily_report(report, report_dir)
        assert report_dir.exists()
