"""Tests for health check result storage."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pandas as pd
import pytest

from heber.health_monitor.models import CheckResult, Severity, Status
from heber.health_monitor.store import HealthStore


@pytest.fixture()
def store(tmp_path):
    return HealthStore(data_root=tmp_path)


@pytest.fixture()
def sample_results():
    ts = datetime(2026, 3, 26, 16, 0, 0, tzinfo=UTC)
    return [
        CheckResult(
            check_name="partition_completeness",
            feed="bars",
            severity=Severity.P0_CRITICAL,
            status=Status.FAIL,
            message="Missing hour=14 partition",
            details={"missing_hours": [14]},
            ts_checked=ts,
        ),
        CheckResult(
            check_name="volume_trending",
            feed="trades",
            severity=Severity.P2_INFO,
            status=Status.PASS,
            message="Volume within baseline",
            details={"row_count": 50000, "baseline": 48000},
            ts_checked=ts,
        ),
    ]


class TestHealthStore:
    def test_write_results(self, store, sample_results):
        store.write_results(sample_results, report_date=date(2026, 3, 26))
        partition = store.data_root / "gold" / "dataset=data_health" / "dt=2026-03-26"
        assert partition.exists()
        files = list(partition.glob("*.parquet"))
        assert len(files) == 1

    def test_read_results(self, store, sample_results):
        store.write_results(sample_results, report_date=date(2026, 3, 26))
        df = store.read_results(date(2026, 3, 26))
        assert len(df) == 2
        assert "check_name" in df.columns
        assert set(df["check_name"]) == {"partition_completeness", "volume_trending"}

    def test_read_empty(self, store):
        df = store.read_results(date(2026, 3, 26))
        assert len(df) == 0

    def test_write_baseline(self, store):
        baseline = pd.DataFrame(
            {
                "feed": ["bars", "trades"],
                "hour": [10, 10],
                "row_count_median": [45000.0, 52000.0],
            }
        )
        store.write_baseline(baseline, report_date=date(2026, 3, 26))
        partition = store.data_root / "gold" / "dataset=data_health_baselines" / "dt=2026-03-26"
        assert partition.exists()

    def test_read_baseline_range(self, store):
        for day_offset in range(5):
            d = date(2026, 3, 20 + day_offset)
            baseline = pd.DataFrame(
                {
                    "feed": ["bars"],
                    "hour": [10],
                    "row_count_median": [45000.0 + day_offset * 100],
                }
            )
            store.write_baseline(baseline, report_date=d)
        df = store.read_baselines(start_date=date(2026, 3, 20), end_date=date(2026, 3, 24))
        assert len(df) == 5
