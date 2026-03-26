"""Tests for health monitor data models."""

from __future__ import annotations

from datetime import UTC, datetime

from heber.health_monitor.models import CheckResult, Severity, Status


class TestSeverity:
    def test_values(self):
        assert Severity.P0_CRITICAL == "critical"
        assert Severity.P1_WARNING == "warning"
        assert Severity.P2_INFO == "info"

    def test_ordering(self):
        assert Severity.P0_CRITICAL.is_more_severe_than(Severity.P1_WARNING)
        assert Severity.P1_WARNING.is_more_severe_than(Severity.P2_INFO)
        assert not Severity.P2_INFO.is_more_severe_than(Severity.P0_CRITICAL)


class TestStatus:
    def test_values(self):
        assert Status.PASS == "pass"
        assert Status.WARN == "warn"
        assert Status.FAIL == "fail"
        assert Status.ERROR == "error"

    def test_is_healthy(self):
        assert Status.PASS.is_healthy
        assert not Status.WARN.is_healthy
        assert not Status.FAIL.is_healthy
        assert not Status.ERROR.is_healthy


class TestCheckResult:
    def test_creation(self):
        result = CheckResult(
            check_name="test_check",
            feed="bars",
            severity=Severity.P1_WARNING,
            status=Status.FAIL,
            message="Missing partition",
            details={"partition": "dt=2026-03-26"},
            ts_checked=datetime(2026, 3, 26, 16, 0, 0, tzinfo=UTC),
        )
        assert result.check_name == "test_check"
        assert result.feed == "bars"
        assert result.instrument_key is None

    def test_to_dict(self):
        result = CheckResult(
            check_name="test_check",
            feed="bars",
            severity=Severity.P0_CRITICAL,
            status=Status.FAIL,
            message="Critical failure",
            details={"error": "missing"},
            ts_checked=datetime(2026, 3, 26, 16, 0, 0, tzinfo=UTC),
        )
        d = result.to_dict()
        assert d["check_name"] == "test_check"
        assert d["severity"] == "critical"
        assert d["status"] == "fail"
        assert isinstance(d["ts_checked"], str)

    def test_to_flat_row(self):
        result = CheckResult(
            check_name="volume_check",
            feed="trades",
            severity=Severity.P2_INFO,
            status=Status.PASS,
            message="Volume OK",
            details={"row_count": 50000, "baseline": 48000},
            ts_checked=datetime(2026, 3, 26, 16, 0, 0, tzinfo=UTC),
        )
        row = result.to_flat_row()
        assert row["check_name"] == "volume_check"
        assert row["feed"] == "trades"
        assert isinstance(row["details_json"], str)
