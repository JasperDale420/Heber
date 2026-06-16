"""Tests for liveness-loop + notifier wiring in HealthMonitorService."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from heber.health_monitor.models import CheckResult, Severity, Status
from heber.health_monitor.service import HealthMonitorService

UTC = UTC


@pytest.mark.unit
async def test_maybe_alert_dispatches_results() -> None:
    svc = HealthMonitorService()
    svc._notifier = MagicMock()
    svc._notifier.dispatch = MagicMock()
    results = [
        CheckResult(
            check_name="feed_liveness",
            feed="flow_alerts",
            severity=Severity.P0_CRITICAL,
            status=Status.FAIL,
            message="dark",
            details={},
            ts_checked=datetime.now(UTC),
        )
    ]
    await svc._maybe_alert(results)
    svc._notifier.dispatch.assert_called_once_with(results)


@pytest.mark.unit
async def test_maybe_alert_swallows_notifier_errors() -> None:
    svc = HealthMonitorService()
    svc._notifier = MagicMock()
    svc._notifier.dispatch = MagicMock(side_effect=RuntimeError("boom"))
    # Must not raise.
    await svc._maybe_alert([])


@pytest.mark.unit
async def test_maybe_alert_noop_when_no_notifier() -> None:
    svc = HealthMonitorService()
    svc._notifier = None
    await svc._maybe_alert([])  # must not raise
