"""Regression: the EOD poller computes TODAY's just-closed session.

The default trigger is 16:35 ET, but _run_all_pipelines used to call
_last_trading_day(), which gated on a hardcoded 17:00 ET and returned
*yesterday* for any run before 17:00 — and because _last_run_date is then set
to today, the corrective 17:00 run never fired, leaving the lakehouse one
trading day stale every 16:35-16:59 ET window.
"""

from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

import heber.gold_poller.service as service
from heber.gold_poller.service import GoldFeaturePoller

ET = ZoneInfo("America/New_York")


class _FrozenDateTime(datetime):
    """datetime subclass whose .now(tz) returns a fixed ET instant."""

    _frozen: datetime

    @classmethod
    def now(cls, tz=None):  # type: ignore[override]
        return cls._frozen if tz is None else cls._frozen.astimezone(tz)


@pytest.mark.unit
async def test_run_all_pipelines_targets_todays_session(monkeypatch) -> None:
    # 2026-06-25 (Thu, a trading day) at 16:40 ET — after the 16:35 trigger,
    # before the old 17:00 cutoff: the exact window that used to go stale.
    frozen = datetime(2026, 6, 25, 16, 40, tzinfo=ET)
    _FrozenDateTime._frozen = frozen
    monkeypatch.setattr(service, "datetime", _FrozenDateTime)

    poller = GoldFeaturePoller()
    poller._settings = SimpleNamespace(
        gold_poller_lookback_days=1,
        gold_poller_disabled_pipeline_set=set(),
    )

    captured: list[tuple[date, date]] = []

    async def _fake_run_pipeline(entry, start_date, end_date):
        captured.append((start_date, end_date))
        return {"status": "success", "rows": 0}

    monkeypatch.setattr(poller, "_run_pipeline", _fake_run_pipeline)
    # PIPELINE_REGISTRY is module-global; replace with a single stub entry.
    monkeypatch.setattr(service, "PIPELINE_REGISTRY", [{"name": "stub", "datasets": None}])

    await poller._run_all_pipelines()

    assert captured, "no pipeline was run"
    start_date, end_date = captured[0]
    assert end_date == date(2026, 6, 25), f"end_date should be today's session, got {end_date}"
    assert start_date == date(2026, 6, 24)  # lookback=1
    # The bug returned 2026-06-24 as end_date (yesterday).
    assert end_date != date(2026, 6, 24)
