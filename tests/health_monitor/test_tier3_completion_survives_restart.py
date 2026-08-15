"""Tier 3 must not re-run from scratch after every restart.

`_should_run_tier3` gates on `self._last_tier3_date`, which lives only in
memory. The health monitor restarted 59 times in one week — 20 in a single day
— so each restart cleared the marker and the daily schema, statistical and
ml-readiness sweep began again from the top. Across that week not one Tier 3
run was ever recorded as finishing.

Completion is inferred from results already on disk rather than a new marker:
`HealthStore` writes every run into a dated partition, and the Tier 3 check
names are distinct from the other tiers'. The store is consulted once per
process, when the in-memory marker is empty, so the 60s loop does not re-read
Parquet from the bind mount on every tick.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest

from heber.health_monitor.service import TIER3_CHECK_NAMES, HealthMonitorService


def _service(monkeypatch: pytest.MonkeyPatch, *, stored: pd.DataFrame) -> HealthMonitorService:
    service = HealthMonitorService.__new__(HealthMonitorService)
    service._last_tier3_date = None
    service._tier3_store_checked = False
    store = MagicMock()
    store.read_results.return_value = stored
    service._store = store
    return service


def _tier3_rows() -> pd.DataFrame:
    return pd.DataFrame({"check_name": ["schema_drift", "statistical_null_rate"], "status": ["ok", "ok"]})


def _other_tier_rows() -> pd.DataFrame:
    return pd.DataFrame({"check_name": ["stream_liveness", "partition_freshness"], "status": ["ok", "ok"]})


def test_tier3_names_cover_all_three_check_families() -> None:
    """A marker set that misses a family would under-detect completion."""
    assert {"schema_drift", "statistical_null_rate", "ml_leakage_audit"} <= TIER3_CHECK_NAMES


def test_completed_run_is_recognised_after_a_restart(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _service(monkeypatch, stored=_tier3_rows())

    assert service._tier3_already_ran(date(2026, 8, 13)) is True


def test_other_tiers_do_not_count_as_completion(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tier 1 and 2 write into the same dated partition all day."""
    service = _service(monkeypatch, stored=_other_tier_rows())

    assert service._tier3_already_ran(date(2026, 8, 13)) is False


def test_no_results_means_not_run(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _service(monkeypatch, stored=pd.DataFrame(columns=["check_name"]))

    assert service._tier3_already_ran(date(2026, 8, 13)) is False


def test_store_is_consulted_once_not_every_tick(monkeypatch: pytest.MonkeyPatch) -> None:
    """The loop ticks every 60s; re-reading Parquet each time is I/O on a slow mount."""
    service = _service(monkeypatch, stored=_tier3_rows())

    for _ in range(5):
        service._tier3_already_ran(date(2026, 8, 13))

    assert service._store.read_results.call_count == 1


def test_unreadable_store_does_not_block_the_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """If completion cannot be determined, running again is the safe answer."""
    service = _service(monkeypatch, stored=_tier3_rows())
    service._store.read_results.side_effect = OSError("volume unavailable")

    assert service._tier3_already_ran(date(2026, 8, 13)) is False


def test_should_run_tier3_consults_the_store_after_a_restart(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pins the wiring, not just the probe.

    An earlier version of this file tested `_tier3_already_ran` in isolation, so
    deleting the call to it from `_should_run_tier3` broke nothing and the
    daily sweep would still have restarted from scratch on every crash.
    """
    from datetime import datetime

    from heber.health_monitor.service import ET

    service = _service(monkeypatch, stored=_tier3_rows())
    today = datetime.now(ET).date()

    assert service._should_run_tier3() is False, "re-ran a sweep already recorded for today"
    assert service._last_tier3_date == today, "in-memory marker not restored from disk"


def test_should_run_tier3_proceeds_when_the_store_is_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """With nothing recorded, the gate must fall through to the calendar/time checks."""
    service = _service(monkeypatch, stored=pd.DataFrame(columns=["check_name"]))
    service._calendar = MagicMock()
    service._calendar.is_trading_day.return_value = False  # stops here, deterministically

    assert service._should_run_tier3() is False
    assert service._last_tier3_date is None, "marker set despite nothing being recorded"
