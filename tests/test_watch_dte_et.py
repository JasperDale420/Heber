"""DTE must be computed against the ET market date, not UTC/local.

In the evening ET (past UTC midnight) a UTC-based `today` is already tomorrow,
so days-to-expiry would be understated by a day for every alert processed after
~20:00 ET — pushing options into the wrong DTE bucket.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

import heber.watch.consumer as wc
from heber.watch.consumer import AlertWatchConsumer

ET = ZoneInfo("America/New_York")


class _FrozenDateTime(datetime):
    _frozen: datetime

    @classmethod
    def now(cls, tz=None):  # type: ignore[override]
        return cls._frozen.astimezone(tz) if tz is not None else cls._frozen


@pytest.mark.unit
def test_dte_uses_et_date_in_the_evening(monkeypatch) -> None:
    # 2026-06-25 21:00 ET == 2026-06-26 01:00 UTC.
    _FrozenDateTime._frozen = datetime(2026, 6, 25, 21, 0, tzinfo=ET)
    monkeypatch.setattr(wc, "datetime", _FrozenDateTime)

    # ET today = 06-25 -> expiry 06-26 is 1 DTE. A UTC `today` (06-26) gives 0.
    assert AlertWatchConsumer._calculate_dte(None, "2026-06-26") == 1
    assert AlertWatchConsumer._calculate_dte(None, "2026-06-25") == 0


@pytest.mark.unit
def test_dte_missing_expiry_falls_back(monkeypatch) -> None:
    _FrozenDateTime._frozen = datetime(2026, 6, 25, 10, 0, tzinfo=ET)
    monkeypatch.setattr(wc, "datetime", _FrozenDateTime)
    assert AlertWatchConsumer._calculate_dte(None, None) == 5
    assert AlertWatchConsumer._calculate_dte(None, "not-a-date") == 5
