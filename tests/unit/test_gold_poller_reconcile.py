"""Tests for the post-EOD self-heal reconcile (re-pull missing daily UW feeds)."""

from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

import heber.gold_poller.reconcile as reconcile_module
import heber.gold_poller.service as service
from heber.gold_poller.reconcile import detect_missing_eod_feeds, reconcile_eod_feeds
from heber.gold_poller.service import GoldFeaturePoller

ET = ZoneInfo("America/New_York")
TODAY = date(2026, 6, 25)


class _FakeReader:
    """Reader stub: returns `rows_by_feed[feed]` rows, or raises if feed is in `errors`."""

    def __init__(self, rows_by_feed: dict[str, int], errors: set[str] | None = None) -> None:
        self._rows = rows_by_feed
        self._errors = errors or set()

    def read_silver(self, feed, **_kwargs):  # noqa: ANN001, ANN003
        if feed in self._errors:
            raise OSError("bind-mount read error")
        return pd.DataFrame({"ts_event": list(range(self._rows.get(feed, 0)))})


class _FakeResponse:
    def __init__(self, ok: bool) -> None:
        self._ok = ok

    def raise_for_status(self) -> None:
        if not self._ok:
            raise RuntimeError("backfill returned 500")


class _FakeClient:
    """Async-context-manager HTTP client stub recording POSTs; fails feeds in `fail`."""

    def __init__(self, fail: set[str] | None = None) -> None:
        self.calls: list[dict] = []
        self._fail = fail or set()

    async def post(self, route, json=None, headers=None):  # noqa: ANN001
        self.calls.append({"route": route, "json": json, "headers": headers})
        return _FakeResponse(ok=json["feed"] not in self._fail)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_a):
        return False


def _settings(**overrides):
    base = dict(
        eod_reconcile_feed_list=["oi_change", "iv_rank", "historic_option_volume"],
        eod_reconcile_symbol_list=["SPY", "AAPL"],
        watch_gateway_url="http://gw:8080",
        watch_gateway_api_key="testkey",  # pragma: allowlist secret
    )
    base.update(overrides)
    return SimpleNamespace(**base)


@pytest.mark.unit
def test_detect_missing_flags_only_empty_feeds() -> None:
    reader = _FakeReader({"oi_change": 0, "iv_rank": 12, "historic_option_volume": 0})
    missing = detect_missing_eod_feeds(reader, ["oi_change", "iv_rank", "historic_option_volume"], TODAY)
    assert missing == ["oi_change", "historic_option_volume"]


@pytest.mark.unit
def test_read_error_is_treated_as_present() -> None:
    # A read blip must NOT trigger a spurious backfill — only a genuine empty does.
    reader = _FakeReader({"oi_change": 0}, errors={"oi_change"})
    assert detect_missing_eod_feeds(reader, ["oi_change"], TODAY) == []


@pytest.mark.unit
async def test_reconcile_no_gaps_makes_no_calls(monkeypatch) -> None:
    client = _FakeClient()
    monkeypatch.setattr(reconcile_module, "create_async_http_client", lambda **_k: client)
    reader = _FakeReader({"oi_change": 5, "iv_rank": 5, "historic_option_volume": 5})

    summary = await reconcile_eod_feeds(_settings(), reader, today=TODAY)

    assert summary["missing"] == [] and summary["triggered"] == []
    assert client.calls == []


@pytest.mark.unit
async def test_reconcile_triggers_backfill_for_missing(monkeypatch) -> None:
    client = _FakeClient()
    monkeypatch.setattr(reconcile_module, "create_async_http_client", lambda **_k: client)
    reader = _FakeReader({"oi_change": 0, "iv_rank": 9, "historic_option_volume": 0})

    summary = await reconcile_eod_feeds(_settings(), reader, today=TODAY)

    assert summary["triggered"] == ["oi_change", "historic_option_volume"]
    assert summary["failed"] == []
    assert [c["json"]["feed"] for c in client.calls] == ["oi_change", "historic_option_volume"]
    body = client.calls[0]["json"]
    assert body["provider"] == "unusual_whales"
    assert body["symbols"] == ["SPY", "AAPL"]
    assert body["start"] == "2026-06-25" and body["end"] == "2026-06-25"
    assert client.calls[0]["headers"]["X-Gateway-Key"] == "testkey"
    assert client.calls[0]["route"] == "/api/v1/backfill"


@pytest.mark.unit
async def test_reconcile_records_failed_without_raising(monkeypatch) -> None:
    client = _FakeClient(fail={"oi_change"})
    monkeypatch.setattr(reconcile_module, "create_async_http_client", lambda **_k: client)
    reader = _FakeReader({"oi_change": 0, "iv_rank": 5, "historic_option_volume": 0})

    summary = await reconcile_eod_feeds(_settings(), reader, today=TODAY)

    assert summary["failed"] == ["oi_change"]
    assert summary["triggered"] == ["historic_option_volume"]


@pytest.mark.unit
async def test_reconcile_no_symbols_skips_calls(monkeypatch) -> None:
    client = _FakeClient()
    monkeypatch.setattr(reconcile_module, "create_async_http_client", lambda **_k: client)
    reader = _FakeReader({"oi_change": 0, "iv_rank": 5, "historic_option_volume": 5})

    summary = await reconcile_eod_feeds(_settings(eod_reconcile_symbol_list=[]), reader, today=TODAY)

    assert summary["failed"] == ["oi_change"] and client.calls == []


class _FrozenDateTime(datetime):
    _frozen: datetime

    @classmethod
    def now(cls, tz=None):  # type: ignore[override]
        return cls._frozen if tz is None else cls._frozen.astimezone(tz)


def _poller(enabled: bool, last: date | None = None) -> GoldFeaturePoller:
    poller = GoldFeaturePoller.__new__(GoldFeaturePoller)
    poller._settings = SimpleNamespace(eod_reconcile_enabled=enabled, eod_reconcile_hour=17, eod_reconcile_minute=45)
    poller._last_reconcile_date = last
    return poller


@pytest.mark.unit
@pytest.mark.parametrize(
    "enabled,when,last,expected",
    [
        (False, datetime(2026, 6, 25, 18, 0, tzinfo=ET), None, False),  # disabled
        (True, datetime(2026, 6, 25, 17, 30, tzinfo=ET), None, False),  # before 17:45
        (True, datetime(2026, 6, 25, 17, 45, tzinfo=ET), None, True),  # at trigger
        (True, datetime(2026, 6, 25, 18, 0, tzinfo=ET), date(2026, 6, 25), False),  # already ran
        (True, datetime(2026, 6, 27, 18, 0, tzinfo=ET), None, False),  # Saturday, not trading
    ],
)
def test_should_reconcile_gating(monkeypatch, enabled, when, last, expected) -> None:
    _FrozenDateTime._frozen = when
    monkeypatch.setattr(service, "datetime", _FrozenDateTime)
    assert _poller(enabled, last)._should_reconcile() is expected
