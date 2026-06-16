# Heber Critical-Feed Discord Alarm Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fire one Discord alert the moment a must-flow lakehouse feed goes dark or drops to a trickle during the hours it should be flowing.

**Architecture:** Extend the existing `HealthMonitorService` with (1) a per-feed *liveness* check that compares recent Silver activity to an absolute floor (treating a missing partition as zero — the case the current volume check silently skips), and (2) a `DiscordNotifier` sink with severity gating, per-feed cooldown, and recovery notices. A new short-interval loop in the service runs the liveness check and routes its results to the notifier. Reads the filesystem (Silver) as source of truth so a dead consumer still surfaces "no new data."

**Tech Stack:** Python 3.12, Pydantic Settings, pyarrow/pandas (via `HeberReader`), httpx (Discord webhook), structlog, pytest (`asyncio_mode=auto`).

**Reference spec:** `docs/superpowers/specs/2026-06-08-heber-critical-feed-discord-alarm-design.md`

---

## Design decision reconciliation (read before starting)

The spec proposed wiring the notifier into the shared `_record_and_store` sink so *all* tiers alert. During planning we chose the **more surgical** route: **wire alerting into the new liveness loop only.** Rationale:
- The user's goal is feed-liveness alerting. If Redis/the consumer dies, feeds go dark, and the liveness check catches that downstream symptom anyway — so we lose no real coverage.
- It avoids touching the working tier1/tier2/tier3 loops (smaller blast radius, per "surgical changes").
- The `DiscordNotifier` still accepts a generic `list[CheckResult]`, so generalizing to other tiers later is trivial.

## File structure

| File | Responsibility | New/Modify |
|------|----------------|------------|
| `heber/config.py` | Add `HEBER_ALERT_*` settings fields | Modify |
| `heber/health_monitor/feed_registry.py` | `FeedRule` dataclass + `DEFAULT_REGISTRY` + `resolved_registry()` | Create |
| `heber/health_monitor/checks/liveness.py` | `run_liveness_checks()` — the detection logic | Create |
| `heber/ops/notifier.py` | `DiscordNotifier` — webhook send, severity gate, cooldown, recovery, state file | Create |
| `heber/health_monitor/service.py` | Liveness loop + notifier wiring | Modify |
| `heber/cli.py` | `alert-test` + `alert-calibrate` subcommands | Modify |
| `heber/ops/calibrate_floors.py` | Floor-calibration logic used by the CLI | Create |
| `tests/health_monitor/test_feed_registry.py` | Registry tests | Create |
| `tests/health_monitor/test_liveness.py` | Liveness check tests | Create |
| `tests/ops/test_notifier.py` | Notifier tests | Create |
| `tests/health_monitor/test_service_liveness.py` | Service wiring test | Create |
| `tests/test_cli_alert.py` | CLI command tests | Create |
| `CHANGELOG.md` | Changelog entry | Modify |
| `docs/operations/native-launchd.md` | Deployment note for alerting env vars | Modify |

---

## Task 1: Config — `HEBER_ALERT_*` settings

**Files:**
- Modify: `heber/config.py` (insert after the Health Monitor block, ~line 524)
- Test: `tests/health_monitor/test_alert_config.py`

- [ ] **Step 1: Write the failing test**

Create `tests/health_monitor/test_alert_config.py`:

```python
"""Tests for HEBER_ALERT_* settings."""

from __future__ import annotations

import pytest

from heber.config import Settings


@pytest.mark.unit
def test_alert_defaults() -> None:
    s = Settings()
    assert s.alert_discord_enabled is False
    assert s.alert_discord_webhook_url == ""
    assert s.alert_min_severity == "critical"
    assert s.alert_cooldown_seconds == 3600
    assert s.alert_send_recovery is True
    assert s.alert_liveness_check_interval_seconds == 300
    assert s.alert_floor_overrides == {}


@pytest.mark.unit
def test_alert_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HEBER_ALERT_DISCORD_ENABLED", "true")
    monkeypatch.setenv("HEBER_ALERT_DISCORD_WEBHOOK_URL", "https://discord.com/api/webhooks/1/abc")
    monkeypatch.setenv("HEBER_ALERT_FLOOR_OVERRIDES", '{"darkpool": 8, "flow_alerts": 25}')
    s = Settings()
    assert s.alert_discord_enabled is True
    assert s.alert_discord_webhook_url.endswith("/abc")
    assert s.alert_floor_overrides == {"darkpool": 8, "flow_alerts": 25}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/health_monitor/test_alert_config.py -v`
Expected: FAIL — `AttributeError: 'Settings' object has no attribute 'alert_discord_enabled'`

- [ ] **Step 3: Add the settings**

In `heber/config.py`, insert this block immediately after the `health_leakage_sample_size` field (after line 524, before `# Quarantine`):

```python
    # Critical-feed data-quality alerting (HEBER_ALERT_*)
    alert_discord_enabled: bool = Field(
        default=False,
        description="Enable Discord alerts for critical data-quality failures",
    )
    alert_discord_webhook_url: str = Field(
        default="",
        description="Discord webhook URL for critical data-quality alerts",
    )
    alert_min_severity: str = Field(
        default="critical",
        description="Minimum severity to send an alert (critical|warning|info)",
    )
    alert_cooldown_seconds: int = Field(
        default=3600,
        ge=0,
        description="Minimum seconds between repeat alerts for the same (check, feed)",
    )
    alert_send_recovery: bool = Field(
        default=True,
        description="Send a one-line recovery note when a previously-alerting feed returns to healthy",
    )
    alert_liveness_check_interval_seconds: int = Field(
        default=300,
        ge=30,
        le=3600,
        description="Interval (seconds) for the per-feed liveness loop",
    )
    alert_floor_overrides: dict[str, int] = Field(
        default_factory=dict,
        description="Per-feed floor overrides for liveness (JSON env); floor 0 disables that feed",
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/health_monitor/test_alert_config.py -v`
Expected: PASS (both tests)

- [ ] **Step 5: Commit**

```bash
git add heber/config.py tests/health_monitor/test_alert_config.py
git commit -m "feat(config): add HEBER_ALERT_* settings for critical-feed alerting"
```

---

## Task 2: Must-flow feed registry

**Files:**
- Create: `heber/health_monitor/feed_registry.py`
- Test: `tests/health_monitor/test_feed_registry.py`

- [ ] **Step 1: Write the failing test**

Create `tests/health_monitor/test_feed_registry.py`:

```python
"""Tests for the must-flow feed registry."""

from __future__ import annotations

import pytest

from heber.health_monitor.feed_registry import DEFAULT_REGISTRY, FeedRule, resolved_registry


@pytest.mark.unit
def test_default_registry_has_expected_feeds() -> None:
    feeds = {r.feed for r in DEFAULT_REGISTRY}
    assert feeds == {"flow_alerts", "darkpool", "bars", "trades", "oi_change", "greek_exposure"}


@pytest.mark.unit
def test_default_registry_kinds() -> None:
    by_feed = {r.feed: r for r in DEFAULT_REGISTRY}
    assert by_feed["flow_alerts"].kind == "continuous"
    assert by_feed["darkpool"].kind == "continuous"
    assert by_feed["oi_change"].kind == "daily"
    assert by_feed["greek_exposure"].kind == "daily"


@pytest.mark.unit
def test_floor_override_changes_floor() -> None:
    rules = resolved_registry({"darkpool": 8})
    darkpool = next(r for r in rules if r.feed == "darkpool")
    assert darkpool.floor == 8


@pytest.mark.unit
def test_floor_zero_disables_feed() -> None:
    rules = resolved_registry({"bars": 0})
    assert all(r.feed != "bars" for r in rules)


@pytest.mark.unit
def test_no_overrides_returns_defaults() -> None:
    assert resolved_registry({}) == list(DEFAULT_REGISTRY)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/health_monitor/test_feed_registry.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'heber.health_monitor.feed_registry'`

- [ ] **Step 3: Write the module**

Create `heber/health_monitor/feed_registry.py`:

```python
"""Must-flow feed registry for the critical-feed liveness alarm.

Two cadence classes:
  - continuous: rows must keep landing during an active ET window.
  - daily: today's partition must exist by an ET deadline.

Floors are absolute (not a rolling baseline) so a slowly-degrading or
chronically-low feed is still flagged. Override floors via
``HEBER_ALERT_FLOOR_OVERRIDES`` (JSON map feed -> floor); floor 0 disables a feed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class FeedRule:
    feed: str
    kind: Literal["continuous", "daily"]
    window_start_et: str  # "HH:MM" — active-window start (continuous); ignored for daily
    window_end_et: str    # "HH:MM" — active-window end (continuous) / deadline (daily)
    lookback_minutes: int  # sliding window for continuous; ignored for daily
    floor: int             # min rows required in the window (continuous) / by deadline (daily)


DEFAULT_REGISTRY: list[FeedRule] = [
    FeedRule("flow_alerts", "continuous", "09:30", "16:00", 60, 1),
    FeedRule("darkpool", "continuous", "04:00", "20:00", 60, 1),
    FeedRule("bars", "continuous", "09:30", "16:00", 30, 1),
    FeedRule("trades", "continuous", "09:30", "16:00", 30, 1),
    FeedRule("oi_change", "daily", "", "17:30", 0, 1),
    FeedRule("greek_exposure", "daily", "", "17:30", 0, 1),
]


def resolved_registry(floor_overrides: dict[str, int]) -> list[FeedRule]:
    """Apply per-feed floor overrides. Floor 0 disables a feed (drops it)."""
    rules: list[FeedRule] = []
    for rule in DEFAULT_REGISTRY:
        if rule.feed in floor_overrides:
            new_floor = floor_overrides[rule.feed]
            if new_floor <= 0:
                continue
            rule = FeedRule(
                rule.feed, rule.kind, rule.window_start_et, rule.window_end_et,
                rule.lookback_minutes, new_floor,
            )
        rules.append(rule)
    return rules
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/health_monitor/test_feed_registry.py -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add heber/health_monitor/feed_registry.py tests/health_monitor/test_feed_registry.py
git commit -m "feat(health): add must-flow feed registry with absolute floors"
```

---

## Task 3: Liveness check

**Files:**
- Create: `heber/health_monitor/checks/liveness.py`
- Test: `tests/health_monitor/test_liveness.py`

**Detection rules:**
- **continuous**, `now` (ET) within `[window_start, window_end]` on a trading day: count Silver rows with `ts_event` in the last `lookback_minutes` via `ctx.reader.read_silver(feed, time_range=(cutoff_utc, now_utc), columns=["ts_event"])`. `count < floor` → `FAIL`/`P0_CRITICAL`. A missing partition / read error / empty frame is treated as **0 rows** (not skipped).
- **daily**, `now` (ET) at/after `window_end` (deadline) on a trading day: count today's rows via `read_silver(feed, time_range=(midnight_utc, now_utc), columns=["ts_event"])`. `count < floor` → `FAIL`/`P0_CRITICAL`.
- A healthy in-scope feed → `PASS`/`P2_INFO`. Out of window / before deadline / non-trading day → **no result for that feed** (so the notifier's recovery logic isn't tripped by a feed merely leaving its window).
- Severity is **always `P0_CRITICAL`** on breach — do NOT call `calendar.adjust_severity` here (it downgrades to INFO outside market hours, which would suppress darkpool's after-hours window and the EOD daily checks).

- [ ] **Step 1: Write the failing tests**

Create `tests/health_monitor/test_liveness.py`:

```python
"""Tests for the per-feed liveness check."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from heber.health_monitor.checks.liveness import run_liveness_checks
from heber.health_monitor.models import Severity, Status
from tests.health_monitor.conftest import make_check_context

ET = ZoneInfo("America/New_York")
UTC = timezone.utc

# A trading weekday at 11:30 ET (inside the 09:30-16:00 continuous windows).
MIDDAY_ET = datetime(2026, 3, 25, 11, 30, tzinfo=ET)
# Same trading day at 18:00 ET (past the 17:30 daily deadline; outside RTH).
EVENING_ET = datetime(2026, 3, 25, 18, 0, tzinfo=ET)


def _reader_returning(counts: dict[str, int]) -> MagicMock:
    """A reader whose read_silver returns a ts_event frame of `counts[feed]` rows."""
    reader = MagicMock()

    def _read(dataset: str, time_range=None, columns=None, **_kw):
        n = counts.get(dataset, 0)
        ts = pd.Timestamp("2026-03-25T15:00:00Z")
        return pd.DataFrame({"ts_event": [ts] * n})

    reader.read_silver = MagicMock(side_effect=_read)
    return reader


def _ctx(tmp_path: Path, reader: MagicMock, overrides: dict | None = None):
    cal = MagicMock()
    cal.is_trading_day = MagicMock(return_value=True)
    settings_overrides = {"alert_floor_overrides": overrides or {}}
    return make_check_context(tmp_path, calendar=cal, reader=reader, settings_overrides=settings_overrides)


@pytest.mark.unit
async def test_continuous_feed_flowing_passes(tmp_path: Path) -> None:
    reader = _reader_returning({"flow_alerts": 50, "darkpool": 50, "bars": 50, "trades": 50})
    ctx = _ctx(tmp_path, reader)
    results = await run_liveness_checks(ctx, now=MIDDAY_ET)
    flow = [r for r in results if r.feed == "flow_alerts"]
    assert len(flow) == 1
    assert flow[0].status == Status.PASS


@pytest.mark.unit
async def test_continuous_feed_dark_fails_critical(tmp_path: Path) -> None:
    reader = _reader_returning({"flow_alerts": 0, "darkpool": 50, "bars": 50, "trades": 50})
    ctx = _ctx(tmp_path, reader)
    results = await run_liveness_checks(ctx, now=MIDDAY_ET)
    flow = [r for r in results if r.feed == "flow_alerts"]
    assert len(flow) == 1
    assert flow[0].status == Status.FAIL
    assert flow[0].severity == Severity.P0_CRITICAL
    assert "flow_alerts" in flow[0].message


@pytest.mark.unit
async def test_trickle_below_floor_fails(tmp_path: Path) -> None:
    # darkpool floor raised to 8 via override; only 3 rows in window -> FAIL.
    reader = _reader_returning({"flow_alerts": 50, "darkpool": 3, "bars": 50, "trades": 50})
    ctx = _ctx(tmp_path, reader, overrides={"darkpool": 8})
    results = await run_liveness_checks(ctx, now=MIDDAY_ET)
    dp = [r for r in results if r.feed == "darkpool"]
    assert len(dp) == 1
    assert dp[0].status == Status.FAIL
    assert dp[0].severity == Severity.P0_CRITICAL


@pytest.mark.unit
async def test_missing_partition_treated_as_zero(tmp_path: Path) -> None:
    reader = MagicMock()
    reader.read_silver = MagicMock(side_effect=FileNotFoundError("no partition"))
    ctx = _ctx(tmp_path, reader)
    results = await run_liveness_checks(ctx, now=MIDDAY_ET)
    flow = [r for r in results if r.feed == "flow_alerts"]
    assert len(flow) == 1
    assert flow[0].status == Status.FAIL  # NOT skipped


@pytest.mark.unit
async def test_daily_feed_present_before_deadline_no_result(tmp_path: Path) -> None:
    # At midday (before 17:30) daily feeds are not yet in scope -> no result.
    reader = _reader_returning({"flow_alerts": 50, "darkpool": 50, "bars": 50, "trades": 50})
    ctx = _ctx(tmp_path, reader)
    results = await run_liveness_checks(ctx, now=MIDDAY_ET)
    assert all(r.feed not in {"oi_change", "greek_exposure"} for r in results)


@pytest.mark.unit
async def test_daily_feed_missing_past_deadline_fails(tmp_path: Path) -> None:
    reader = _reader_returning({"oi_change": 0, "greek_exposure": 5})
    ctx = _ctx(tmp_path, reader)
    results = await run_liveness_checks(ctx, now=EVENING_ET)
    oi = [r for r in results if r.feed == "oi_change"]
    assert len(oi) == 1
    assert oi[0].status == Status.FAIL
    assert oi[0].severity == Severity.P0_CRITICAL
    gx = [r for r in results if r.feed == "greek_exposure"]
    assert gx[0].status == Status.PASS


@pytest.mark.unit
async def test_continuous_feed_outside_window_no_result(tmp_path: Path) -> None:
    # 18:00 ET is outside flow_alerts' 09:30-16:00 window -> no flow_alerts result.
    reader = _reader_returning({"flow_alerts": 0})
    ctx = _ctx(tmp_path, reader)
    results = await run_liveness_checks(ctx, now=EVENING_ET)
    assert all(r.feed != "flow_alerts" for r in results)


@pytest.mark.unit
async def test_non_trading_day_empty(tmp_path: Path) -> None:
    reader = _reader_returning({"flow_alerts": 0})
    cal = MagicMock()
    cal.is_trading_day = MagicMock(return_value=False)
    ctx = make_check_context(tmp_path, calendar=cal, reader=reader)
    results = await run_liveness_checks(ctx, now=MIDDAY_ET)
    assert results == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/health_monitor/test_liveness.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'heber.health_monitor.checks.liveness'`

- [ ] **Step 3: Write the liveness check**

Create `heber/health_monitor/checks/liveness.py`:

```python
"""Per-feed liveness check — the critical-feed alarm's detector.

Compares recent Silver activity to an absolute floor. A feed that produces no
partition (or errors on read) is treated as zero rows — the worst case — rather
than skipped.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import structlog

from heber.health_monitor.feed_registry import FeedRule, resolved_registry
from heber.health_monitor.models import CheckContext, CheckResult, Severity, Status

logger = structlog.get_logger(__name__)

ET = ZoneInfo("America/New_York")
UTC = timezone.utc
CHECK_NAME = "feed_liveness"


def _parse_hhmm(value: str) -> time:
    hh, mm = value.split(":")
    return time(int(hh), int(mm))


def _window_row_count(ctx: CheckContext, feed: str, start_utc: datetime, end_utc: datetime) -> int:
    """Count Silver rows for `feed` with ts_event in [start_utc, end_utc].

    Missing partition / read error / empty frame -> 0 (the worst case).
    """
    try:
        df = ctx.reader.read_silver(
            feed,
            time_range=(start_utc.isoformat(), end_utc.isoformat()),
            columns=["ts_event"],
        )
    except Exception:
        logger.warning("liveness_read_error", feed=feed, exc_info=True)
        return 0
    if df is None or df.empty:
        return 0
    return int(len(df))


def _result(feed: str, status: Status, message: str, now: datetime, details: dict) -> CheckResult:
    severity = Severity.P2_INFO if status == Status.PASS else Severity.P0_CRITICAL
    return CheckResult(
        check_name=CHECK_NAME,
        feed=feed,
        severity=severity,
        status=status,
        message=message,
        details=details,
        ts_checked=now,
    )


def _check_continuous(ctx: CheckContext, rule: FeedRule, now_et: datetime) -> CheckResult | None:
    start = _parse_hhmm(rule.window_start_et)
    end = _parse_hhmm(rule.window_end_et)
    if not (start <= now_et.time() <= end):
        return None  # out of window -> no result

    now_utc = now_et.astimezone(UTC)
    cutoff_utc = now_utc - timedelta(minutes=rule.lookback_minutes)
    count = _window_row_count(ctx, rule.feed, cutoff_utc, now_utc)
    details = {
        "feed": rule.feed,
        "rows": count,
        "floor": rule.floor,
        "lookback_minutes": rule.lookback_minutes,
    }
    if count < rule.floor:
        msg = (
            f"{rule.feed}: {count} rows in last {rule.lookback_minutes}m "
            f"(floor {rule.floor}) — feed appears dark/degraded"
        )
        return _result(rule.feed, Status.FAIL, msg, now_et, details)
    msg = f"{rule.feed}: {count} rows in last {rule.lookback_minutes}m (floor {rule.floor})"
    return _result(rule.feed, Status.PASS, msg, now_et, details)


def _check_daily(ctx: CheckContext, rule: FeedRule, now_et: datetime) -> CheckResult | None:
    deadline = _parse_hhmm(rule.window_end_et)
    if now_et.time() < deadline:
        return None  # before deadline -> not yet in scope

    now_utc = now_et.astimezone(UTC)
    midnight_utc = now_et.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(UTC)
    count = _window_row_count(ctx, rule.feed, midnight_utc, now_utc)
    details = {"feed": rule.feed, "rows": count, "floor": rule.floor, "deadline_et": rule.window_end_et}
    if count < rule.floor:
        msg = (
            f"{rule.feed}: {count} rows today by {rule.window_end_et} ET "
            f"(floor {rule.floor}) — EOD feed missing"
        )
        return _result(rule.feed, Status.FAIL, msg, now_et, details)
    msg = f"{rule.feed}: {count} rows today (floor {rule.floor})"
    return _result(rule.feed, Status.PASS, msg, now_et, details)


async def run_liveness_checks(ctx: CheckContext, now: datetime) -> list[CheckResult]:
    """Run per-feed liveness checks. `now` must be a tz-aware datetime."""
    now_et = now.astimezone(ET)
    today = now_et.date()
    if not ctx.calendar.is_trading_day(today):
        return []

    results: list[CheckResult] = []
    for rule in resolved_registry(ctx.settings.alert_floor_overrides):
        if rule.kind == "continuous":
            res = _check_continuous(ctx, rule, now_et)
        else:
            res = _check_daily(ctx, rule, now_et)
        if res is not None:
            results.append(res)
    return results
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/health_monitor/test_liveness.py -v`
Expected: PASS (8 tests)

- [ ] **Step 5: Commit**

```bash
git add heber/health_monitor/checks/liveness.py tests/health_monitor/test_liveness.py
git commit -m "feat(health): add per-feed liveness check with absolute floors"
```

---

## Task 4: Discord notifier

**Files:**
- Create: `heber/ops/notifier.py`
- Test: `tests/ops/test_notifier.py`

**Behavior:**
- `dispatch(results, now=None)`: for each result, alert if `status in (FAIL, ERROR)` and severity ≥ `min_severity`; throttle per `(check_name, feed)` key by `cooldown_seconds`, but a **status change** (was-PASS/unseen → FAIL) bypasses the cooldown. On a `PASS` for a key that previously alerted, send a recovery note (if enabled) and clear the key.
- State persisted to `${data_root}/ops/alerts/state.json` as `{ "check|feed": {"last_sent_ts": iso, "last_status": "fail"} }`.
- All network/IO errors are caught and logged; `dispatch` never raises.
- Increments `heber_alerts_sent_total{check_name, outcome}`.

- [ ] **Step 1: Write the failing tests**

Create `tests/ops/test_notifier.py` (create `tests/ops/__init__.py` too if `tests/ops/` does not exist):

```python
"""Tests for the Discord critical-alert notifier."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from heber.config import Settings
from heber.health_monitor.models import CheckResult, Severity, Status
from heber.ops.notifier import DiscordNotifier

UTC = timezone.utc
T0 = datetime(2026, 3, 25, 15, 0, tzinfo=UTC)


def _settings(tmp_path: Path, **overrides) -> Settings:
    base = {
        "data_root": str(tmp_path),
        "alert_discord_enabled": True,
        "alert_discord_webhook_url": "https://discord.com/api/webhooks/1/abc",
    }
    base.update(overrides)
    return Settings(**base)


def _fail(feed: str = "flow_alerts") -> CheckResult:
    return CheckResult(
        check_name="feed_liveness", feed=feed, severity=Severity.P0_CRITICAL,
        status=Status.FAIL, message=f"{feed} dark", details={}, ts_checked=T0,
    )


def _passing(feed: str = "flow_alerts") -> CheckResult:
    return CheckResult(
        check_name="feed_liveness", feed=feed, severity=Severity.P2_INFO,
        status=Status.PASS, message=f"{feed} ok", details={}, ts_checked=T0,
    )


def _client() -> MagicMock:
    client = MagicMock()
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    client.post = MagicMock(return_value=resp)
    return client


@pytest.mark.unit
def test_critical_sends_once(tmp_path: Path) -> None:
    client = _client()
    n = DiscordNotifier(_settings(tmp_path), client=client)
    n.dispatch([_fail()], now=T0)
    assert client.post.call_count == 1


@pytest.mark.unit
def test_repeat_within_cooldown_suppressed(tmp_path: Path) -> None:
    client = _client()
    n = DiscordNotifier(_settings(tmp_path, alert_cooldown_seconds=3600), client=client)
    n.dispatch([_fail()], now=T0)
    n.dispatch([_fail()], now=T0 + timedelta(minutes=10))
    assert client.post.call_count == 1


@pytest.mark.unit
def test_repeat_after_cooldown_resends(tmp_path: Path) -> None:
    client = _client()
    n = DiscordNotifier(_settings(tmp_path, alert_cooldown_seconds=3600), client=client)
    n.dispatch([_fail()], now=T0)
    n.dispatch([_fail()], now=T0 + timedelta(hours=2))
    assert client.post.call_count == 2


@pytest.mark.unit
def test_recovery_sent_after_fail(tmp_path: Path) -> None:
    client = _client()
    n = DiscordNotifier(_settings(tmp_path), client=client)
    n.dispatch([_fail()], now=T0)
    n.dispatch([_passing()], now=T0 + timedelta(minutes=10))
    assert client.post.call_count == 2
    recovery_call = client.post.call_args_list[1]
    assert "recover" in recovery_call.kwargs["json"]["content"].lower()


@pytest.mark.unit
def test_pass_without_prior_fail_is_silent(tmp_path: Path) -> None:
    client = _client()
    n = DiscordNotifier(_settings(tmp_path), client=client)
    n.dispatch([_passing()], now=T0)
    assert client.post.call_count == 0


@pytest.mark.unit
def test_warning_below_min_severity_not_sent(tmp_path: Path) -> None:
    client = _client()
    n = DiscordNotifier(_settings(tmp_path), client=client)
    warn = CheckResult(
        check_name="x", feed="f", severity=Severity.P1_WARNING, status=Status.WARN,
        message="m", details={}, ts_checked=T0,
    )
    n.dispatch([warn], now=T0)
    assert client.post.call_count == 0


@pytest.mark.unit
def test_disabled_notifier_sends_nothing(tmp_path: Path) -> None:
    client = _client()
    n = DiscordNotifier(_settings(tmp_path, alert_discord_enabled=False), client=client)
    n.dispatch([_fail()], now=T0)
    assert client.post.call_count == 0


@pytest.mark.unit
def test_post_failure_is_swallowed(tmp_path: Path) -> None:
    client = MagicMock()
    client.post = MagicMock(side_effect=OSError("network down"))
    n = DiscordNotifier(_settings(tmp_path), client=client)
    n.dispatch([_fail()], now=T0)  # must not raise


@pytest.mark.unit
def test_state_persists_across_instances(tmp_path: Path) -> None:
    settings = _settings(tmp_path, alert_cooldown_seconds=3600)
    n1 = DiscordNotifier(settings, client=_client())
    n1.dispatch([_fail()], now=T0)
    client2 = _client()
    n2 = DiscordNotifier(settings, client=client2)
    n2.dispatch([_fail()], now=T0 + timedelta(minutes=10))  # still within cooldown
    assert client2.post.call_count == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/ops/test_notifier.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'heber.ops.notifier'`

- [ ] **Step 3: Write the notifier**

Create `heber/ops/notifier.py`:

```python
"""Discord notifier for critical data-quality alerts.

Severity-gated, per-(check, feed) cooldown with status-change override, and a
recovery note when a previously-alerting feed returns to healthy. Network and IO
errors are swallowed (logged) so a broken webhook never crashes the monitor.
State persists to ``${data_root}/ops/alerts/state.json``.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import structlog
from prometheus_client import Counter

from heber.config import Settings
from heber.health_monitor.models import CheckResult, Severity, Status
from heber.ops.metrics import _get_or_create

logger = structlog.get_logger(__name__)
UTC = timezone.utc

# _get_or_create takes the Prometheus class first (see heber/ops/metrics.py:17 and
# heber/health_monitor/metrics.py for the calling convention).
alerts_sent_total = _get_or_create(
    Counter,
    "heber_alerts_sent_total",
    "Critical data-quality alerts sent",
    ["check_name", "outcome"],
)


def _severity_meets(sev: Severity, minimum: Severity) -> bool:
    return sev == minimum or sev.is_more_severe_than(minimum)


class DiscordNotifier:
    def __init__(
        self,
        settings: Settings,
        *,
        client: httpx.Client | None = None,
        state_path: Path | None = None,
    ) -> None:
        self._enabled = settings.alert_discord_enabled
        self._webhook = settings.alert_discord_webhook_url
        self._min_severity = Severity(settings.alert_min_severity)
        self._cooldown = settings.alert_cooldown_seconds
        self._send_recovery = settings.alert_send_recovery
        self._client = client
        self._state_path = state_path or (Path(settings.data_root) / "ops" / "alerts" / "state.json")
        self._state: dict[str, dict[str, Any]] = self._load_state()

    # ----- state -----
    def _load_state(self) -> dict[str, dict[str, Any]]:
        try:
            return json.loads(self._state_path.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _save_state(self) -> None:
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            self._state_path.write_text(json.dumps(self._state, default=str))
        except OSError:
            logger.warning("alert_state_save_failed", path=str(self._state_path), exc_info=True)

    # ----- dispatch -----
    def dispatch(self, results: list[CheckResult], now: datetime | None = None) -> None:
        if not self._enabled or not self._webhook:
            return
        now = now or datetime.now(UTC)
        changed = False
        for r in results:
            key = f"{r.check_name}|{r.feed or ''}"
            if r.status in (Status.FAIL, Status.ERROR) and _severity_meets(r.severity, self._min_severity):
                if self._should_alert(key, now):
                    if self._post(f"🚨 CRITICAL — {r.message}", r.check_name):
                        self._state[key] = {"last_sent_ts": now.isoformat(), "last_status": "fail"}
                        changed = True
            elif r.status == Status.PASS:
                prev = self._state.get(key)
                if prev and prev.get("last_status") == "fail":
                    if self._send_recovery:
                        self._post(f"✅ RECOVERED — {r.message}", r.check_name)
                    self._state.pop(key, None)
                    changed = True
        if changed:
            self._save_state()

    def _should_alert(self, key: str, now: datetime) -> bool:
        prev = self._state.get(key)
        if prev is None or prev.get("last_status") != "fail":
            return True  # new or status changed -> alert immediately
        try:
            last = datetime.fromisoformat(prev["last_sent_ts"])
        except (KeyError, ValueError):
            return True
        return (now - last).total_seconds() >= self._cooldown

    def _post(self, content: str, check_name: str) -> bool:
        client = self._client or httpx.Client(timeout=10.0)
        try:
            resp = client.post(self._webhook, json={"content": content})
            resp.raise_for_status()
            alerts_sent_total.labels(check_name=check_name, outcome="sent").inc()
            logger.info("alert_sent", check_name=check_name, content=content)
            return True
        except (httpx.HTTPError, OSError, TypeError, ValueError):
            alerts_sent_total.labels(check_name=check_name, outcome="error").inc()
            logger.error("alert_send_failed", check_name=check_name, exc_info=True)
            return False
        finally:
            if self._client is None:
                client.close()

    def send_test(self, text: str) -> bool:
        """Send an ad-hoc test message (used by `heber alert-test`)."""
        if not self._webhook:
            logger.error("alert_test_no_webhook")
            return False
        return self._post(text, "alert_test")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/ops/test_notifier.py -v`
Expected: PASS (9 tests)

- [ ] **Step 5: Commit**

```bash
git add heber/ops/notifier.py tests/ops/__init__.py tests/ops/test_notifier.py
git commit -m "feat(ops): add Discord notifier with cooldown and recovery notices"
```

---

## Task 5: Wire liveness loop + notifier into the service

**Files:**
- Modify: `heber/health_monitor/service.py`
- Test: `tests/health_monitor/test_service_liveness.py`

- [ ] **Step 1: Write the failing test**

Create `tests/health_monitor/test_service_liveness.py`:

```python
"""Tests for liveness-loop + notifier wiring in HealthMonitorService."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from heber.health_monitor.models import CheckResult, Severity, Status
from heber.health_monitor.service import HealthMonitorService

UTC = timezone.utc


@pytest.mark.unit
async def test_maybe_alert_dispatches_results() -> None:
    svc = HealthMonitorService()
    svc._notifier = MagicMock()
    svc._notifier.dispatch = MagicMock()
    results = [
        CheckResult(
            check_name="feed_liveness", feed="flow_alerts", severity=Severity.P0_CRITICAL,
            status=Status.FAIL, message="dark", details={}, ts_checked=datetime.now(UTC),
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/health_monitor/test_service_liveness.py -v`
Expected: FAIL — `AttributeError: 'HealthMonitorService' object has no attribute '_maybe_alert'`

- [ ] **Step 3: Add notifier + liveness loop to the service**

In `heber/health_monitor/service.py`:

(a) Add to imports near the top:

```python
import asyncio
```
(already imported — confirm) and add:
```python
from heber.ops.notifier import DiscordNotifier
```

(b) In `__init__`, after `self._last_tier3_date: date | None = None`, add:

```python
        self._notifier: DiscordNotifier | None = None
        self._last_alert_ts: datetime | None = None
```

(c) In `start()`, immediately before `self._running = True`, add:

```python
        if self._settings.alert_discord_enabled:
            self._notifier = DiscordNotifier(self._settings)
```

(d) In `start()`, add a fourth task to the `self._tasks = [...]` list:

```python
            asyncio.create_task(self._liveness_loop(), name="health-liveness"),
```

(e) Add the loop and helper as new methods (place after `_tier3_loop` / `_should_run_tier3`):

```python
    # ----- Liveness: per-feed critical alarm (short interval, market days) -----

    async def _liveness_loop(self) -> None:
        """Run per-feed liveness checks and route criticals to Discord."""
        from heber.health_monitor.checks.liveness import run_liveness_checks

        interval = self._settings.alert_liveness_check_interval_seconds
        assert self._ctx is not None

        while self._running:
            try:
                now_et = datetime.now(ET)
                if self._calendar.is_trading_day(now_et.date()):
                    t0 = time.monotonic()
                    results = await run_liveness_checks(self._ctx, now=now_et)
                    elapsed = time.monotonic() - t0
                    self._record_and_store(results, elapsed, "liveness")
                    await self._maybe_alert(results)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("health_liveness_error", exc_info=True)

            await asyncio.sleep(interval)

    async def _maybe_alert(self, results: list[CheckResult]) -> None:
        """Dispatch results to the Discord notifier off the event loop."""
        if self._notifier is None:
            return
        try:
            await asyncio.to_thread(self._notifier.dispatch, results)
            self._last_alert_ts = datetime.now(ET)
        except Exception:
            logger.error("health_alert_dispatch_error", exc_info=True)
```

(f) Add to `get_runtime_snapshot()`'s returned dict:

```python
            "alerting_enabled": self._notifier is not None,
            "last_alert_ts": self._last_alert_ts.isoformat() if self._last_alert_ts else None,
```

- [ ] **Step 4: Run the new test + the full service test file**

Run: `uv run pytest tests/health_monitor/test_service_liveness.py tests/health_monitor/test_service.py -v`
Expected: PASS (new tests pass; existing service tests still pass)

- [ ] **Step 5: Commit**

```bash
git add heber/health_monitor/service.py tests/health_monitor/test_service_liveness.py
git commit -m "feat(health): run liveness loop and dispatch critical alerts to Discord"
```

---

## Task 6: CLI `alert-test`

**Files:**
- Modify: `heber/cli.py`
- Test: `tests/test_cli_alert.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_cli_alert.py`:

```python
"""Tests for `heber alert-test` and `heber alert-calibrate` CLI commands."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from heber.cli import _cmd_alert_test


@pytest.mark.unit
def test_alert_test_sends_message() -> None:
    fake_notifier = MagicMock()
    fake_notifier.send_test = MagicMock(return_value=True)
    with patch("heber.cli.DiscordNotifier", return_value=fake_notifier):
        rc = _cmd_alert_test(SimpleNamespace(message=None))
    assert rc == 0
    fake_notifier.send_test.assert_called_once()


@pytest.mark.unit
def test_alert_test_reports_failure() -> None:
    fake_notifier = MagicMock()
    fake_notifier.send_test = MagicMock(return_value=False)
    with patch("heber.cli.DiscordNotifier", return_value=fake_notifier):
        rc = _cmd_alert_test(SimpleNamespace(message="hi"))
    assert rc == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_cli_alert.py::test_alert_test_sends_message -v`
Expected: FAIL — `ImportError: cannot import name '_cmd_alert_test' from 'heber.cli'`

- [ ] **Step 3: Implement the command**

In `heber/cli.py`:

(a) Add near the other imports (top of file):

```python
from heber.ops.notifier import DiscordNotifier
```

(b) Add the handler function (near the other `_cmd_*` functions):

```python
def _cmd_alert_test(args: argparse.Namespace) -> int:
    """Send a test message through the Discord notifier."""
    from heber.config import get_settings

    settings = get_settings()
    notifier = DiscordNotifier(settings)
    text = args.message or "✅ Heber alert-test — data-quality alerting is wired up."
    ok = notifier.send_test(text)
    print("Sent." if ok else "Failed to send (check HEBER_ALERT_DISCORD_* settings).")
    return 0 if ok else 1
```

(c) Register the subcommand in `main()` (after the `health-daily` parser, before `args = parser.parse_args()`):

```python
    # Alert test command
    alert_test_parser = subparsers.add_parser("alert-test", help="Send a test Discord alert")
    alert_test_parser.add_argument("--message", help="Custom message text", default=None)
```

(d) Add to the `_SUBCOMMAND_HANDLERS` dict:

```python
    "alert-test": _cmd_alert_test,
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_cli_alert.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add heber/cli.py tests/test_cli_alert.py
git commit -m "feat(cli): add `heber alert-test` to verify the Discord webhook"
```

---

## Task 7: Floor calibration (`heber alert-calibrate`)

**Files:**
- Create: `heber/ops/calibrate_floors.py`
- Modify: `heber/cli.py`
- Test: `tests/ops/test_calibrate_floors.py`

**What it does:** For each *continuous* feed in the registry, read a healthy reference day (the most recent trading day at least `--days-back` days ago that has data), bin its active-window `ts_event`s into `lookback_minutes` buckets, take the median bucket count, and suggest `floor = max(1, int(ratio * median))`. Prints a ready-to-paste `HEBER_ALERT_FLOOR_OVERRIDES` JSON. Read-only.

- [ ] **Step 1: Write the failing test**

Create `tests/ops/test_calibrate_floors.py`:

```python
"""Tests for floor calibration."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from heber.ops.calibrate_floors import suggest_floor_from_counts

UTC = timezone.utc


@pytest.mark.unit
def test_suggest_floor_is_ratio_of_median() -> None:
    # Buckets of 100, 200, 300 -> median 200 -> 30% -> 60.
    counts = [100, 200, 300]
    assert suggest_floor_from_counts(counts, ratio=0.3) == 60


@pytest.mark.unit
def test_suggest_floor_minimum_one() -> None:
    assert suggest_floor_from_counts([0, 1, 0], ratio=0.3) == 1


@pytest.mark.unit
def test_suggest_floor_empty_returns_one() -> None:
    assert suggest_floor_from_counts([], ratio=0.3) == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/ops/test_calibrate_floors.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'heber.ops.calibrate_floors'`

- [ ] **Step 3: Implement the calibration helper**

Create `heber/ops/calibrate_floors.py`:

```python
"""Suggest per-feed liveness floors from a healthy historical window."""

from __future__ import annotations

import json
import statistics
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import structlog

from heber.health_monitor.feed_registry import DEFAULT_REGISTRY
from heber.reader import HeberReader

logger = structlog.get_logger(__name__)
ET = ZoneInfo("America/New_York")
UTC = timezone.utc


def suggest_floor_from_counts(bucket_counts: list[int], ratio: float = 0.3) -> int:
    """Suggested floor = max(1, int(ratio * median(bucket_counts)))."""
    if not bucket_counts:
        return 1
    median = statistics.median(bucket_counts)
    return max(1, int(ratio * median))


def _bucket_counts(ts_series, ref_day: date, rule, lookback: int) -> list[int]:
    """Count ts_event rows in consecutive `lookback`-minute buckets over the window."""
    start_t = time(*[int(x) for x in rule.window_start_et.split(":")])
    end_t = time(*[int(x) for x in rule.window_end_et.split(":")])
    counts: list[int] = []
    cursor = datetime.combine(ref_day, start_t, tzinfo=ET)
    window_end = datetime.combine(ref_day, end_t, tzinfo=ET)
    ts_utc = ts_series.dt.tz_convert("UTC")
    while cursor < window_end:
        nxt = cursor + timedelta(minutes=lookback)
        lo, hi = cursor.astimezone(UTC), nxt.astimezone(UTC)
        counts.append(int(((ts_utc >= lo) & (ts_utc < hi)).sum()))
        cursor = nxt
    return counts


def calibrate(days_back: int = 50, ratio: float = 0.3, reader: HeberReader | None = None) -> dict[str, int]:
    """Return suggested floors for continuous feeds keyed by feed name."""
    reader = reader or HeberReader()
    ref_day = (datetime.now(ET) - timedelta(days=days_back)).date()
    suggestions: dict[str, int] = {}
    for rule in DEFAULT_REGISTRY:
        if rule.kind != "continuous":
            continue
        try:
            df = reader.read_silver(
                rule.feed,
                time_range=(f"{ref_day.isoformat()}T00:00:00+00:00",
                            f"{ref_day.isoformat()}T23:59:59+00:00"),
                columns=["ts_event"],
            )
        except Exception:
            logger.warning("calibrate_read_error", feed=rule.feed, exc_info=True)
            df = None
        if df is None or df.empty:
            suggestions[rule.feed] = 1
            continue
        counts = _bucket_counts(df["ts_event"], ref_day, rule, rule.lookback_minutes)
        suggestions[rule.feed] = suggest_floor_from_counts(counts, ratio=ratio)
    return suggestions


def calibrate_json(days_back: int = 50, ratio: float = 0.3) -> str:
    return json.dumps(calibrate(days_back=days_back, ratio=ratio))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/ops/test_calibrate_floors.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Wire the CLI command**

In `heber/cli.py`:

(a) Add the handler:

```python
def _cmd_alert_calibrate(args: argparse.Namespace) -> int:
    """Print suggested per-feed liveness floors from healthy history."""
    from heber.ops.calibrate_floors import calibrate_json

    print("Suggested HEBER_ALERT_FLOOR_OVERRIDES (paste into .env):")
    print(calibrate_json(days_back=args.days_back, ratio=args.ratio))
    return 0
```

(b) Register the subcommand in `main()`:

```python
    alert_cal_parser = subparsers.add_parser("alert-calibrate", help="Suggest liveness floors from history")
    alert_cal_parser.add_argument("--days-back", type=int, default=50, help="Reference day age in days")
    alert_cal_parser.add_argument("--ratio", type=float, default=0.3, help="Floor as fraction of median rate")
```

(c) Add to `_SUBCOMMAND_HANDLERS`:

```python
    "alert-calibrate": _cmd_alert_calibrate,
```

- [ ] **Step 6: Run the CLI test file (smoke) + commit**

Run: `uv run pytest tests/ops/test_calibrate_floors.py tests/test_cli_alert.py -v`
Expected: PASS

```bash
git add heber/ops/calibrate_floors.py heber/cli.py tests/ops/test_calibrate_floors.py
git commit -m "feat(cli): add `heber alert-calibrate` to suggest liveness floors"
```

---

## Task 8: Docs, changelog, deployment note

**Files:**
- Modify: `CHANGELOG.md`
- Modify: `docs/operations/native-launchd.md`

- [ ] **Step 1: Add a CHANGELOG entry**

In `CHANGELOG.md`, under `## [Unreleased]` → `### Added`:

```markdown
- Critical-feed data-quality alarm: the health monitor now sends a Discord alert
  the moment a must-flow feed (flow_alerts, darkpool, bars, trades, oi_change,
  greek_exposure) goes dark or drops to a trickle during the hours it should be
  flowing. Configured via `HEBER_ALERT_*`. New CLI: `heber alert-test` (verify the
  webhook) and `heber alert-calibrate` (suggest per-feed floors from history).
```

- [ ] **Step 2: Add a deployment note**

In `docs/operations/native-launchd.md`, add a section:

```markdown
## Critical-feed Discord alerting

The `health-monitor` service sends Discord alerts when a must-flow feed stops or
trickles. The runner sources `.env`, so set these there (no plist edit needed):

```
HEBER_ALERT_DISCORD_ENABLED=true
HEBER_ALERT_DISCORD_WEBHOOK_URL=<discord webhook>
# Optional, after calibration:
HEBER_ALERT_FLOOR_OVERRIDES={"darkpool": 8, "flow_alerts": 25}
```

Verify end-to-end: `uv run heber alert-test`.
Suggest floors from healthy history: `uv run heber alert-calibrate`.
Requires `HEBER_HEALTH_MONITOR_ENABLED=true` (default).
```

- [ ] **Step 3: Run the full test suite for the new code**

Run: `uv run pytest tests/health_monitor/test_alert_config.py tests/health_monitor/test_feed_registry.py tests/health_monitor/test_liveness.py tests/ops/ tests/health_monitor/test_service_liveness.py tests/test_cli_alert.py -v`
Expected: PASS (all)

- [ ] **Step 4: Lint**

Run: `ruff check heber/health_monitor/feed_registry.py heber/health_monitor/checks/liveness.py heber/ops/notifier.py heber/ops/calibrate_floors.py heber/health_monitor/service.py heber/cli.py heber/config.py`
Expected: no errors (fix any that appear)

- [ ] **Step 5: Commit**

```bash
git add CHANGELOG.md docs/operations/native-launchd.md
git commit -m "docs: document critical-feed Discord alerting and deployment"
```

---

## Final verification

- [ ] Run the complete new-feature test set (all green):

```bash
uv run pytest tests/health_monitor/test_alert_config.py \
  tests/health_monitor/test_feed_registry.py \
  tests/health_monitor/test_liveness.py \
  tests/ops/test_notifier.py \
  tests/ops/test_calibrate_floors.py \
  tests/health_monitor/test_service_liveness.py \
  tests/test_cli_alert.py -v
```

- [ ] Run the existing health-monitor suite to confirm no regressions:

```bash
uv run pytest tests/health_monitor -v
```

- [ ] Manual smoke (requires the 3Roses webhook already in `.env`):

```bash
uv run heber alert-test
```
Expected: a message appears in the Discord channel; command prints `Sent.`

---

## Self-review notes (author)

- **Spec coverage:** notifier sink (Task 4), liveness with missing-partition-as-zero + absolute floors (Task 3), feed registry/two cadences (Task 2), config (Task 1), service wiring (Task 5), calibration for trickle (Task 7), `alert-test` (Task 6), deployment+docs (Task 8). Recovery notes (Task 4). All spec sections map to a task.
- **Deviation from spec:** alerting wired into the new liveness loop only, not the shared `_record_and_store` sink (documented in "Design decision reconciliation"). Reduces blast radius; no real coverage lost.
- **Type consistency:** `CheckResult`/`Severity`/`Status` used exactly as defined in `heber/health_monitor/models.py`; `read_silver(dataset, time_range, columns)` matches `heber/reader/core.py`; `_get_or_create(Counter, ...)` matches `heber/health_monitor/metrics.py`; calendar `is_trading_day(date)` matches `HealthCalendar`.
- **Known follow-ups (out of scope):** upstream Data-Gateway root cause; generalizing alerting to all tiers; keying liveness off write/availability time instead of `ts_event` if late-but-old-timestamped delivery ever becomes an issue.
