# Heber Critical-Feed Discord Alarm — Design

**Date:** 2026-06-08
**Status:** Approved (design); pending implementation plan
**Author:** Claude (brainstormed with Jacob)
**Repo:** Heber

## Problem

Several Unusual Whales feeds that should land in the lakehouse all day stopped
storing properly and went unnoticed for weeks, breaking downstream trading
systems. Confirmed on disk (`/Volumes/heber/data`, 2026-06-08):

| Feed | Observed | Failure shape |
|------|----------|---------------|
| `oi_change` | no partition after 2026-06-05 | hard stop |
| `darkpool` | 1–5 raw files/day, constant ~256 KB Silver | chronic trickle/burst |
| `flow_alerts` | volume cliff ~2026-06-05 (8 raw files on 06-08 vs 80–120 healthy) | degradation |
| `greek_exposure`, `bars`, `quotes`, `trades` | continuous | healthy |

**Why nothing warned us.** Heber already has a `HealthMonitorService` with a
volume-trending check, a results store, metrics, and a launchd-wired
entrypoint — but:

1. **No notification sink.** Check results are only logged and written to a
   Parquet file in Gold. `heber/ops/alerting.py` only emits Prometheus YAML
   rules (assumes an external Alertmanager we don't run). Nothing pushes to
   Discord/Slack/etc.
2. **The checks would have missed these failures anyway:**
   - A feed that stops producing a partition is **silently skipped**
     (`heber/health_monitor/checks/volume.py:80` — `if not dt_dir.exists(): continue`).
     The single most important case ("feed went dark") is never flagged.
   - **Boiling-frog baseline.** The volume check compares against the median of
     the *trailing 5 days*. A feed that degrades slowly (flow_alerts) or has
     been chronically low for weeks (darkpool) drags its own baseline down, so
     the ratio stays ~1.0 and never trips.

## Goal

The moment a must-flow feed goes dark **or drops to a trickle** during the hours
it should be flowing, send **one** Discord alert (not a storm) with enough
detail to act. Catch both failure shapes: hard-stop (`oi_change`) and
burst/trickle (`darkpool`, degraded `flow_alerts`).

## Non-goals

- No daily "all healthy" digest (user chose immediate-critical-only).
- No upstream Data-Gateway fix (separate fast-follow; root-cause leads documented
  below).
- No Prometheus/Alertmanager/Grafana stack.
- No backfill of already-lost data.
- No changes to the ingest hot path (consumer/writer).

## Decisions (locked during brainstorming)

- **Scope:** monitor + alerting only.
- **Cadence:** immediate critical only — no routine digest.
- **Placement:** extend the existing `HealthMonitorService` (Approach A), not a
  standalone service.
- **Alert channel:** reuse the existing 3Roses Discord webhook URL (pasted into
  Heber's `.env` as `HEBER_ALERT_DISCORD_WEBHOOK_URL`; Heber config stays
  self-contained — no cross-repo `.env` reading).
- **Feed set (v1):** continuous = `flow_alerts`, `darkpool`, `bars`, `trades`;
  daily/EOD = `oi_change`, `greek_exposure`.

## Architecture

Build on `heber/health_monitor/service.py` (`HealthMonitorService`) — already
market-hours-aware, launchd-wired (`launchd/com.empire.heber.health-monitor.plist`
→ `scripts/run_native_heber_service.sh health-monitor` → `python -m heber.health_monitor`),
with a calendar, a results store, and metrics.

```
HealthMonitorService  (already runs under launchd)
  ├─ NEW  checks/liveness.py     → per-feed "is it flowing now?" → CheckResult(CRITICAL) on breach
  │                                runs on a dedicated short loop (~5 min) during market hours
  ├─ existing tiers (stream/volume/schema/...) also produce CheckResults
  └─ NEW  notifier dispatch in _record_and_store()
            └─ NEW ops/notifier.py (DiscordNotifier) → severity gate + throttle/dedup → webhook POST
```

Because dispatch sits at the shared result sink (`_record_and_store`), **every**
tier's criticals (stream-down, DLQ-growing, etc.) also reach Discord for free.
The new liveness check is the one that catches the feed-outage class.

### Detection primitive

Use **recent per-feed activity vs. an absolute floor** — not a rolling baseline.
This (a) sidesteps the boiling-frog problem and (b) catches both hard-stop and
trickle. Read the **filesystem (Silver) as source of truth**, column-projected on
`ts_event`, so if the consumer process itself dies, "no new rows" still surfaces
correctly (a metrics-scrape would just go stale or error).

## Components

### 1. Must-flow feed registry

Default lives in code (typed, testable) as a module constant; floors tunable via
a single `.env` JSON override. Two cadence classes:

```python
# heber/health_monitor/feed_registry.py  (new)
@dataclass(frozen=True)
class FeedRule:
    feed: str
    kind: Literal["continuous", "daily"]
    window_start_et: str   # "HH:MM"  (continuous: active window; daily: ignored)
    window_end_et: str     # "HH:MM"  (continuous: active window; daily: deadline = this)
    lookback_minutes: int  # continuous only
    floor: int             # min rows required in window (continuous) / by deadline (daily)

DEFAULT_REGISTRY = [
    FeedRule("flow_alerts", "continuous", "09:30", "16:00", 60, 1),
    FeedRule("darkpool",    "continuous", "04:00", "20:00", 60, 1),
    FeedRule("bars",        "continuous", "09:30", "16:00", 30, 1),
    FeedRule("trades",      "continuous", "09:30", "16:00", 30, 1),
    FeedRule("oi_change",   "daily",      "",      "17:30", 0,  1),
    FeedRule("greek_exposure","daily",    "",      "17:30", 0,  1),
]
```

Floors default to `1` (catch hard-stops with ~zero false-positive risk).
Trickle-catching floors come from calibration (below). A feed can be disabled or
re-floored via `HEBER_ALERT_FLOOR_OVERRIDES` (JSON: `{"darkpool": 8, "flow_alerts": 25}`);
floor `0` disables a feed. Adding a brand-new feed is a code edit (rare).

### 2. Liveness check — `heber/health_monitor/checks/liveness.py` (new)

`async def run_liveness_checks(ctx: CheckContext, now: datetime) -> list[CheckResult]`

For each `FeedRule` (after applying overrides):

- **continuous**, and `now` (ET) inside `[window_start, window_end]` on a trading day:
  count Silver rows with `ts_event >= now - lookback_minutes` for today's
  partition (column-projected on `ts_event`). `count < floor` → `CheckResult`
  status `FAIL`, severity `P0_CRITICAL`, message e.g.
  `"flow_alerts: 0 rows in last 60m (floor 25) — feed appears dark"`.
  Missing partition / zero rows → treated as `count = 0` (the worst case, **not**
  skipped).
- **daily**, and `now` (ET) past `window_end` (deadline) on a trading day:
  today's partition row count `< floor` → `FAIL` / `P0_CRITICAL`.
- Otherwise (outside window / before deadline / non-trading day): emit a
  `PASS`/`P2_INFO` result (or no result) — no alert.

Returns `CheckResult`s using the existing `models.py` types so they flow through
`_record_and_store` and the notifier unchanged.

### 3. Discord notifier — `heber/ops/notifier.py` (new)

Ports the self-contained 3Roses webhook pattern (httpx POST `{"content": msg}` to
the webhook URL via `core.http_client`-style client). Adds what 3Roses lacks:

```python
class DiscordNotifier:
    def __init__(self, settings): ...
    def dispatch(self, results: list[CheckResult]) -> None:
        # filter severity >= min_severity (default critical)
        # for each (check_name, feed): apply throttle/dedup, then POST
    def send_test(self, text: str) -> bool: ...   # used by CLI
```

- **Severity gate:** only `>= HEBER_ALERT_MIN_SEVERITY` (default `critical`).
- **Throttle/dedup:** at most one alert per `(check_name, feed)` per
  `HEBER_ALERT_COOLDOWN_SECONDS` (default 3600). State persisted to
  `${data_root}/ops/alerts/state.json` (keyed by `(check_name, feed)` →
  `{last_sent_ts, last_status}`) so it survives restarts/relaunchd. A **status
  change** (e.g. PASS→FAIL or FAIL→PASS) bypasses the cooldown and sends
  immediately.
- **Recovery note:** when a `(check, feed)` that previously alerted returns to
  `PASS`, send one line (`"✅ darkpool recovered — 142 rows in last 60m"`) and
  clear its alert state. Gated by `HEBER_ALERT_SEND_RECOVERY` (default true).
  Not a digest — only fires on an actual recover transition.
- **Resilience:** all network/serialization errors are caught and logged
  loudly (`logger.error("discord_alert_failed", ...)`) but never raised into the
  check loop. A broken webhook must not crash the monitor.
- **Message body:** feed, what failed, observed vs. floor, window, ET timestamp,
  check name.

### 4. Wiring — `heber/health_monitor/service.py`

- Add a dedicated short loop (`_liveness_loop`, interval
  `HEBER_ALERT_LIVENESS_CHECK_INTERVAL_SECONDS`, default 300) that calls
  `run_liveness_checks` during trading days and routes results through
  `_record_and_store`.
- Instantiate a `DiscordNotifier` in `start()` and call `notifier.dispatch(results)`
  at the end of `_record_and_store(...)` so **all** tiers' results are eligible for
  alerting (liveness is the primary source; stream/DLQ criticals come free).
- `get_runtime_snapshot()` gains `alerting_enabled` and `last_alert_ts`.

### 5. Calibration helper — `heber alert-calibrate` (new CLI subcommand)

Reads a **healthy historical window** (default 45–60 days ago) for each
continuous feed, computes the median rows-per-`lookback_minutes` during active
hours, and prints a suggested floor at ~25–30% of that median. Output is a ready-to-paste
`HEBER_ALERT_FLOOR_OVERRIDES` JSON blob. This is what makes trickle detection
real (e.g. catching darkpool) without hand-guessing numbers. Read-only; prints
only.

### 6. Test command — `heber alert-test` (new CLI subcommand)

Sends a real Discord test message via `DiscordNotifier.send_test(...)` to verify
the webhook end-to-end. Prints success/failure.

## Configuration (new `HEBER_ALERT_*` in `heber/config.py`)

| Env var | Default | Purpose |
|---------|---------|---------|
| `HEBER_ALERT_DISCORD_ENABLED` | `false` | Master switch (stays off until URL set) |
| `HEBER_ALERT_DISCORD_WEBHOOK_URL` | `""` | Webhook URL (paste the 3Roses one) |
| `HEBER_ALERT_MIN_SEVERITY` | `critical` | Minimum severity to alert on |
| `HEBER_ALERT_COOLDOWN_SECONDS` | `3600` | Per-(check,feed) re-alert cooldown |
| `HEBER_ALERT_SEND_RECOVERY` | `true` | Send one-line recovery note on PASS transition |
| `HEBER_ALERT_LIVENESS_CHECK_INTERVAL_SECONDS` | `300` | Liveness loop interval |
| `HEBER_ALERT_FLOOR_OVERRIDES` | `{}` | JSON map `feed → floor`; `0` disables a feed |

`.env` is auto-sourced by `scripts/run_native_heber_service.sh`, so no plist
edits are needed. Requires `health_monitor_enabled=true` (already default).

## Deployment

1. Add `HEBER_ALERT_DISCORD_ENABLED=true` and `HEBER_ALERT_DISCORD_WEBHOOK_URL=<3Roses webhook>`
   (plus any calibrated `HEBER_ALERT_FLOOR_OVERRIDES`) to Heber's `.env`.
2. `uv run heber alert-test` → confirm the message lands in Discord.
3. Reinstall/reload via existing `scripts/install_native_launchd.sh` (loads
   `com.empire.heber.health-monitor.plist`).

## Testing (TDD)

Unit tests (`tests/`):
- **liveness — continuous breach:** today's partition has 0 / below-floor rows in
  window during active hours → one `P0_CRITICAL` `FAIL`.
- **liveness — daily deadline miss:** past 17:30 ET, `oi_change` partition absent
  → `P0_CRITICAL` `FAIL`.
- **liveness — healthy:** rows ≥ floor in window → `PASS`, no alert.
- **liveness — outside window / non-trading day:** no `FAIL` emitted.
- **liveness — missing partition is treated as zero**, not skipped (regression
  guard for the `volume.py:80` hole).
- **notifier — cooldown:** second identical critical within cooldown is
  suppressed; after cooldown it re-sends.
- **notifier — status-change override:** FAIL→PASS (and PASS→FAIL) bypasses
  cooldown.
- **notifier — severity gate:** warning/info never sent when min=critical.
- **notifier — webhook failure is swallowed + logged**, loop continues
  (mock httpx raising).
- **notifier — state persists** across notifier instances (state file round-trip).

Markers: `unit` for all of the above (no real network — httpx mocked).

## Out of scope / follow-ups

- **Upstream root cause (Data-Gateway):** the UW poller REST-polls (flow_alerts
  every 5 min market-hours-only; darkpool 15–60s). "Burst then silence" most
  likely = market-hours gating bug, Redis-sink circuit breaker tripping (opens
  after 20 failures), a silently-dying poller task, or auth/token failure. Tracked
  as a separate effort. Files: `gateway/core/uw_poller.py`,
  `gateway/core/redis_sink.py`, `gateway/core/circuit_breaker.py`.
- Backfilling the lost flow_alerts/darkpool/oi_change data.
- A periodic health digest (explicitly declined for v1).

## Open items (resolved at implementation time, not blocking)

- Exact calibrated floors per feed — produced by `alert-calibrate` against
  healthy history during rollout; v1 ships with floor=1 defaults.
