# Data Health Monitor — Design Spec

## Problem

Heber has no continuous way to detect data gaps, missing instruments, volume anomalies, schema drift, or ML data quality degradation. Existing checks (daily_health, dataflow_health) cover partition freshness and Redis connectivity, but miss intraday gaps, per-instrument coverage, row count trending, and statistical profiling. For ML training data, silent data quality issues are the most expensive kind — they corrupt models without any visible error.

## Solution

A single async service (`heber.health_monitor`) with three check tiers running at different intervals, storing results as Parquet in the Gold layer and exposing Prometheus metrics. Market-calendar-aware to suppress false positives on weekends, holidays, and outside market hours.

## Architecture

### Module Structure

```
heber/health_monitor/
    __init__.py
    __main__.py              # Entry: python -m heber.health_monitor
    service.py               # HealthMonitorService (async scheduler)
    checks/
        __init__.py
        stream_health.py     # Tier 1: Redis stream checks (30s)
        partition.py         # Tier 2: Partition completeness (15min)
        volume.py            # Tier 2: Row count trending (15min)
        statistical.py       # Tier 3: Distribution profiling (EOD)
        schema.py            # Tier 3: Schema drift detection (EOD)
        ml_readiness.py      # Tier 3: ML-specific checks (EOD)
    calendar.py              # Market calendar (holidays, half-days, hours)
    models.py                # CheckResult, Severity, Status, CheckContext
    store.py                 # Write results to gold/dataset=data_health/
    metrics.py               # Prometheus gauges/counters
```

### Runtime

Single long-running async process using `asyncio.TaskGroup` to schedule checks at their respective intervals. Entry point: `python -m heber.health_monitor`. Added to `docker-compose.yml` as `heber-health-monitor` service.

### Check Context

Every check receives a shared `CheckContext`:

```python
@dataclass
class CheckContext:
    settings: Settings
    reader: HeberReader
    redis: Redis
    calendar: MarketCalendar
    store: HealthStore
```

### Check Result Model

```python
class Severity(str, Enum):
    P0_CRITICAL = "critical"
    P1_WARNING = "warning"
    P2_INFO = "info"

class Status(str, Enum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"
    ERROR = "error"

@dataclass
class CheckResult:
    check_name: str
    feed: str | None
    severity: Severity
    status: Status
    message: str
    details: dict[str, Any]
    ts_checked: datetime
    instrument_key: str | None = None
```

Severity mapping:
- P0 → `logger.error` + critical metric counter
- P1 → `logger.warning` + warning metric counter
- P2 → `logger.info` (trends only)

## Check Tiers

### Tier 1 — Stream Health (every 30s)

Checks:
- Redis stream reachable + consumer group exists
- Per-stream pending message count (consumer lag)
- Per-feed message rate (messages/sec over last interval)
- DLQ depth and age of oldest DLQ message
- Feed freshness: time since last write per feed

Calendar aware: During market hours (9:30-16:00 ET), stale core feeds = P0. Outside market hours = P2. Weekends/holidays suppress freshness alerts entirely.

Reuses: `heber.bus.streams` Redis client, `heber.ops.dataflow_health._fetch_metrics_samples()`.

### Tier 2 — Partition Completeness & Volume (every 15 min, market hours only)

Partition completeness:
- Verify expected `dt=` partitions exist for today per Silver feed
- For hourly-partitioned feeds (bars, trades, quotes), verify expected `hour=` dirs based on elapsed market hours
- Detect empty partitions (0 parquet files or 0 rows via metadata reads)
- Per-instrument coverage: distinct `instrument_key` counts vs catalog universe

Volume trending:
- Row counts from Parquet metadata (footer only, no data scan)
- Compare against trailing 5-day median for same feed + hour
- Warn at <50% of baseline (P1), critical at <20% (P0)
- Baselines stored in `gold/dataset=data_health_baselines/dt=YYYY-MM-DD/`

Reuses: `GapDetector`, `pyarrow.parquet.read_metadata()`, `heber/calendar/market.py`, catalog DB for instrument universe.

### Tier 3 — Statistical Profiling & ML Readiness (EOD, ~16:35 ET)

Statistical profiling:
- Per-column stats for each Silver feed: count, null_count, null_pct, min, max, mean, stddev, p50, p95, p99
- Compare against trailing 30-day baselines
- Flag: null rate increase >5pp, mean/stddev shift >2σ from baseline, min/max outside historical range
- Schema drift: hash Arrow schema per feed, compare against last known, alert on column add/remove/type change

ML readiness (after gold_poller completes):
- Zero-leakage audit: full scan of Gold partitions, verify `ts_available >= ts_event` on 100% of rows. Any violation = P0.
- Label distribution stability: compare today's TP/SL/timeout ratios against 30-day trailing. PSI > 0.2 = P1.
- Feature null rates: per-column null % in Gold features. ML-critical column >5% nulls = P1.
- Cross-sectional completeness: for multi-instrument features (sector_flow, market_regime, gex_regime), verify ≥80% of expected universe contributed.

Reuses: `HeberReader`, `quality/contracts.py` (activating dormant contract framework), `daily_health.py` zero-leakage pattern (upgraded from sampling to full scan).

## Market Calendar

```python
class MarketCalendar:
    def is_market_open(self, dt: datetime) -> bool
    def is_trading_day(self, d: date) -> bool
    def market_hours(self, d: date) -> tuple[time, time] | None
    def expected_hours(self, d: date) -> list[int]
    def next_trading_day(self, d: date) -> date
```

Extends existing `heber/calendar/market.py` with hour-level granularity. Handles holidays, half-days (truncated hours), weekends. Primary false-positive suppressor.

## Storage

Check results written as Parquet to `gold/dataset=data_health/dt=YYYY-MM-DD/`. One file per check run, appended throughout the day. Baselines stored in `gold/dataset=data_health_baselines/dt=YYYY-MM-DD/`. Both queryable via `HeberReader.read_gold()`.

## Prometheus Metrics

| Metric | Type | Labels | Purpose |
|--------|------|--------|---------|
| `heber_health_check_status` | Gauge | check_name, feed, status | Current check state |
| `heber_health_check_duration_seconds` | Histogram | check_name | Check execution time |
| `heber_health_checks_total` | Counter | check_name, status | Cumulative check runs |
| `heber_health_gap_hours` | Gauge | feed | Longest detected gap (hours) |
| `heber_health_volume_ratio` | Gauge | feed | Today vs baseline row count ratio |
| `heber_health_null_rate` | Gauge | feed, column | Current null percentage |
| `heber_health_schema_changes_total` | Counter | feed | Schema change events |
| `heber_health_leakage_violations` | Gauge | dataset | ts_available violation count |

## EmpireUI Integration

Two consumption channels:

**Prometheus metrics (real-time):** The `heber_health_*` metrics feed into EmpireUI's existing metrics panel pattern. Key dashboard panels: feed status heatmap, volume ratio sparklines, gap timeline, DLQ depth trend.

**Catalog API endpoint (historical):** New `/api/v1/health/summary` endpoint on `heber-catalog` (port 8085) reads from `gold/dataset=data_health/` and returns: latest check results per feed, trend data (7/30 day history), current gap inventory. Thin read layer, no new service dependency.

Frontend components (React) are out of scope — this spec covers the data layer and API.

## Configuration

Added to `heber/config.py` Settings class, all via `HEBER_*` env vars:

| Variable | Default | Purpose |
|----------|---------|---------|
| `HEBER_HEALTH_MONITOR_ENABLED` | `true` | Enable the health monitor service |
| `HEBER_HEALTH_STREAM_CHECK_INTERVAL_SECONDS` | `30` | Tier 1 check frequency |
| `HEBER_HEALTH_PARTITION_CHECK_INTERVAL_SECONDS` | `900` | Tier 2 check frequency (15 min) |
| `HEBER_HEALTH_VOLUME_BASELINE_DAYS` | `5` | Days for volume baseline |
| `HEBER_HEALTH_STATS_BASELINE_DAYS` | `30` | Days for statistical baseline |
| `HEBER_HEALTH_VOLUME_WARN_RATIO` | `0.5` | Volume warn threshold (50%) |
| `HEBER_HEALTH_VOLUME_CRITICAL_RATIO` | `0.2` | Volume critical threshold (20%) |
| `HEBER_HEALTH_NULL_RATE_THRESHOLD` | `0.05` | Null rate alert threshold (5%) |
| `HEBER_HEALTH_PSI_THRESHOLD` | `0.2` | Label drift PSI threshold |
| `HEBER_HEALTH_LEAKAGE_SAMPLE_SIZE` | `0` | 0 = full scan, >0 = sample N rows |

## Testing Strategy

TDD throughout. All tests use `pytest` markers (`unit`, `integration`).

**Unit tests per check module:** Synthetic Parquet fixtures in tmp dirs, mock Redis for stream checks, mock calendar for time-sensitivity. Tests verify correct `CheckResult` for: healthy data, gaps, empty partitions, volume drops, schema changes, null spikes, leakage violations.

**Calendar tests:** Holiday suppression, half-day handling, weekend detection, market hour boundaries.

**Store tests:** Parquet write/read round-trip, baseline computation and comparison.

**Integration test:** Simulate a full day — create partitions with intentional gaps, run all three tiers, verify correct checks fire with correct severity.

## What This Reuses (Not Reinventing)

- `HeberReader` — all data reads
- `GapDetector` — date-level gap scanning
- `quality/contracts.py` — dormant contract framework, now activated
- `heber/calendar/market.py` — holiday/trading day data
- `heber.bus.streams` — Redis client
- `heber.ops.dataflow_health` — metrics sample fetching
- `heber.config.Settings` — configuration pattern
- `heber.ops.metrics` — Prometheus metric patterns
- Existing `structlog` + `empire_core.logger` patterns

## What This Does NOT Change

- `daily_health.py` — stays as EOD report (complementary, not replaced)
- `dataflow_health.py` — stays as `/health` endpoint (complementary)
- `backfill_scanner.py` — stays for enrichment backfill (different purpose)
- Consumer, writer, or any data path — monitoring only, no write-path changes
