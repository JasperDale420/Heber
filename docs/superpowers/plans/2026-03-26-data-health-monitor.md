# Data Health Monitor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a tiered data health monitoring service that detects gaps, volume anomalies, schema drift, and ML quality issues across all Silver feeds, with market-calendar awareness and Prometheus metrics.

**Architecture:** Single async service (`heber.health_monitor`) with three check tiers (stream health every 30s, partition completeness every 15min, statistical profiling at EOD). Each check module is independent, receives a shared CheckContext, and returns CheckResult objects. Results stored as Parquet in `gold/dataset=data_health/`.

**Tech Stack:** Python 3.12, asyncio, PyArrow (metadata reads), structlog, prometheus_client, redis (XINFO), pandas (statistical profiling), Pydantic Settings

**Spec:** `docs/superpowers/specs/2026-03-26-data-health-monitor-design.md`

---

## File Structure

### New files to create:
```
heber/health_monitor/__init__.py          # Public API: start/stop/get
heber/health_monitor/__main__.py          # Entry point
heber/health_monitor/service.py           # HealthMonitorService (async scheduler)
heber/health_monitor/models.py            # CheckResult, Severity, Status, CheckContext
heber/health_monitor/calendar.py          # Extended MarketCalendar with hour-level granularity
heber/health_monitor/store.py             # Write/read results to gold/dataset=data_health/
heber/health_monitor/metrics.py           # Prometheus metrics definitions
heber/health_monitor/checks/__init__.py   # Check registry
heber/health_monitor/checks/stream_health.py   # Tier 1
heber/health_monitor/checks/partition.py       # Tier 2
heber/health_monitor/checks/volume.py          # Tier 2
heber/health_monitor/checks/statistical.py     # Tier 3
heber/health_monitor/checks/schema.py          # Tier 3
heber/health_monitor/checks/ml_readiness.py    # Tier 3

tests/health_monitor/__init__.py
tests/health_monitor/test_models.py
tests/health_monitor/test_calendar.py
tests/health_monitor/test_store.py
tests/health_monitor/test_stream_health.py
tests/health_monitor/test_partition.py
tests/health_monitor/test_volume.py
tests/health_monitor/test_statistical.py
tests/health_monitor/test_schema.py
tests/health_monitor/test_ml_readiness.py
tests/health_monitor/test_service.py
tests/health_monitor/test_integration.py
```

### Files to modify:
```
heber/config.py                    # Add health monitor settings
heber/health_monitor/metrics.py    # New Prometheus metrics
docker-compose.yml                 # Add heber-health-monitor service
heber/catalog/api.py               # Add /api/v1/health/summary endpoint
CHANGELOG.md                       # Document the new service
```

### Key existing files to reuse (read-only references):
```
heber/gold_poller/service.py       # Async service pattern to follow
heber/gold_poller/__main__.py      # Entry point pattern to follow
heber/calendar/market.py           # Base calendar (extend, don't modify)
heber/ops/metrics.py               # _get_or_create() helper, metric patterns
heber/reader/core.py               # HeberReader for data access
heber/backfill/__init__.py         # GapDetector for gap scanning
heber/quality/contracts.py         # DataQualityValidator (activate existing)
heber/writer/ingest_contracts.py   # CONTRACTED_RAW_FEEDS feed list
heber/bus/streams.py               # Redis client patterns
```

---

## Task 1: Models & Data Types

**Files:**
- Create: `heber/health_monitor/__init__.py`
- Create: `heber/health_monitor/models.py`
- Test: `tests/health_monitor/__init__.py`
- Test: `tests/health_monitor/test_models.py`

- [ ] **Step 1: Write failing tests for models**

```python
# tests/health_monitor/__init__.py
# (empty)

# tests/health_monitor/test_models.py
"""Tests for health monitor data models."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from heber.health_monitor.models import CheckContext, CheckResult, Severity, Status


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
        """Flat row suitable for Parquet storage."""
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
        assert isinstance(row["details_json"], str)  # JSON serialized
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/health_monitor/test_models.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'heber.health_monitor'`

- [ ] **Step 3: Implement models**

```python
# heber/health_monitor/__init__.py
"""Heber Data Health Monitor — tiered data quality monitoring service."""

# heber/health_monitor/models.py
"""Data models for health check results."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from heber.config import Settings
from heber.reader.core import HeberReader


class Severity(str, Enum):
    """Alert severity tier."""

    P0_CRITICAL = "critical"
    P1_WARNING = "warning"
    P2_INFO = "info"

    def is_more_severe_than(self, other: Severity) -> bool:
        order = {Severity.P0_CRITICAL: 0, Severity.P1_WARNING: 1, Severity.P2_INFO: 2}
        return order[self] < order[other]


class Status(str, Enum):
    """Check outcome status."""

    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"
    ERROR = "error"

    @property
    def is_healthy(self) -> bool:
        return self == Status.PASS


@dataclass
class CheckResult:
    """Result of a single health check execution."""

    check_name: str
    feed: str | None
    severity: Severity
    status: Status
    message: str
    details: dict[str, Any]
    ts_checked: datetime
    instrument_key: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "check_name": self.check_name,
            "feed": self.feed,
            "severity": self.severity.value,
            "status": self.status.value,
            "message": self.message,
            "details": self.details,
            "ts_checked": self.ts_checked.isoformat(),
            "instrument_key": self.instrument_key,
        }

    def to_flat_row(self) -> dict[str, Any]:
        """Flatten for Parquet storage."""
        return {
            "check_name": self.check_name,
            "feed": self.feed or "",
            "severity": self.severity.value,
            "status": self.status.value,
            "message": self.message,
            "details_json": json.dumps(self.details, default=str),
            "ts_checked": self.ts_checked,
            "instrument_key": self.instrument_key or "",
        }


@dataclass
class CheckContext:
    """Shared context passed to every check function."""

    settings: Settings
    reader: HeberReader
    redis: Any  # redis.asyncio.Redis
    calendar: Any  # MarketCalendar
    store: Any  # HealthStore
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/health_monitor/test_models.py -v`
Expected: PASS (all 6 tests)

- [ ] **Step 5: Commit**

```bash
git add heber/health_monitor/__init__.py heber/health_monitor/models.py \
       tests/health_monitor/__init__.py tests/health_monitor/test_models.py
git commit -m "feat(health-monitor): add CheckResult, Severity, Status data models"
```

---

## Task 2: Market Calendar Extension

**Files:**
- Create: `heber/health_monitor/calendar.py`
- Test: `tests/health_monitor/test_calendar.py`

- [ ] **Step 1: Write failing tests for extended calendar**

```python
# tests/health_monitor/test_calendar.py
"""Tests for health monitor market calendar."""

from __future__ import annotations

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import pytest

from heber.health_monitor.calendar import HealthCalendar

ET = ZoneInfo("America/New_York")


class TestHealthCalendar:
    @pytest.fixture()
    def cal(self):
        return HealthCalendar()

    def test_regular_trading_day_hours(self, cal):
        # Monday March 24 2026 is a regular trading day
        d = date(2026, 3, 24)
        assert cal.is_trading_day(d)
        hours = cal.expected_hours(d)
        assert hours == [9, 10, 11, 12, 13, 14, 15]

    def test_weekend_not_trading_day(self, cal):
        # Saturday March 28 2026
        d = date(2026, 3, 28)
        assert not cal.is_trading_day(d)
        assert cal.expected_hours(d) == []

    def test_market_hours(self, cal):
        d = date(2026, 3, 24)
        open_time, close_time = cal.market_hours(d)
        assert open_time == time(9, 30)
        assert close_time == time(16, 0)

    def test_closed_day_market_hours(self, cal):
        # Saturday
        d = date(2026, 3, 28)
        assert cal.market_hours(d) is None

    def test_is_market_open_during_hours(self, cal):
        dt = datetime(2026, 3, 24, 10, 30, 0, tzinfo=ET)
        assert cal.is_market_open(dt)

    def test_is_market_closed_before_open(self, cal):
        dt = datetime(2026, 3, 24, 9, 0, 0, tzinfo=ET)
        assert not cal.is_market_open(dt)

    def test_is_market_closed_after_close(self, cal):
        dt = datetime(2026, 3, 24, 16, 30, 0, tzinfo=ET)
        assert not cal.is_market_open(dt)

    def test_next_trading_day_from_friday(self, cal):
        friday = date(2026, 3, 27)
        assert cal.next_trading_day(friday) == date(2026, 3, 30)  # Monday

    def test_elapsed_market_hours(self, cal):
        """At 12:15 ET on a trading day, hours 9,10,11,12 should be elapsed."""
        dt = datetime(2026, 3, 24, 12, 15, 0, tzinfo=ET)
        elapsed = cal.elapsed_hours(dt)
        assert elapsed == [9, 10, 11, 12]

    def test_elapsed_hours_before_market_open(self, cal):
        dt = datetime(2026, 3, 24, 8, 0, 0, tzinfo=ET)
        assert cal.elapsed_hours(dt) == []

    def test_suppress_severity_outside_market(self, cal):
        """Outside market hours, P0 should be suppressed to P2."""
        from heber.health_monitor.models import Severity

        weekend_dt = datetime(2026, 3, 28, 12, 0, 0, tzinfo=ET)
        result = cal.adjust_severity(Severity.P0_CRITICAL, weekend_dt)
        assert result == Severity.P2_INFO

    def test_no_suppression_during_market(self, cal):
        from heber.health_monitor.models import Severity

        market_dt = datetime(2026, 3, 24, 10, 0, 0, tzinfo=ET)
        result = cal.adjust_severity(Severity.P0_CRITICAL, market_dt)
        assert result == Severity.P0_CRITICAL
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/health_monitor/test_calendar.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'heber.health_monitor.calendar'`

- [ ] **Step 3: Implement HealthCalendar**

```python
# heber/health_monitor/calendar.py
"""Extended market calendar with hour-level granularity for health checks."""

from __future__ import annotations

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import structlog

from heber.calendar.market import MarketCalendar as BaseCalendar
from heber.calendar.market import get_calendar
from heber.health_monitor.models import Severity

logger = structlog.get_logger(__name__)

ET = ZoneInfo("America/New_York")

# Regular market hours: 9:30 AM - 4:00 PM ET
REGULAR_OPEN = time(9, 30)
REGULAR_CLOSE = time(16, 0)
# Early close (half days): 9:30 AM - 1:00 PM ET
EARLY_CLOSE = time(13, 0)


class HealthCalendar:
    """Market calendar extended with hour-level granularity for partition checks."""

    def __init__(self) -> None:
        self._base = get_calendar()

    def is_trading_day(self, d: date) -> bool:
        """Return True if d is a regular or half trading day."""
        return self._base.is_trading_day(d)

    def is_market_open(self, dt: datetime) -> bool:
        """Return True if dt falls within market hours."""
        return self._base.is_market_open(dt)

    def market_hours(self, d: date) -> tuple[time, time] | None:
        """Return (open, close) times for a trading day, or None if closed."""
        if not self.is_trading_day(d):
            return None
        close = self._base.session_close(datetime.combine(d, time(12, 0), tzinfo=ET))
        close_time = close.time() if close else REGULAR_CLOSE
        return REGULAR_OPEN, close_time

    def expected_hours(self, d: date) -> list[int]:
        """Return list of hour integers where data is expected.

        For regular day: [9, 10, 11, 12, 13, 14, 15]
        For early close: [9, 10, 11, 12]
        For non-trading day: []
        """
        hours = self.market_hours(d)
        if hours is None:
            return []
        _, close_time = hours
        # Hours that contain trading activity: 9 (9:30-10), 10 (10-11), ... up to close
        last_hour = close_time.hour - 1 if close_time.minute == 0 else close_time.hour
        return list(range(9, last_hour + 1))

    def elapsed_hours(self, dt: datetime) -> list[int]:
        """Return hours that have elapsed at the given time on that trading day."""
        d = dt.date() if not isinstance(dt, date) else dt
        if isinstance(dt, datetime):
            et_dt = dt.astimezone(ET) if dt.tzinfo else dt.replace(tzinfo=ET)
            d = et_dt.date()
        else:
            return self.expected_hours(d)

        expected = self.expected_hours(d)
        if not expected:
            return []

        et_dt = dt.astimezone(ET) if dt.tzinfo else dt.replace(tzinfo=ET)
        current_hour = et_dt.hour
        return [h for h in expected if h <= current_hour]

    def next_trading_day(self, d: date) -> date:
        """Return the next trading day after d."""
        from datetime import timedelta

        candidate = d + timedelta(days=1)
        while not self.is_trading_day(candidate):
            candidate += timedelta(days=1)
        return candidate

    def adjust_severity(self, severity: Severity, dt: datetime) -> Severity:
        """Suppress severity outside market hours.

        During market hours: severity unchanged.
        Outside market hours / weekends / holidays: downgrade to P2_INFO.
        """
        if self.is_market_open(dt):
            return severity
        return Severity.P2_INFO
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/health_monitor/test_calendar.py -v`
Expected: PASS (all 13 tests)

- [ ] **Step 5: Commit**

```bash
git add heber/health_monitor/calendar.py tests/health_monitor/test_calendar.py
git commit -m "feat(health-monitor): add HealthCalendar with hour-level granularity"
```

---

## Task 3: Prometheus Metrics

**Files:**
- Create: `heber/health_monitor/metrics.py`

- [ ] **Step 1: Create metrics module**

```python
# heber/health_monitor/metrics.py
"""Prometheus metrics for the data health monitor."""

from __future__ import annotations

from heber.ops.metrics import _get_or_create
from prometheus_client import Counter, Gauge, Histogram

# Check execution
health_check_status = _get_or_create(
    Gauge,
    "heber_health_check_status",
    "Current health check status (1=pass, 0=fail)",
    ["check_name", "feed"],
)

health_check_duration_seconds = _get_or_create(
    Histogram,
    "heber_health_check_duration_seconds",
    "Health check execution duration",
    ["check_name"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0],
)

health_checks_total = _get_or_create(
    Counter,
    "heber_health_checks_total",
    "Total health checks executed",
    ["check_name", "status"],
)

# Data quality gauges
health_gap_hours = _get_or_create(
    Gauge,
    "heber_health_gap_hours",
    "Longest detected data gap in hours",
    ["feed"],
)

health_volume_ratio = _get_or_create(
    Gauge,
    "heber_health_volume_ratio",
    "Today vs baseline row count ratio",
    ["feed"],
)

health_null_rate = _get_or_create(
    Gauge,
    "heber_health_null_rate",
    "Current null percentage per column",
    ["feed", "column"],
)

health_schema_changes_total = _get_or_create(
    Counter,
    "heber_health_schema_changes_total",
    "Schema change events detected",
    ["feed"],
)

health_leakage_violations = _get_or_create(
    Gauge,
    "heber_health_leakage_violations",
    "Count of ts_available < ts_event violations",
    ["dataset"],
)


def record_check(check_name: str, feed: str, status: str, duration: float) -> None:
    """Record a health check execution."""
    status_val = 1.0 if status == "pass" else 0.0
    health_check_status.labels(check_name=check_name, feed=feed or "").set(status_val)
    health_check_duration_seconds.labels(check_name=check_name).observe(duration)
    health_checks_total.labels(check_name=check_name, status=status).inc()
```

- [ ] **Step 2: Verify imports work**

Run: `uv run python -c "from heber.health_monitor.metrics import record_check; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add heber/health_monitor/metrics.py
git commit -m "feat(health-monitor): add Prometheus metrics definitions"
```

---

## Task 4: Health Store (Parquet persistence)

**Files:**
- Create: `heber/health_monitor/store.py`
- Test: `tests/health_monitor/test_store.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/health_monitor/test_store.py
"""Tests for health check result storage."""

from __future__ import annotations

from datetime import UTC, datetime, date
from pathlib import Path

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
        baseline = pd.DataFrame({
            "feed": ["bars", "trades"],
            "hour": [10, 10],
            "row_count_median": [45000.0, 52000.0],
        })
        store.write_baseline(baseline, report_date=date(2026, 3, 26))
        partition = store.data_root / "gold" / "dataset=data_health_baselines" / "dt=2026-03-26"
        assert partition.exists()

    def test_read_baseline_range(self, store):
        for day_offset in range(5):
            d = date(2026, 3, 20 + day_offset)
            baseline = pd.DataFrame({
                "feed": ["bars"],
                "hour": [10],
                "row_count_median": [45000.0 + day_offset * 100],
            })
            store.write_baseline(baseline, report_date=d)

        df = store.read_baselines(
            start_date=date(2026, 3, 20),
            end_date=date(2026, 3, 24),
        )
        assert len(df) == 5
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/health_monitor/test_store.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement HealthStore**

```python
# heber/health_monitor/store.py
"""Persist health check results and baselines as Parquet in the Gold layer."""

from __future__ import annotations

from datetime import date, datetime, UTC
from pathlib import Path
from uuid import uuid4

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from heber.health_monitor.models import CheckResult

logger = structlog.get_logger(__name__)

HEALTH_DATASET = "data_health"
BASELINE_DATASET = "data_health_baselines"

HEALTH_SCHEMA = pa.schema([
    ("check_name", pa.string()),
    ("feed", pa.string()),
    ("severity", pa.string()),
    ("status", pa.string()),
    ("message", pa.string()),
    ("details_json", pa.string()),
    ("ts_checked", pa.timestamp("us", tz="UTC")),
    ("instrument_key", pa.string()),
])


class HealthStore:
    """Write and read health check results from Gold layer Parquet."""

    def __init__(self, data_root: Path | None = None) -> None:
        if data_root is None:
            from heber.config import get_settings
            data_root = get_settings().data_root
        self.data_root = Path(data_root)

    def _health_partition(self, report_date: date) -> Path:
        return self.data_root / "gold" / f"dataset={HEALTH_DATASET}" / f"dt={report_date.isoformat()}"

    def _baseline_partition(self, report_date: date) -> Path:
        return self.data_root / "gold" / f"dataset={BASELINE_DATASET}" / f"dt={report_date.isoformat()}"

    def write_results(self, results: list[CheckResult], report_date: date) -> None:
        """Write check results as Parquet."""
        if not results:
            return
        rows = [r.to_flat_row() for r in results]
        df = pd.DataFrame(rows)
        table = pa.Table.from_pandas(df, schema=HEALTH_SCHEMA)

        partition = self._health_partition(report_date)
        partition.mkdir(parents=True, exist_ok=True)
        out_path = partition / f"health_{uuid4().hex[:8]}.parquet"
        pq.write_table(table, out_path)
        logger.info("health_results_written", path=str(out_path), count=len(results))

    def read_results(self, report_date: date) -> pd.DataFrame:
        """Read check results for a date."""
        partition = self._health_partition(report_date)
        if not partition.exists():
            return pd.DataFrame(columns=[f.name for f in HEALTH_SCHEMA])
        files = list(partition.glob("*.parquet"))
        if not files:
            return pd.DataFrame(columns=[f.name for f in HEALTH_SCHEMA])
        return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    def write_baseline(self, df: pd.DataFrame, report_date: date) -> None:
        """Write volume/stats baseline for a date."""
        partition = self._baseline_partition(report_date)
        partition.mkdir(parents=True, exist_ok=True)
        out_path = partition / "baseline.parquet"
        df.to_parquet(out_path, index=False)

    def read_baselines(self, start_date: date, end_date: date) -> pd.DataFrame:
        """Read baseline data across a date range."""
        from datetime import timedelta

        frames = []
        current = start_date
        while current <= end_date:
            partition = self._baseline_partition(current)
            if partition.exists():
                for f in partition.glob("*.parquet"):
                    frame = pd.read_parquet(f)
                    frame["dt"] = current.isoformat()
                    frames.append(frame)
            current += timedelta(days=1)
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/health_monitor/test_store.py -v`
Expected: PASS (all 5 tests)

- [ ] **Step 5: Commit**

```bash
git add heber/health_monitor/store.py tests/health_monitor/test_store.py
git commit -m "feat(health-monitor): add HealthStore for Parquet result persistence"
```

---

## Task 5: Configuration Settings

**Files:**
- Modify: `heber/config.py`

- [ ] **Step 1: Add health monitor settings to config.py**

Add the following fields to the `Settings` class in `heber/config.py`, after the existing gold_poller settings block (around line 284):

```python
    # Health Monitor
    health_monitor_enabled: bool = Field(
        default=True,
        description="Enable the data health monitor service",
    )
    health_stream_check_interval_seconds: int = Field(
        default=30,
        ge=5,
        le=300,
        description="Tier 1 stream health check interval (seconds)",
    )
    health_partition_check_interval_seconds: int = Field(
        default=900,
        ge=60,
        le=3600,
        description="Tier 2 partition completeness check interval (seconds)",
    )
    health_volume_baseline_days: int = Field(
        default=5,
        ge=1,
        le=30,
        description="Days of history for volume baseline comparison",
    )
    health_stats_baseline_days: int = Field(
        default=30,
        ge=7,
        le=90,
        description="Days of history for statistical baseline comparison",
    )
    health_volume_warn_ratio: float = Field(
        default=0.5,
        ge=0.0,
        le=1.0,
        description="Volume ratio threshold for warning (e.g., 0.5 = 50% of baseline)",
    )
    health_volume_critical_ratio: float = Field(
        default=0.2,
        ge=0.0,
        le=1.0,
        description="Volume ratio threshold for critical alert",
    )
    health_null_rate_threshold: float = Field(
        default=0.05,
        ge=0.0,
        le=1.0,
        description="Null rate threshold for ML feature alerts (e.g., 0.05 = 5%)",
    )
    health_psi_threshold: float = Field(
        default=0.2,
        ge=0.0,
        le=1.0,
        description="Population Stability Index threshold for label drift detection",
    )
    health_leakage_sample_size: int = Field(
        default=0,
        ge=0,
        description="Rows to sample for zero-leakage audit (0 = full scan)",
    )
```

- [ ] **Step 2: Verify config loads**

Run: `uv run python -c "from heber.config import get_settings; s = get_settings(); print(s.health_monitor_enabled, s.health_stream_check_interval_seconds)"`
Expected: `True 30`

- [ ] **Step 3: Commit**

```bash
git add heber/config.py
git commit -m "feat(health-monitor): add health monitor settings to config"
```

---

## Task 6: Tier 1 — Stream Health Check

**Files:**
- Create: `heber/health_monitor/checks/__init__.py`
- Create: `heber/health_monitor/checks/stream_health.py`
- Test: `tests/health_monitor/test_stream_health.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/health_monitor/test_stream_health.py
"""Tests for Tier 1 stream health checks."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

from heber.health_monitor.calendar import HealthCalendar
from heber.health_monitor.checks.stream_health import run_stream_health_checks
from heber.health_monitor.models import CheckContext, Severity, Status

ET = ZoneInfo("America/New_York")


@pytest.fixture()
def mock_context():
    settings = MagicMock()
    settings.redis_stream_name = "heber:events"
    settings.redis_consumer_group = "heber-writers"
    settings.redis_dlq_stream_name = "heber:events:dlq"
    settings.health_freshness_seconds = 900

    redis = AsyncMock()
    calendar = HealthCalendar()
    store = MagicMock()
    reader = MagicMock()

    return CheckContext(
        settings=settings,
        reader=reader,
        redis=redis,
        calendar=calendar,
        store=store,
    )


class TestStreamHealthChecks:
    @pytest.mark.asyncio
    async def test_healthy_stream(self, mock_context):
        """All streams reachable, no lag, no DLQ."""
        mock_context.redis.xinfo_stream.return_value = {
            "length": 1000,
            "first-entry": ("1-0", {}),
            "last-entry": ("1000-0", {}),
        }
        mock_context.redis.xinfo_groups.return_value = [
            {"name": "heber-writers", "pending": 0, "consumers": 1}
        ]
        mock_context.redis.xlen.return_value = 0  # DLQ empty

        # Simulate market hours
        market_dt = datetime(2026, 3, 24, 10, 0, 0, tzinfo=ET)
        with patch("heber.health_monitor.checks.stream_health._now_et", return_value=market_dt):
            results = await run_stream_health_checks(mock_context)

        statuses = {r.check_name: r.status for r in results}
        assert statuses["stream_reachable"] == Status.PASS
        assert statuses["consumer_group"] == Status.PASS
        assert statuses["dlq_depth"] == Status.PASS

    @pytest.mark.asyncio
    async def test_stream_unreachable(self, mock_context):
        """Redis connection failure."""
        mock_context.redis.xinfo_stream.side_effect = Exception("Connection refused")

        market_dt = datetime(2026, 3, 24, 10, 0, 0, tzinfo=ET)
        with patch("heber.health_monitor.checks.stream_health._now_et", return_value=market_dt):
            results = await run_stream_health_checks(mock_context)

        stream_check = next(r for r in results if r.check_name == "stream_reachable")
        assert stream_check.status == Status.FAIL
        assert stream_check.severity == Severity.P0_CRITICAL

    @pytest.mark.asyncio
    async def test_dlq_nonempty(self, mock_context):
        """DLQ has messages — should warn."""
        mock_context.redis.xinfo_stream.return_value = {"length": 100, "first-entry": ("1-0", {}), "last-entry": ("100-0", {})}
        mock_context.redis.xinfo_groups.return_value = [{"name": "heber-writers", "pending": 0, "consumers": 1}]
        mock_context.redis.xlen.return_value = 50

        market_dt = datetime(2026, 3, 24, 10, 0, 0, tzinfo=ET)
        with patch("heber.health_monitor.checks.stream_health._now_et", return_value=market_dt):
            results = await run_stream_health_checks(mock_context)

        dlq = next(r for r in results if r.check_name == "dlq_depth")
        assert dlq.status == Status.WARN
        assert dlq.details["depth"] == 50

    @pytest.mark.asyncio
    async def test_severity_suppressed_outside_market(self, mock_context):
        """Outside market hours, failures downgrade to P2_INFO."""
        mock_context.redis.xinfo_stream.side_effect = Exception("Connection refused")

        weekend_dt = datetime(2026, 3, 28, 12, 0, 0, tzinfo=ET)
        with patch("heber.health_monitor.checks.stream_health._now_et", return_value=weekend_dt):
            results = await run_stream_health_checks(mock_context)

        stream_check = next(r for r in results if r.check_name == "stream_reachable")
        assert stream_check.severity == Severity.P2_INFO

    @pytest.mark.asyncio
    async def test_high_pending_count(self, mock_context):
        """High pending messages indicate consumer lag."""
        mock_context.redis.xinfo_stream.return_value = {"length": 10000, "first-entry": ("1-0", {}), "last-entry": ("10000-0", {})}
        mock_context.redis.xinfo_groups.return_value = [{"name": "heber-writers", "pending": 5000, "consumers": 1}]
        mock_context.redis.xlen.return_value = 0

        market_dt = datetime(2026, 3, 24, 10, 0, 0, tzinfo=ET)
        with patch("heber.health_monitor.checks.stream_health._now_et", return_value=market_dt):
            results = await run_stream_health_checks(mock_context)

        lag = next(r for r in results if r.check_name == "consumer_lag")
        assert lag.status == Status.WARN
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/health_monitor/test_stream_health.py -v`
Expected: FAIL

- [ ] **Step 3: Implement stream health checks**

```python
# heber/health_monitor/checks/__init__.py
"""Health check modules."""

# heber/health_monitor/checks/stream_health.py
"""Tier 1: Redis stream health checks (runs every 30s)."""

from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import structlog

from heber.health_monitor.models import CheckContext, CheckResult, Severity, Status

logger = structlog.get_logger(__name__)

ET = ZoneInfo("America/New_York")

# Thresholds
PENDING_WARN_THRESHOLD = 1000
PENDING_CRITICAL_THRESHOLD = 10000
DLQ_WARN_THRESHOLD = 1


def _now_et() -> datetime:
    return datetime.now(ET)


async def run_stream_health_checks(ctx: CheckContext) -> list[CheckResult]:
    """Run all Tier 1 stream health checks."""
    now = datetime.now(UTC)
    now_et = _now_et()
    results: list[CheckResult] = []

    # 1. Stream reachable
    try:
        stream_info = await ctx.redis.xinfo_stream(ctx.settings.redis_stream_name)
        results.append(CheckResult(
            check_name="stream_reachable",
            feed=None,
            severity=ctx.calendar.adjust_severity(Severity.P0_CRITICAL, now_et),
            status=Status.PASS,
            message=f"Stream reachable, length={stream_info.get('length', 0)}",
            details={"length": stream_info.get("length", 0)},
            ts_checked=now,
        ))
    except Exception as exc:
        results.append(CheckResult(
            check_name="stream_reachable",
            feed=None,
            severity=ctx.calendar.adjust_severity(Severity.P0_CRITICAL, now_et),
            status=Status.FAIL,
            message=f"Stream unreachable: {exc}",
            details={"error": str(exc)},
            ts_checked=now,
        ))
        return results  # Can't check further if stream is down

    # 2. Consumer group exists + lag
    try:
        groups = await ctx.redis.xinfo_groups(ctx.settings.redis_stream_name)
        group = next((g for g in groups if g["name"] == ctx.settings.redis_consumer_group), None)

        if group is None:
            results.append(CheckResult(
                check_name="consumer_group",
                feed=None,
                severity=ctx.calendar.adjust_severity(Severity.P0_CRITICAL, now_et),
                status=Status.FAIL,
                message=f"Consumer group '{ctx.settings.redis_consumer_group}' not found",
                details={"available_groups": [g["name"] for g in groups]},
                ts_checked=now,
            ))
        else:
            results.append(CheckResult(
                check_name="consumer_group",
                feed=None,
                severity=ctx.calendar.adjust_severity(Severity.P2_INFO, now_et),
                status=Status.PASS,
                message=f"Consumer group active, consumers={group.get('consumers', 0)}",
                details={"consumers": group.get("consumers", 0), "pending": group.get("pending", 0)},
                ts_checked=now,
            ))

            # Consumer lag check
            pending = group.get("pending", 0)
            if pending >= PENDING_CRITICAL_THRESHOLD:
                lag_status, lag_sev = Status.FAIL, Severity.P0_CRITICAL
            elif pending >= PENDING_WARN_THRESHOLD:
                lag_status, lag_sev = Status.WARN, Severity.P1_WARNING
            else:
                lag_status, lag_sev = Status.PASS, Severity.P2_INFO

            results.append(CheckResult(
                check_name="consumer_lag",
                feed=None,
                severity=ctx.calendar.adjust_severity(lag_sev, now_et),
                status=lag_status,
                message=f"Pending messages: {pending}",
                details={"pending": pending},
                ts_checked=now,
            ))
    except Exception as exc:
        results.append(CheckResult(
            check_name="consumer_group",
            feed=None,
            severity=ctx.calendar.adjust_severity(Severity.P0_CRITICAL, now_et),
            status=Status.ERROR,
            message=f"Failed to check consumer group: {exc}",
            details={"error": str(exc)},
            ts_checked=now,
        ))

    # 3. DLQ depth
    try:
        dlq_len = await ctx.redis.xlen(ctx.settings.redis_dlq_stream_name)
        dlq_status = Status.WARN if dlq_len >= DLQ_WARN_THRESHOLD else Status.PASS
        dlq_sev = Severity.P1_WARNING if dlq_len >= DLQ_WARN_THRESHOLD else Severity.P2_INFO

        results.append(CheckResult(
            check_name="dlq_depth",
            feed=None,
            severity=ctx.calendar.adjust_severity(dlq_sev, now_et),
            status=dlq_status,
            message=f"DLQ depth: {dlq_len}",
            details={"depth": dlq_len},
            ts_checked=now,
        ))
    except Exception as exc:
        results.append(CheckResult(
            check_name="dlq_depth",
            feed=None,
            severity=ctx.calendar.adjust_severity(Severity.P1_WARNING, now_et),
            status=Status.ERROR,
            message=f"Failed to check DLQ: {exc}",
            details={"error": str(exc)},
            ts_checked=now,
        ))

    return results
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/health_monitor/test_stream_health.py -v`
Expected: PASS (all 5 tests)

- [ ] **Step 5: Commit**

```bash
git add heber/health_monitor/checks/__init__.py heber/health_monitor/checks/stream_health.py \
       tests/health_monitor/test_stream_health.py
git commit -m "feat(health-monitor): add Tier 1 stream health checks"
```

---

## Task 7: Tier 2 — Partition Completeness Check

**Files:**
- Create: `heber/health_monitor/checks/partition.py`
- Test: `tests/health_monitor/test_partition.py`

- [ ] **Step 1: Write failing tests**

Tests should cover: all expected partitions present (PASS), missing dt= partition (FAIL), missing hour= subdirectory (WARN), empty partition (WARN), weekend suppression. Use `tmp_path` to create synthetic Hive-partitioned directories with small Parquet files.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/health_monitor/test_partition.py -v`

- [ ] **Step 3: Implement partition check**

The check should:
1. Get today's date and the list of Silver feeds from `CONTRACTED_RAW_FEEDS` minus `BRONZE_ONLY_SILVER_DATASETS`
2. For each feed, check if `silver/feed={feed}/dt={today}` exists
3. For hourly feeds (bars, trades, quotes), check expected `hour=` subdirectories against `calendar.elapsed_hours(now)`
4. Read Parquet metadata to check for empty partitions (0 rows)
5. Return CheckResults with appropriate severity (calendar-adjusted)

Reuse: `CONTRACTED_RAW_FEEDS` from `heber.writer.ingest_contracts`, `HealthCalendar.elapsed_hours()`, `pyarrow.parquet.read_metadata()`.

- [ ] **Step 4: Run tests to verify they pass**
- [ ] **Step 5: Commit**

```bash
git commit -m "feat(health-monitor): add Tier 2 partition completeness check"
```

---

## Task 8: Tier 2 — Volume Trending Check

**Files:**
- Create: `heber/health_monitor/checks/volume.py`
- Test: `tests/health_monitor/test_volume.py`

- [ ] **Step 1: Write failing tests**

Tests should cover: volume within baseline (PASS), volume at 40% of baseline (WARN), volume at 10% of baseline (FAIL/P0), no baseline available (skip with INFO), first run baseline creation.

- [ ] **Step 2: Run tests to verify they fail**
- [ ] **Step 3: Implement volume check**

The check should:
1. For each Silver feed, scan today's partitions for row counts via `pyarrow.parquet.read_metadata()` (footer only)
2. Read trailing N-day baselines from `HealthStore.read_baselines()`
3. Compute median row count per feed+hour from baselines
4. Compare today's counts against baselines using `settings.health_volume_warn_ratio` and `health_volume_critical_ratio`
5. Update `health_volume_ratio` Prometheus gauge per feed
6. If no baseline exists, write today's counts as the first baseline

- [ ] **Step 4: Run tests to verify they pass**
- [ ] **Step 5: Commit**

```bash
git commit -m "feat(health-monitor): add Tier 2 volume trending check"
```

---

## Task 9: Tier 3 — Schema Drift Detection

**Files:**
- Create: `heber/health_monitor/checks/schema.py`
- Test: `tests/health_monitor/test_schema.py`

- [ ] **Step 1: Write failing tests**

Tests should cover: no drift detected (PASS), new column added (WARN), column removed (FAIL), type changed (FAIL), first run (stores baseline schema, PASS).

- [ ] **Step 2: Run tests to verify they fail**
- [ ] **Step 3: Implement schema check**

The check should:
1. For each Silver feed with data today, read the Arrow schema from the first Parquet file
2. Hash the schema (sorted column names + types) as a fingerprint
3. Compare against stored schema fingerprint in baselines
4. Detect: columns added, columns removed, type changes
5. Store new schema fingerprint if it changed
6. Update `health_schema_changes_total` counter

- [ ] **Step 4: Run tests to verify they pass**
- [ ] **Step 5: Commit**

```bash
git commit -m "feat(health-monitor): add Tier 3 schema drift detection"
```

---

## Task 10: Tier 3 — Statistical Profiling

**Files:**
- Create: `heber/health_monitor/checks/statistical.py`
- Test: `tests/health_monitor/test_statistical.py`

- [ ] **Step 1: Write failing tests**

Tests should cover: stats within baseline (PASS), null rate spike (WARN), mean shift >2σ (WARN), first run (creates baseline, PASS).

- [ ] **Step 2: Run tests to verify they fail**
- [ ] **Step 3: Implement statistical profiling**

The check should:
1. For each Silver feed, read today's data via `HeberReader.read_silver()`
2. Compute per-numeric-column: count, null_count, null_pct, min, max, mean, stddev, p50, p95, p99
3. Compare against trailing 30-day baselines
4. Flag anomalies: null rate increase >5pp, mean shift >2σ, min/max outside historical range
5. Update `health_null_rate` gauge per feed+column
6. Store today's stats as new baseline entry

- [ ] **Step 4: Run tests to verify they pass**
- [ ] **Step 5: Commit**

```bash
git commit -m "feat(health-monitor): add Tier 3 statistical profiling"
```

---

## Task 11: Tier 3 — ML Readiness Checks

**Files:**
- Create: `heber/health_monitor/checks/ml_readiness.py`
- Test: `tests/health_monitor/test_ml_readiness.py`

- [ ] **Step 1: Write failing tests**

Tests should cover: no leakage violations (PASS), ts_available < ts_event violation (P0 FAIL), label distribution stable (PASS), label PSI > threshold (WARN), high feature null rate (WARN), cross-sectional completeness below threshold (WARN).

- [ ] **Step 2: Run tests to verify they fail**
- [ ] **Step 3: Implement ML readiness checks**

The check should implement four sub-checks:
1. **Zero-leakage audit**: Read Gold partitions, check `ts_available >= ts_event` on all rows (or sample if configured). Any violation = P0.
2. **Label distribution**: Read `labels_alert_barriers` for today, compute TP/SL/timeout ratios, compute PSI against 30-day trailing. PSI > threshold = P1.
3. **Feature null rates**: Read Gold feature datasets, compute null % per column, alert if any exceeds threshold.
4. **Cross-sectional completeness**: For multi-instrument Gold datasets, count distinct instrument_keys, compare against catalog universe count.

- [ ] **Step 4: Run tests to verify they pass**
- [ ] **Step 5: Commit**

```bash
git commit -m "feat(health-monitor): add Tier 3 ML readiness checks"
```

---

## Task 12: Service Orchestrator

**Files:**
- Create: `heber/health_monitor/service.py`
- Create: `heber/health_monitor/__main__.py`
- Test: `tests/health_monitor/test_service.py`

- [ ] **Step 1: Write failing tests**

Tests should cover: service starts and stops cleanly, Tier 1 runs at configured interval, Tier 2 only runs during market hours, Tier 3 only runs after EOD trigger, check results are stored and logged, errors in one tier don't crash others.

- [ ] **Step 2: Run tests to verify they fail**
- [ ] **Step 3: Implement HealthMonitorService**

Follow the `GoldFeaturePoller` pattern from `heber/gold_poller/service.py`:
- Module-level singleton with `start_health_monitor()` / `stop_health_monitor()` / `get_health_monitor()`
- Three async tasks: `_tier1_loop()`, `_tier2_loop()`, `_tier3_loop()`
- Each loop catches exceptions, logs errors, records metrics, and continues
- Tier 1 runs every `health_stream_check_interval_seconds`
- Tier 2 runs every `health_partition_check_interval_seconds` but only during market hours
- Tier 3 runs once after 16:35 ET (same as gold_poller)
- All results written to `HealthStore` and logged

- [ ] **Step 4: Implement `__main__.py`**

Follow the `heber/gold_poller/__main__.py` pattern:
- Configure logging with service name `heber-health-monitor`
- Start metrics server on port 9093
- Register signal handlers for SIGTERM/SIGINT
- Run `start_health_monitor()` and wait for shutdown

- [ ] **Step 5: Run tests to verify they pass**
- [ ] **Step 6: Commit**

```bash
git commit -m "feat(health-monitor): add HealthMonitorService orchestrator and entry point"
```

---

## Task 13: Docker Compose & Catalog API

**Files:**
- Modify: `docker-compose.yml`
- Modify: `heber/catalog/api.py`

- [ ] **Step 1: Add heber-health-monitor to docker-compose.yml**

Add after the `heber-dataflow-health` service block:

```yaml
  heber-health-monitor:
    build:
      context: ..
      dockerfile: Heber/Dockerfile
    container_name: heber-health-monitor
    command: [ "python", "-m", "heber.health_monitor" ]
    environment:
      - HEBER_DATA_ROOT=/data
      - HEBER_REDIS_URL=redis://host.docker.internal:6379
      - HEBER_REDIS_STREAM_NAME=heber:events
      - HEBER_REDIS_CONSUMER_GROUP=heber-writers
      - HEBER_HEALTH_MONITOR_ENABLED=true
      - HEBER_HEALTH_STREAM_CHECK_INTERVAL_SECONDS=30
      - HEBER_HEALTH_PARTITION_CHECK_INTERVAL_SECONDS=900
    extra_hosts:
      - "host.docker.internal:host-gateway"
    volumes:
      - ${HEBER_VOLUME_ROOT:-/Volumes/heber}/data:/data
    ports:
      - "9093:9093"
    depends_on:
      heber-consumer:
        condition: service_started
    healthcheck:
      disable: true
    logging:
      driver: json-file
      options:
        max-size: "50m"
        max-file: "5"
    restart: always
```

- [ ] **Step 2: Add /api/v1/health/summary endpoint to catalog API**

Add a new endpoint to `heber/catalog/api.py` that reads from the health store:

```python
@app.get("/api/v1/health/summary")
async def health_summary(
    days: int = 1,
) -> dict[str, Any]:
    """Return latest health check results and trend data."""
    from heber.health_monitor.store import HealthStore

    store = HealthStore()
    today = date.today()
    start = today - timedelta(days=days - 1)

    frames = []
    current = start
    while current <= today:
        df = store.read_results(current)
        if len(df) > 0:
            frames.append(df)
        current += timedelta(days=1)

    if not frames:
        return {"checks": [], "summary": {"total": 0, "pass": 0, "warn": 0, "fail": 0, "error": 0}}

    all_results = pd.concat(frames, ignore_index=True)

    # Latest result per check_name+feed
    latest = all_results.sort_values("ts_checked").groupby(["check_name", "feed"]).last().reset_index()

    summary = latest["status"].value_counts().to_dict()

    return {
        "checks": latest.to_dict(orient="records"),
        "summary": {
            "total": len(latest),
            "pass": summary.get("pass", 0),
            "warn": summary.get("warn", 0),
            "fail": summary.get("fail", 0),
            "error": summary.get("error", 0),
        },
    }
```

- [ ] **Step 3: Commit**

```bash
git commit -m "feat(health-monitor): add Docker Compose service and catalog API endpoint"
```

---

## Task 14: Integration Test

**Files:**
- Create: `tests/health_monitor/test_integration.py`

- [ ] **Step 1: Write integration test**

Create a test that simulates a partial day of data with intentional gaps, runs all three check tiers, and verifies the correct findings:

1. Create synthetic Silver partitions in tmp_path for bars, quotes, trades (with one feed missing a partition, one feed with empty partition)
2. Create a Gold partition with one ts_available violation
3. Mock Redis with healthy stream info
4. Run all three tiers against the synthetic data
5. Assert: partition check catches the missing partition, volume check detects the empty partition, ml_readiness catches the leakage violation

Mark with `@pytest.mark.integration`.

- [ ] **Step 2: Run integration test**

Run: `uv run pytest tests/health_monitor/test_integration.py -v -m integration`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git commit -m "test(health-monitor): add integration test for full check cycle"
```

---

## Task 15: CHANGELOG & Final Verification

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add CHANGELOG entry**

Add under `## [Unreleased]` → `Added`:

```markdown
- **Data Health Monitor** — New tiered monitoring service (`python -m heber.health_monitor`) that detects data gaps, volume anomalies, schema drift, and ML quality issues across all Silver feeds. Three check tiers: stream health (30s), partition completeness (15min), statistical profiling (EOD). Market-calendar-aware to suppress false positives. Results stored in `gold/dataset=data_health/` and exposed via Prometheus metrics and `/api/v1/health/summary` catalog endpoint.
```

- [ ] **Step 2: Run full test suite**

Run: `uv run pytest -x -q`
Expected: All tests pass

- [ ] **Step 3: Run linter**

Run: `ruff check heber/health_monitor/ tests/health_monitor/`
Expected: All checks passed

- [ ] **Step 4: Commit**

```bash
git commit -m "docs: add data health monitor to CHANGELOG"
```
