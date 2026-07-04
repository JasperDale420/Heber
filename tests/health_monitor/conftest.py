"""Shared fixtures for health monitor tests."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.config import Settings
from heber.health_monitor.models import CheckContext

ET = ZoneInfo("America/New_York")

# Canonical test dates — Wednesday (trading) and Saturday (weekend).
TRADING_DAY = date(2026, 3, 25)
WEEKEND_DAY = date(2026, 3, 28)
MARKET_OPEN_DT = datetime(2026, 3, 25, 11, 30, tzinfo=ET)


# ---------------------------------------------------------------------------
# Low-level mock factories (used by fixtures and directly by tests)
# ---------------------------------------------------------------------------


def make_calendar_mock(*, is_trading: bool = True) -> MagicMock:
    """Return a MagicMock calendar with sensible defaults.

    - ``is_trading_day`` returns *is_trading*
    - ``adjust_severity`` is a passthrough (returns the severity unchanged)
    - ``elapsed_hours`` returns ``[9, 10, 11]`` (used by partition checks)
    """
    cal = MagicMock()
    cal.is_trading_day = MagicMock(return_value=is_trading)
    cal.adjust_severity = MagicMock(side_effect=lambda sev, _dt: sev)
    cal.elapsed_hours = MagicMock(return_value=[9, 10, 11])
    return cal


def make_store_mock() -> MagicMock:
    """Return a MagicMock store with empty baselines and a no-op write."""
    store = MagicMock()
    store.read_baselines = MagicMock(return_value=pd.DataFrame())
    store.write_baseline = MagicMock()
    return store


def make_check_context(
    tmp_path: Path,
    *,
    calendar: MagicMock | None = None,
    store: MagicMock | None = None,
    reader: MagicMock | None = None,
    redis: AsyncMock | MagicMock | None = None,
    settings_overrides: dict | None = None,
) -> CheckContext:
    """Build a ``CheckContext`` wired to *tmp_path* with mock collaborators.

    Any collaborator left as ``None`` gets a sensible default mock.
    """
    kwargs = {"data_root": str(tmp_path)}
    if settings_overrides:
        kwargs.update(settings_overrides)
    # _env_file=None keeps the fixture hermetic — otherwise a developer's local
    # .env (e.g. HEBER_ALERT_FLOOR_OVERRIDES) leaks into test Settings.
    settings = Settings(_env_file=None, **kwargs)

    return CheckContext(
        settings=settings,
        reader=reader or MagicMock(),
        redis=redis or MagicMock(),
        calendar=calendar or make_calendar_mock(),
        store=store or make_store_mock(),
    )


# ---------------------------------------------------------------------------
# Parquet helpers
# ---------------------------------------------------------------------------


def write_parquet(path: Path, num_rows: int = 10) -> None:
    """Write a minimal Parquet file with *num_rows* rows."""
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table({"x": list(range(num_rows))})
    pq.write_table(table, path)


def write_empty_parquet(path: Path) -> None:
    """Write an empty (0-row) Parquet file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table({"x": pa.array([], type=pa.int64())})
    pq.write_table(table, path)


def create_feed_partition(
    silver_root: Path,
    feed: str,
    dt: date,
    *,
    num_rows: int = 10,
    hours: list[int] | None = None,
    empty: bool = False,
    instrument_type: str = "equity",
) -> None:
    """Create a synthetic Silver partition for *feed* on *dt*.

    Mirrors the writer's real layout ``feed={feed}/instrument_type={type}/dt={dt}``
    (``get_partition_key``); the checks resolve partitions the same way.

    Parameters
    ----------
    hours:
        If given, create ``hour=HH`` sub-partitions instead of a flat file.
    empty:
        Write a 0-row Parquet file instead of a populated one.
    instrument_type:
        Hive ``instrument_type=`` level the writer always nests dt under.
    """
    dt_dir = silver_root / f"feed={feed}" / f"instrument_type={instrument_type}" / f"dt={dt.isoformat()}"
    if hours is not None:
        for h in hours:
            hour_dir = dt_dir / f"hour={h:02d}"
            if empty:
                write_empty_parquet(hour_dir / "part-0.parquet")
            else:
                write_parquet(hour_dir / "part-0.parquet", num_rows=num_rows)
    else:
        if empty:
            write_empty_parquet(dt_dir / "part-0.parquet")
        else:
            write_parquet(dt_dir / "part-0.parquet", num_rows=num_rows)


# ---------------------------------------------------------------------------
# Redis mock
# ---------------------------------------------------------------------------


def make_healthy_redis() -> AsyncMock:
    """Return an ``AsyncMock`` Redis that looks like a healthy stream."""
    redis = AsyncMock()
    redis.xinfo_stream = AsyncMock(return_value={"length": 42})
    redis.xinfo_groups = AsyncMock(
        return_value=[{"name": "heber-writers", "pending": 5, "consumers": 2}],
    )
    redis.xlen = AsyncMock(return_value=0)
    return redis


# ---------------------------------------------------------------------------
# Pytest fixtures (thin wrappers for common setups)
# ---------------------------------------------------------------------------


@pytest.fixture()
def trading_calendar() -> MagicMock:
    """A mock calendar where every day is a trading day."""
    return make_calendar_mock(is_trading=True)


@pytest.fixture()
def weekend_calendar() -> MagicMock:
    """A mock calendar where the day is *not* a trading day."""
    return make_calendar_mock(is_trading=False)


@pytest.fixture()
def empty_store() -> MagicMock:
    """A mock store with no baselines."""
    return make_store_mock()


@pytest.fixture()
def healthy_redis() -> AsyncMock:
    """An ``AsyncMock`` Redis representing a healthy stream."""
    return make_healthy_redis()
