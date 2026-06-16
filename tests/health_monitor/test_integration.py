"""Integration test — full health check cycle against synthetic data with intentional gaps."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.config import Settings
from heber.health_monitor.calendar import HealthCalendar
from heber.health_monitor.checks.ml_readiness import run_ml_readiness_checks
from heber.health_monitor.checks.partition import run_partition_checks
from heber.health_monitor.checks.stream_health import run_stream_health_checks
from heber.health_monitor.models import CheckContext, Status
from heber.health_monitor.store import HealthStore
from tests.health_monitor.conftest import make_healthy_redis

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

CHECK_DATE = date(2026, 3, 24)  # Tuesday — trading day
MARKET_HOURS_DT = datetime(2026, 3, 24, 12, 0, 0, tzinfo=ET)


def _create_silver_bars(silver_root: Path) -> None:
    """Create a bars partition with 10 valid rows."""
    dt_dir = silver_root / "feed=bars" / f"dt={CHECK_DATE.isoformat()}"
    dt_dir.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "event_id": [f"e{i}" for i in range(10)],
            "instrument_key": ["equity:AAPL"] * 10,
            "ts_event": [datetime(2026, 3, 24, 10, i, 0, tzinfo=UTC) for i in range(10)],
            "ts_available": [datetime(2026, 3, 24, 10, i, 1, tzinfo=UTC) for i in range(10)],
            "open": [150.0] * 10,
            "high": [151.0] * 10,
            "low": [149.0] * 10,
            "close": [150.5] * 10,
            "volume": [1000.0] * 10,
        }
    )
    pq.write_table(table, dt_dir / "data.parquet")


def _create_silver_trades_empty(silver_root: Path) -> None:
    """Create trades partition directory with an empty Parquet file (0 rows)."""
    dt_dir = silver_root / "feed=trades" / f"dt={CHECK_DATE.isoformat()}"
    dt_dir.mkdir(parents=True, exist_ok=True)
    table = pa.table({"x": pa.array([], type=pa.int64())})
    pq.write_table(table, dt_dir / "empty.parquet")


def _create_gold_with_leakage(data_root: Path) -> pd.DataFrame:
    """Create a Gold dataframe with ts_available BEFORE ts_event (leakage violation)."""
    return pd.DataFrame(
        {
            "alert_id": ["a1"],
            "instrument_key": ["equity:AAPL"],
            "ts_event": pd.to_datetime(["2026-03-24T10:00:00"], utc=True),
            "ts_available": pd.to_datetime(["2026-03-24T09:59:00"], utc=True),  # BEFORE ts_event!
            "delta": [0.5],
        }
    )


def _healthy_redis_mock() -> AsyncMock:
    """Return an AsyncMock Redis with healthy stream state."""
    redis = make_healthy_redis()
    # Override defaults to match integration test expectations
    redis.xinfo_stream = AsyncMock(return_value={"length": 100})
    redis.xinfo_groups = AsyncMock(return_value=[{"name": "heber-writers", "pending": 0, "consumers": 1}])
    return redis


def _build_context(tmp_path: Path, redis_mock: AsyncMock) -> CheckContext:
    """Build a CheckContext with real HealthCalendar, real HealthStore, mock Redis."""
    settings = Settings(data_root=str(tmp_path))
    store = HealthStore(data_root=tmp_path)
    calendar = HealthCalendar()
    reader = MagicMock()
    return CheckContext(
        settings=settings,
        reader=reader,
        redis=redis_mock,
        calendar=calendar,
        store=store,
    )


def _find_results(results: list, *, check_name: str | None = None, feed: str | None = None) -> list:
    """Filter results by check_name and/or feed."""
    out = results
    if check_name is not None:
        out = [r for r in out if r.check_name == check_name]
    if feed is not None:
        out = [r for r in out if r.feed == feed]
    return out


@pytest.mark.integration
@patch(
    "heber.health_monitor.checks.partition._now_et",
    return_value=MARKET_HOURS_DT,
)
async def test_full_check_cycle(_mock_now_partition: MagicMock, tmp_path: Path) -> None:
    """Run all check tiers against synthetic data with intentional gaps."""

    # ── 1. Create synthetic Silver data ──────────────────────────────────
    silver_root = tmp_path / "silver"
    silver_root.mkdir(parents=True, exist_ok=True)

    _create_silver_bars(silver_root)
    _create_silver_trades_empty(silver_root)
    # quotes: intentionally NOT created (missing partition)

    # ── 2. Build context ─────────────────────────────────────────────────
    redis_mock = _healthy_redis_mock()
    ctx = _build_context(tmp_path, redis_mock)

    # Mock reader.read_gold to return leakage data for meta_label_features
    gold_df_leakage = _create_gold_with_leakage(tmp_path)
    gold_df_empty = pd.DataFrame()

    def _fake_read_gold(dataset: str, **kwargs) -> pd.DataFrame:
        if dataset == "meta_label_features":
            return gold_df_leakage
        return gold_df_empty

    ctx.reader.read_gold = MagicMock(side_effect=_fake_read_gold)

    # ── 3. Run partition checks ──────────────────────────────────────────
    partition_results = await run_partition_checks(ctx, check_date=CHECK_DATE)
    assert len(partition_results) > 0, "Partition checks should produce results"

    # bars: has data -> at least one PASS or hourly check
    bars_results = _find_results(partition_results, feed="bars")
    assert len(bars_results) > 0, "Expected partition results for bars"
    # bars directory has data, but no hour= subdirectories for an hourly feed,
    # so the exact status depends on hourly sub-check logic. The key point is
    # bars is NOT a FAIL with "Missing dt= partition".
    bars_missing = [r for r in bars_results if r.status == Status.FAIL and "Missing dt=" in r.message]
    assert len(bars_missing) == 0, "bars partition exists and should not be missing"

    # trades: directory exists but empty -> WARN
    trades_results = _find_results(partition_results, feed="trades")
    assert len(trades_results) > 0, "Expected partition results for trades"
    trades_warn = [r for r in trades_results if r.status == Status.WARN]
    assert len(trades_warn) > 0, "trades should WARN (empty partition)"

    # quotes: directory missing -> FAIL
    quotes_results = _find_results(partition_results, feed="quotes")
    assert len(quotes_results) > 0, "Expected partition results for quotes"
    quotes_fail = [r for r in quotes_results if r.status == Status.FAIL]
    assert len(quotes_fail) > 0, "quotes should FAIL (missing partition)"

    # ── 4. Run stream health checks ──────────────────────────────────────
    with patch(
        "heber.health_monitor.checks.stream_health._now_et",
        return_value=MARKET_HOURS_DT,
    ):
        stream_results = await run_stream_health_checks(ctx)

    assert len(stream_results) > 0, "Stream health checks should produce results"
    # With healthy mock Redis: stream reachable, consumer lag healthy, DLQ empty
    stream_statuses = {r.check_name: r.status for r in stream_results}
    assert stream_statuses["stream_reachable"] == Status.PASS
    assert stream_statuses["consumer_lag"] == Status.PASS
    assert stream_statuses["dlq_depth"] == Status.PASS

    # ── 5. Run ML readiness checks ───────────────────────────────────────
    with patch(
        "heber.health_monitor.checks.ml_readiness._now_et",
        return_value=MARKET_HOURS_DT,
    ):
        ml_results = await run_ml_readiness_checks(ctx, check_date=CHECK_DATE)

    assert len(ml_results) > 0, "ML readiness checks should produce results"

    # Leakage check: meta_label_features has ts_available < ts_event -> FAIL
    leakage_results = _find_results(ml_results, check_name="ml_leakage_audit", feed="meta_label_features")
    assert len(leakage_results) == 1, "Expected one leakage result for meta_label_features"
    assert leakage_results[0].status == Status.FAIL, "Leakage violation should FAIL"
    assert "violation" in leakage_results[0].message.lower()

    # ── 6. Verify results written to store ───────────────────────────────
    all_results = partition_results + stream_results + ml_results
    ctx.store.write_results(all_results, CHECK_DATE)

    stored = ctx.store.read_results(CHECK_DATE)
    assert len(stored) > 0, "Store should contain written results"
    assert len(stored) == len(all_results), "All results should be persisted"
    assert "check_name" in stored.columns
    assert "status" in stored.columns
