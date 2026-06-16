"""Tests for Tier 3 — Schema Drift Detection."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.health_monitor.checks.schema import run_schema_checks
from heber.health_monitor.models import Severity, Status
from tests.health_monitor.conftest import (
    MARKET_OPEN_DT,
    TRADING_DAY,
    WEEKEND_DAY,
    make_check_context,
)


def _write_parquet_with_schema(path: Path, schema: pa.Schema, num_rows: int = 5) -> None:
    """Write a Parquet file with the given Arrow schema."""
    path.parent.mkdir(parents=True, exist_ok=True)
    arrays = []
    for field in schema:
        if pa.types.is_int64(field.type):
            arrays.append(pa.array(list(range(num_rows)), type=pa.int64()))
        elif pa.types.is_float64(field.type):
            arrays.append(pa.array([float(i) for i in range(num_rows)], type=pa.float64()))
        else:
            arrays.append(pa.array([f"val{i}" for i in range(num_rows)], type=pa.string()))
    table = pa.table({field.name: arr for field, arr in zip(schema, arrays, strict=True)})
    pq.write_table(table, path)


def _make_ctx(
    tmp_path: Path,
    calendar: MagicMock | None = None,
    store: MagicMock | None = None,
):
    return make_check_context(tmp_path, calendar=calendar, store=store)


SCHEMA_V1 = pa.schema(
    [
        ("event_id", pa.string()),
        ("symbol", pa.string()),
        ("price", pa.float64()),
    ]
)

SCHEMA_V1_PLUS_COL = pa.schema(
    [
        ("event_id", pa.string()),
        ("symbol", pa.string()),
        ("price", pa.float64()),
        ("volume", pa.int64()),
    ]
)

SCHEMA_V1_MINUS_COL = pa.schema(
    [
        ("event_id", pa.string()),
        ("price", pa.float64()),
    ]
)

SCHEMA_V1_TYPE_CHANGED = pa.schema(
    [
        ("event_id", pa.string()),
        ("symbol", pa.int64()),  # was string, now int64
        ("price", pa.float64()),
    ]
)


def _create_feed_with_schema(silver_root: Path, feed: str, dt: date, schema: pa.Schema) -> None:
    """Create a Silver partition with a specific schema."""
    dt_dir = silver_root / f"feed={feed}" / f"dt={dt.isoformat()}"
    _write_parquet_with_schema(dt_dir / "part-0.parquet", schema)


def _schema_baseline_df(feed: str, schema: pa.Schema) -> pd.DataFrame:
    """Create a schema baseline DataFrame from an Arrow schema."""
    import hashlib
    import json

    columns = sorted([(f.name, str(f.type)) for f in schema])
    columns_json = json.dumps(columns)
    schema_hash = hashlib.sha256(columns_json.encode()).hexdigest()
    return pd.DataFrame(
        [
            {
                "feed": feed,
                "schema_hash": schema_hash,
                "columns_json": columns_json,
            }
        ]
    )


@pytest.mark.unit
@patch("heber.health_monitor.checks.schema._now_et", return_value=MARKET_OPEN_DT)
async def test_no_drift_pass(mock_now: MagicMock, tmp_path: Path) -> None:
    """Same schema as baseline results in PASS."""
    silver_root = tmp_path / "silver"
    _create_feed_with_schema(silver_root, "bars", TRADING_DAY, SCHEMA_V1)

    baseline = _schema_baseline_df("bars", SCHEMA_V1)
    store = MagicMock()
    store.read_baselines = MagicMock(return_value=baseline)
    store.write_baseline = MagicMock()

    ctx = _make_ctx(tmp_path, store=store)
    results = await run_schema_checks(ctx, check_date=TRADING_DAY)

    bars_results = [r for r in results if r.feed == "bars"]
    assert len(bars_results) == 1
    assert bars_results[0].status == Status.PASS


@pytest.mark.unit
@patch("heber.health_monitor.checks.schema._now_et", return_value=MARKET_OPEN_DT)
async def test_new_column_added_warn(mock_now: MagicMock, tmp_path: Path) -> None:
    """New column added results in WARN (P1_WARNING)."""
    silver_root = tmp_path / "silver"
    _create_feed_with_schema(silver_root, "bars", TRADING_DAY, SCHEMA_V1_PLUS_COL)

    baseline = _schema_baseline_df("bars", SCHEMA_V1)
    store = MagicMock()
    store.read_baselines = MagicMock(return_value=baseline)
    store.write_baseline = MagicMock()

    ctx = _make_ctx(tmp_path, store=store)
    results = await run_schema_checks(ctx, check_date=TRADING_DAY)

    bars_results = [r for r in results if r.feed == "bars"]
    assert len(bars_results) == 1
    assert bars_results[0].status == Status.WARN
    assert bars_results[0].severity == Severity.P1_WARNING
    assert "added" in bars_results[0].message.lower()


@pytest.mark.unit
@patch("heber.health_monitor.checks.schema._now_et", return_value=MARKET_OPEN_DT)
async def test_column_removed_fail(mock_now: MagicMock, tmp_path: Path) -> None:
    """Column removed results in FAIL (P0_CRITICAL)."""
    silver_root = tmp_path / "silver"
    _create_feed_with_schema(silver_root, "bars", TRADING_DAY, SCHEMA_V1_MINUS_COL)

    baseline = _schema_baseline_df("bars", SCHEMA_V1)
    store = MagicMock()
    store.read_baselines = MagicMock(return_value=baseline)
    store.write_baseline = MagicMock()

    ctx = _make_ctx(tmp_path, store=store)
    results = await run_schema_checks(ctx, check_date=TRADING_DAY)

    bars_results = [r for r in results if r.feed == "bars"]
    assert len(bars_results) == 1
    assert bars_results[0].status == Status.FAIL
    assert bars_results[0].severity == Severity.P0_CRITICAL
    assert "removed" in bars_results[0].message.lower()


@pytest.mark.unit
@patch("heber.health_monitor.checks.schema._now_et", return_value=MARKET_OPEN_DT)
async def test_column_type_changed_fail(mock_now: MagicMock, tmp_path: Path) -> None:
    """Column type changed results in FAIL (P0_CRITICAL)."""
    silver_root = tmp_path / "silver"
    _create_feed_with_schema(silver_root, "bars", TRADING_DAY, SCHEMA_V1_TYPE_CHANGED)

    baseline = _schema_baseline_df("bars", SCHEMA_V1)
    store = MagicMock()
    store.read_baselines = MagicMock(return_value=baseline)
    store.write_baseline = MagicMock()

    ctx = _make_ctx(tmp_path, store=store)
    results = await run_schema_checks(ctx, check_date=TRADING_DAY)

    bars_results = [r for r in results if r.feed == "bars"]
    assert len(bars_results) == 1
    assert bars_results[0].status == Status.FAIL
    assert bars_results[0].severity == Severity.P0_CRITICAL
    assert "type" in bars_results[0].message.lower()


@pytest.mark.unit
@patch("heber.health_monitor.checks.schema._now_et", return_value=MARKET_OPEN_DT)
async def test_first_run_no_baseline_pass(mock_now: MagicMock, tmp_path: Path) -> None:
    """First run with no previous baseline stores current and returns PASS."""
    silver_root = tmp_path / "silver"
    _create_feed_with_schema(silver_root, "bars", TRADING_DAY, SCHEMA_V1)

    store = MagicMock()
    store.read_baselines = MagicMock(return_value=pd.DataFrame())
    store.write_baseline = MagicMock()

    ctx = _make_ctx(tmp_path, store=store)
    results = await run_schema_checks(ctx, check_date=TRADING_DAY)

    bars_results = [r for r in results if r.feed == "bars"]
    assert len(bars_results) == 1
    assert bars_results[0].status == Status.PASS
    assert "first" in bars_results[0].message.lower() or "initial" in bars_results[0].message.lower()

    # Verify baseline was written
    store.write_baseline.assert_called_once()


@pytest.mark.unit
@patch("heber.health_monitor.checks.schema._now_et", return_value=MARKET_OPEN_DT)
async def test_non_trading_day_empty(mock_now: MagicMock, tmp_path: Path) -> None:
    """On a non-trading day, return empty results."""
    calendar = MagicMock()
    calendar.is_trading_day = MagicMock(return_value=False)
    ctx = _make_ctx(tmp_path, calendar=calendar)

    results = await run_schema_checks(ctx, check_date=WEEKEND_DAY)
    assert results == []
