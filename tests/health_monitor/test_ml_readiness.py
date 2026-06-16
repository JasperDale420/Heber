"""Tests for Tier 3 — ML Readiness Checks."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from heber.health_monitor.checks.ml_readiness import compute_psi, run_ml_readiness_checks
from heber.health_monitor.models import Severity, Status
from tests.health_monitor.conftest import (
    MARKET_OPEN_DT,
    TRADING_DAY,
    make_check_context,
)


def _make_ctx(
    tmp_path: Path,
    calendar: MagicMock | None = None,
    store: MagicMock | None = None,
    reader: MagicMock | None = None,
    settings_overrides: dict | None = None,
):
    return make_check_context(
        tmp_path, calendar=calendar, store=store, reader=reader, settings_overrides=settings_overrides
    )


def _gold_df_no_leakage(n: int = 50) -> pd.DataFrame:
    """Gold DataFrame where ts_available >= ts_event (no leakage)."""
    ts_event = pd.date_range("2026-03-25 09:30", periods=n, freq="1min", tz="UTC")
    ts_available = ts_event + pd.Timedelta(seconds=5)
    return pd.DataFrame(
        {
            "ts_event": ts_event,
            "ts_available": ts_available,
            "instrument_key": [f"equity:SYM{i % 5}" for i in range(n)],
            "value": np.random.default_rng(42).normal(0, 1, n),
        }
    )


def _gold_df_with_leakage(n: int = 50, violations: int = 3) -> pd.DataFrame:
    """Gold DataFrame with some ts_available < ts_event violations."""
    ts_event = pd.date_range("2026-03-25 09:30", periods=n, freq="1min", tz="UTC")
    ts_available = ts_event + pd.Timedelta(seconds=5)
    # Create violations: set ts_available before ts_event
    df = pd.DataFrame(
        {
            "ts_event": ts_event,
            "ts_available": ts_available,
            "instrument_key": [f"equity:SYM{i % 5}" for i in range(n)],
            "value": np.random.default_rng(42).normal(0, 1, n),
        }
    )
    for i in range(violations):
        df.loc[i, "ts_available"] = df.loc[i, "ts_event"] - pd.Timedelta(seconds=10)
    return df


def _labels_df(label_dist: dict[int, int]) -> pd.DataFrame:
    """Create a labels DataFrame with a given label distribution."""
    rows = []
    for label_val, count in label_dist.items():
        for _ in range(count):
            rows.append({"label": label_val, "instrument_key": "equity:AAPL", "ts_event": "2026-03-25"})
    return pd.DataFrame(rows)


def _features_df(n: int = 100, null_pct: float = 0.0) -> pd.DataFrame:
    """Create a feature DataFrame with optional null injection."""
    rng = np.random.default_rng(42)
    df = pd.DataFrame(
        {
            "instrument_key": [f"equity:SYM{i % 10}" for i in range(n)],
            "feature_a": rng.normal(0, 1, n),
            "feature_b": rng.normal(0, 1, n),
            "ts_event": pd.date_range("2026-03-25 09:30", periods=n, freq="1min", tz="UTC"),
        }
    )
    if null_pct > 0:
        null_count = int(n * null_pct)
        null_idx = rng.choice(n, null_count, replace=False)
        df.loc[null_idx, "feature_a"] = np.nan
    return df


# --- PSI unit test ---


@pytest.mark.unit
def test_compute_psi_identical_distributions() -> None:
    """Identical distributions produce PSI ~ 0."""
    dist = np.array([100, 200, 300])
    assert compute_psi(dist, dist) < 0.01


@pytest.mark.unit
def test_compute_psi_shifted_distribution() -> None:
    """Significantly shifted distribution produces PSI > 0.2."""
    expected = np.array([100, 100, 100])
    actual = np.array([10, 100, 300])
    psi = compute_psi(expected, actual)
    assert psi > 0.2


# --- 11a: Zero-Leakage Audit ---


@pytest.mark.unit
@patch("heber.health_monitor.checks.ml_readiness._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.ml_readiness.GOLD_DATASETS_TO_AUDIT", ["test_features"])
async def test_no_leakage_pass(mock_now: MagicMock, tmp_path: Path) -> None:
    """No leakage violations results in PASS."""
    reader = MagicMock()
    reader.read_gold = MagicMock(return_value=_gold_df_no_leakage(50))

    ctx = _make_ctx(tmp_path, reader=reader)
    results = await run_ml_readiness_checks(ctx, check_date=TRADING_DAY)

    leakage_results = [r for r in results if r.check_name == "ml_leakage_audit"]
    assert len(leakage_results) >= 1
    for r in leakage_results:
        assert r.status == Status.PASS


@pytest.mark.unit
@patch("heber.health_monitor.checks.ml_readiness._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.ml_readiness.GOLD_DATASETS_TO_AUDIT", ["test_features"])
async def test_leakage_violation_fail(mock_now: MagicMock, tmp_path: Path) -> None:
    """ts_available < ts_event violations result in P0 FAIL."""
    reader = MagicMock()
    reader.read_gold = MagicMock(return_value=_gold_df_with_leakage(50, violations=3))

    ctx = _make_ctx(tmp_path, reader=reader)
    results = await run_ml_readiness_checks(ctx, check_date=TRADING_DAY)

    leakage_results = [r for r in results if r.check_name == "ml_leakage_audit"]
    assert len(leakage_results) == 1
    assert leakage_results[0].status == Status.FAIL
    assert leakage_results[0].severity == Severity.P0_CRITICAL
    assert leakage_results[0].details["violation_count"] == 3


# --- 11b: Label Distribution Stability ---


@pytest.mark.unit
@patch("heber.health_monitor.checks.ml_readiness._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.ml_readiness.GOLD_DATASETS_TO_AUDIT", [])
async def test_label_stable_psi_pass(mock_now: MagicMock, tmp_path: Path) -> None:
    """Stable label distribution (PSI < threshold) results in PASS."""
    reader = MagicMock()
    # Today's labels: similar distribution to baseline
    reader.read_gold = MagicMock(return_value=_labels_df({1: 40, 0: 30, -1: 30}))

    # Baseline with similar proportions
    baseline = pd.DataFrame(
        [
            {"label_value": 1, "count": 42, "proportion": 0.42},
            {"label_value": 0, "count": 28, "proportion": 0.28},
            {"label_value": -1, "count": 30, "proportion": 0.30},
        ]
    )
    store = MagicMock()
    store.read_baselines = MagicMock(return_value=baseline)
    store.write_baseline = MagicMock()

    ctx = _make_ctx(tmp_path, reader=reader, store=store)
    results = await run_ml_readiness_checks(ctx, check_date=TRADING_DAY)

    label_results = [r for r in results if r.check_name == "ml_label_stability"]
    assert len(label_results) == 1
    assert label_results[0].status == Status.PASS


@pytest.mark.unit
@patch("heber.health_monitor.checks.ml_readiness._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.ml_readiness.GOLD_DATASETS_TO_AUDIT", [])
async def test_label_psi_above_threshold_warn(mock_now: MagicMock, tmp_path: Path) -> None:
    """PSI above threshold results in WARN."""
    reader = MagicMock()
    # Today: heavily skewed
    reader.read_gold = MagicMock(return_value=_labels_df({1: 90, 0: 5, -1: 5}))

    # Baseline: uniform
    baseline = pd.DataFrame(
        [
            {"label_value": 1, "count": 33, "proportion": 0.33},
            {"label_value": 0, "count": 34, "proportion": 0.34},
            {"label_value": -1, "count": 33, "proportion": 0.33},
        ]
    )
    store = MagicMock()
    store.read_baselines = MagicMock(return_value=baseline)
    store.write_baseline = MagicMock()

    ctx = _make_ctx(tmp_path, reader=reader, store=store)
    results = await run_ml_readiness_checks(ctx, check_date=TRADING_DAY)

    label_results = [r for r in results if r.check_name == "ml_label_stability"]
    assert len(label_results) == 1
    assert label_results[0].status == Status.WARN
    assert label_results[0].severity == Severity.P1_WARNING


# --- 11c: Feature Null Rates ---


@pytest.mark.unit
@patch("heber.health_monitor.checks.ml_readiness._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.ml_readiness.GOLD_DATASETS_TO_AUDIT", [])
async def test_feature_null_above_threshold_warn(mock_now: MagicMock, tmp_path: Path) -> None:
    """Feature null rate above threshold results in WARN."""
    reader = MagicMock()
    # 20% nulls in feature_a
    features = _features_df(n=100, null_pct=0.20)

    def read_gold_side_effect(dataset, **kwargs):
        if dataset == "labels_alert_barriers":
            return pd.DataFrame()
        if dataset == "meta_label_features":
            return features
        return pd.DataFrame()

    reader.read_gold = MagicMock(side_effect=read_gold_side_effect)

    ctx = _make_ctx(tmp_path, reader=reader)
    results = await run_ml_readiness_checks(ctx, check_date=TRADING_DAY)

    null_results = [r for r in results if r.check_name == "ml_feature_nulls" and r.status == Status.WARN]
    assert len(null_results) >= 1


# --- 11d: No Gold data graceful skip ---


@pytest.mark.unit
@patch("heber.health_monitor.checks.ml_readiness._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.ml_readiness.GOLD_DATASETS_TO_AUDIT", ["test_features"])
async def test_no_gold_data_graceful_skip(mock_now: MagicMock, tmp_path: Path) -> None:
    """No Gold data available results in graceful skip (INFO)."""
    reader = MagicMock()
    reader.read_gold = MagicMock(return_value=pd.DataFrame())

    ctx = _make_ctx(tmp_path, reader=reader)
    results = await run_ml_readiness_checks(ctx, check_date=TRADING_DAY)

    # Should have INFO results, no crashes
    for r in results:
        assert r.status in (Status.PASS, Status.WARN), f"Unexpected {r.status} for {r.check_name}: {r.message}"
