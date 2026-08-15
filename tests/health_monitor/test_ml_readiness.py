"""Tests for Tier 3 — ML Readiness Checks."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from heber.health_monitor.checks.ml_readiness import LABEL_COLUMN, compute_psi, run_ml_readiness_checks
from heber.health_monitor.models import Severity, Status
from tests.health_monitor.conftest import (
    MARKET_OPEN_DT,
    TRADING_DAY,
    make_check_context,
    make_store_mock,
)


def _make_ctx(
    tmp_path: Path,
    calendar: MagicMock | None = None,
    store: MagicMock | None = None,
    reader: Any | None = None,
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
            rows.append({LABEL_COLUMN: label_val, "instrument_key": "equity:AAPL", "ts_event": "2026-03-25"})
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


@pytest.mark.unit
@patch("heber.health_monitor.checks.ml_readiness._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.ml_readiness.GOLD_DATASETS_TO_AUDIT", ["test_features"])
async def test_leakage_read_failure_is_error_not_pass(mock_now: MagicMock, tmp_path: Path) -> None:
    """A failed Gold read must NOT report a clean PASS on the zero-leakage guardrail."""
    reader = MagicMock()
    reader.read_gold = MagicMock(side_effect=OSError("corrupt parquet"))

    ctx = _make_ctx(tmp_path, reader=reader)
    results = await run_ml_readiness_checks(ctx, check_date=TRADING_DAY)

    leakage_results = [r for r in results if r.check_name == "ml_leakage_audit"]
    assert len(leakage_results) == 1
    assert leakage_results[0].status == Status.ERROR
    assert leakage_results[0].status != Status.PASS


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


@pytest.mark.unit
@patch("heber.health_monitor.checks.ml_readiness._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.ml_readiness.GOLD_DATASETS_TO_AUDIT", [])
async def test_fully_quarantined_day_warns_instead_of_skipping(mock_now: MagicMock, tmp_path: Path) -> None:
    """An empty feature read is not benign when the day's rows were quarantined.

    The reader excludes `_quarantine`, so a gateway outage that null-Greeks
    every row of a day makes the feature read come back empty. Without this,
    losing an entire training day reports as a clean PASS.
    """
    reader = MagicMock()
    reader.read_gold = MagicMock(return_value=pd.DataFrame())

    quarantine_dir = (
        tmp_path
        / "gold"
        / "dataset=meta_label_features"
        / "project=watch"
        / "version=v1"
        / "_quarantine"
        / "all_greeks_null"
        / f"dt={TRADING_DAY.isoformat()}"
    )
    quarantine_dir.mkdir(parents=True)
    pd.DataFrame([{"alert_id": "a1", "delta": None}]).to_parquet(quarantine_dir / "q.parquet", index=False)

    ctx = _make_ctx(tmp_path, reader=reader)
    results = await run_ml_readiness_checks(ctx, check_date=TRADING_DAY)

    quarantine_results = [r for r in results if r.check_name == "ml_feature_nulls" and r.feed == "meta_label_features"]
    assert len(quarantine_results) == 1
    assert quarantine_results[0].status == Status.WARN
    assert quarantine_results[0].severity == Severity.P1_WARNING


@pytest.mark.unit
@patch("heber.health_monitor.checks.ml_readiness._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.ml_readiness.GOLD_DATASETS_TO_AUDIT", [])
async def test_quarantine_alongside_clean_partition_does_not_warn(mock_now: MagicMock, tmp_path: Path) -> None:
    """Quarantined rows are normal — only a day with NO usable rows is a problem.

    Guards against a daily false alarm: the audit read is bounded to midnight
    UTC on both ends, so it comes back empty even on days that wrote plenty of
    intraday rows. Emptiness alone must not be read as total loss.
    """
    reader = MagicMock()
    reader.read_gold = MagicMock(return_value=pd.DataFrame())

    version_root = tmp_path / "gold" / "dataset=meta_label_features" / "project=watch" / "version=v1"
    canonical = version_root / f"dt={TRADING_DAY.isoformat()}"
    canonical.mkdir(parents=True)
    pd.DataFrame([{"alert_id": "clean", "delta": 0.5}]).to_parquet(canonical / "data.parquet", index=False)
    quarantine_dir = version_root / "_quarantine" / "all_greeks_null" / f"dt={TRADING_DAY.isoformat()}"
    quarantine_dir.mkdir(parents=True)
    pd.DataFrame([{"alert_id": "bad", "delta": None}]).to_parquet(quarantine_dir / "q.parquet", index=False)

    ctx = _make_ctx(tmp_path, reader=reader)
    results = await run_ml_readiness_checks(ctx, check_date=TRADING_DAY)

    null_results = [r for r in results if r.check_name == "ml_feature_nulls" and r.feed == "meta_label_features"]
    assert len(null_results) == 1
    assert null_results[0].status == Status.PASS


@pytest.mark.unit
@patch("heber.health_monitor.checks.ml_readiness._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.ml_readiness.GOLD_DATASETS_TO_AUDIT", [])
async def test_failed_feature_read_is_error_not_pass(mock_now: MagicMock, tmp_path: Path) -> None:
    """A read that FAILED is not 'no data' — never report it as a clean PASS."""
    reader = MagicMock()
    reader.read_gold = MagicMock(side_effect=OSError("volume unavailable"))

    ctx = _make_ctx(tmp_path, reader=reader)
    results = await run_ml_readiness_checks(ctx, check_date=TRADING_DAY)

    null_results = [r for r in results if r.check_name == "ml_feature_nulls" and r.feed == "meta_label_features"]
    assert len(null_results) == 1
    assert null_results[0].status == Status.ERROR
    assert null_results[0].severity == Severity.P1_WARNING


def _write_quarantine_and_canonical(
    tmp_path: Path,
    *,
    canonical_project: str,
    canonical_rows: list[dict],
) -> None:
    """Quarantine under project=watch/version=v1 plus a canonical partition elsewhere."""
    dataset_root = tmp_path / "gold" / "dataset=meta_label_features"
    quarantine_dir = (
        dataset_root / "project=watch" / "version=v1" / "_quarantine" / "all_greeks_null" / f"dt={TRADING_DAY}"
    )
    quarantine_dir.mkdir(parents=True)
    pd.DataFrame([{"alert_id": "bad", "delta": None}]).to_parquet(quarantine_dir / "q.parquet", index=False)

    canonical = dataset_root / f"project={canonical_project}" / "version=v1" / f"dt={TRADING_DAY}"
    canonical.mkdir(parents=True)
    pd.DataFrame(canonical_rows, columns=["alert_id", "delta"]).to_parquet(canonical / "data.parquet", index=False)


@pytest.mark.unit
@patch("heber.health_monitor.checks.ml_readiness._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.ml_readiness.GOLD_DATASETS_TO_AUDIT", [])
async def test_canonical_rows_under_other_project_do_not_suppress_warning(mock_now: MagicMock, tmp_path: Path) -> None:
    """Another project's healthy day says nothing about this one's."""
    reader = MagicMock()
    reader.read_gold = MagicMock(return_value=pd.DataFrame())
    _write_quarantine_and_canonical(
        tmp_path, canonical_project="other", canonical_rows=[{"alert_id": "clean", "delta": 0.5}]
    )

    ctx = _make_ctx(tmp_path, reader=reader)
    results = await run_ml_readiness_checks(ctx, check_date=TRADING_DAY)

    null_results = [r for r in results if r.check_name == "ml_feature_nulls" and r.feed == "meta_label_features"]
    assert [r.status for r in null_results] == [Status.WARN]


@pytest.mark.unit
@patch("heber.health_monitor.checks.ml_readiness._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.ml_readiness.GOLD_DATASETS_TO_AUDIT", [])
async def test_empty_canonical_file_does_not_suppress_warning(mock_now: MagicMock, tmp_path: Path) -> None:
    """A canonical file with no rows in it is not usable training data."""
    reader = MagicMock()
    reader.read_gold = MagicMock(return_value=pd.DataFrame())
    _write_quarantine_and_canonical(tmp_path, canonical_project="watch", canonical_rows=[])

    ctx = _make_ctx(tmp_path, reader=reader)
    results = await run_ml_readiness_checks(ctx, check_date=TRADING_DAY)

    null_results = [r for r in results if r.check_name == "ml_feature_nulls" and r.feed == "meta_label_features"]
    assert [r.status for r in null_results] == [Status.WARN]


# --- The audit window must actually cover the trading day ---


def _write_gold_day(tmp_path: Path, dataset: str, rows: pd.DataFrame) -> None:
    """Write one Gold partition the real HeberReader can read."""
    part = tmp_path / "gold" / f"dataset={dataset}" / "project=watch" / "version=v1" / f"dt={TRADING_DAY.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    rows.to_parquet(part / "data.parquet", index=False)


def _intraday_rows(n: int, *, null_pct: float = 0.0, leak: bool = False) -> pd.DataFrame:
    """Rows stamped during market hours — 13:30Z is 09:30 ET, not midnight."""
    ts_event = pd.date_range(f"{TRADING_DAY.isoformat()} 13:30", periods=n, freq="1min", tz="UTC")
    ts_available = ts_event - pd.Timedelta(seconds=5) if leak else ts_event + pd.Timedelta(seconds=5)
    values = [None if i < int(n * null_pct) else float(i) for i in range(n)]
    return pd.DataFrame(
        {
            "ts_event": ts_event,
            "ts_available": ts_available,
            "instrument_key": [f"equity:SYM{i % 5}" for i in range(n)],
            "feature_a": values,
        }
    )


@pytest.mark.unit
@patch("heber.health_monitor.checks.ml_readiness._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.ml_readiness.GOLD_DATASETS_TO_AUDIT", [])
async def test_null_check_sees_intraday_rows(mock_now: MagicMock, tmp_path: Path) -> None:
    """The audit read must span the whole day, not just the midnight instant."""
    from heber.reader import HeberReader

    _write_gold_day(tmp_path, "meta_label_features", _intraday_rows(100, null_pct=0.20))

    ctx = _make_ctx(tmp_path, reader=HeberReader(tmp_path))
    results = await run_ml_readiness_checks(ctx, check_date=TRADING_DAY)

    null_results = [r for r in results if r.check_name == "ml_feature_nulls" and r.feed == "meta_label_features"]
    assert [r.status for r in null_results] == [Status.WARN]
    assert "skipping" not in null_results[0].message


@pytest.mark.unit
@patch("heber.health_monitor.checks.ml_readiness._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.ml_readiness.GOLD_DATASETS_TO_AUDIT", ["meta_label_features"])
async def test_leakage_audit_sees_intraday_rows(mock_now: MagicMock, tmp_path: Path) -> None:
    """The zero-leakage guardrail is worthless if its read window matches nothing."""
    from heber.reader import HeberReader

    _write_gold_day(tmp_path, "meta_label_features", _intraday_rows(10, leak=True))

    ctx = _make_ctx(tmp_path, reader=HeberReader(tmp_path))
    results = await run_ml_readiness_checks(ctx, check_date=TRADING_DAY)

    leakage = [r for r in results if r.check_name == "ml_leakage_audit" and r.feed == "meta_label_features"]
    assert [r.status for r in leakage] == [Status.FAIL]
    assert leakage[0].details["violation_count"] == 10


@pytest.mark.unit
@patch("heber.health_monitor.checks.ml_readiness._now_et", return_value=MARKET_OPEN_DT)
async def test_leakage_audit_covers_the_real_label_dataset(mock_now: MagicMock, tmp_path: Path) -> None:
    """The audited names must be datasets that exist, or the guardrail is decorative."""
    from heber.reader import HeberReader

    _write_gold_day(tmp_path, "labels_alert_barriers", _intraday_rows(10, leak=True))

    ctx = _make_ctx(tmp_path, reader=HeberReader(tmp_path))
    results = await run_ml_readiness_checks(ctx, check_date=TRADING_DAY)

    leakage = [r for r in results if r.check_name == "ml_leakage_audit" and r.feed == "labels_alert_barriers"]
    assert [r.status for r in leakage] == [Status.FAIL]
    assert leakage[0].details["violation_count"] == 10


# --- Required vs optional feature columns ---


def _feature_frame(**cols) -> pd.DataFrame:
    n = 100
    base = _intraday_rows(n)
    for name, null_pct in cols.items():
        base[name] = [None if i < int(n * null_pct) else float(i) for i in range(n)]
    return base.drop(columns=["feature_a"])


@pytest.mark.unit
@patch("heber.health_monitor.checks.ml_readiness._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.ml_readiness.GOLD_DATASETS_TO_AUDIT", [])
async def test_enrichment_columns_report_without_warning(mock_now: MagicMock, tmp_path: Path) -> None:
    """Greeks and market context come from outbound calls — null means the call
    did not land, which the enrichment backfill already tracks. Reporting the
    rate is useful; paging on it every run is not."""
    from heber.reader import HeberReader

    _write_gold_day(tmp_path, "meta_label_features", _feature_frame(delta=0.44, gex=0.39, realized_vol_20d=0.44))

    ctx = _make_ctx(tmp_path, reader=HeberReader(tmp_path))
    results = await run_ml_readiness_checks(ctx, check_date=TRADING_DAY)

    nulls = [r for r in results if r.check_name == "ml_feature_nulls"]
    assert nulls, "expected the null check to run"
    assert all(r.status == Status.PASS for r in nulls), [r.message for r in nulls if r.status != Status.PASS]
    reported = {r.details["column"]: r.details["null_pct"] for r in nulls}
    assert reported["delta"] == pytest.approx(0.44)


@pytest.mark.unit
@patch("heber.health_monitor.checks.ml_readiness._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.ml_readiness.GOLD_DATASETS_TO_AUDIT", [])
async def test_payload_columns_still_warn(mock_now: MagicMock, tmp_path: Path) -> None:
    """A null strike or premium means the writer is broken — that must still page."""
    from heber.reader import HeberReader

    _write_gold_day(tmp_path, "meta_label_features", _feature_frame(strike=0.20, delta=0.44))

    ctx = _make_ctx(tmp_path, reader=HeberReader(tmp_path))
    results = await run_ml_readiness_checks(ctx, check_date=TRADING_DAY)

    warned = [r for r in results if r.check_name == "ml_feature_nulls" and r.status == Status.WARN]
    assert [r.details["column"] for r in warned] == ["strike"]
    assert warned[0].severity == Severity.P1_WARNING


@pytest.mark.unit
def test_optional_columns_cover_every_enrichable_field() -> None:
    """Keeps the two lists from drifting apart as enrichment fields are added."""
    from heber.health_monitor.checks.ml_readiness import OPTIONAL_FEATURE_COLUMNS
    from heber.watch.backfill_scanner import ENRICHABLE_FIELDS

    assert set(ENRICHABLE_FIELDS) <= OPTIONAL_FEATURE_COLUMNS


# --- PSI needs a label column that exists ---


@pytest.mark.unit
@patch("heber.health_monitor.checks.ml_readiness._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.ml_readiness.GOLD_DATASETS_TO_AUDIT", [])
async def test_psi_reads_the_barrier_outcome(mock_now: MagicMock, tmp_path: Path) -> None:
    """labels_alert_barriers has no 'label' column — PSI must target a real one."""
    from heber.reader import HeberReader

    rows = _intraday_rows(60).drop(columns=["feature_a"])
    rows["outcome"] = (["expired"] * 40) + (["hit_sl"] * 15) + (["hit_tp"] * 5)
    _write_gold_day(tmp_path, "labels_alert_barriers", rows)

    store = make_store_mock()
    ctx = _make_ctx(tmp_path, reader=HeberReader(tmp_path), store=store)
    results = await run_ml_readiness_checks(ctx, check_date=TRADING_DAY)

    psi = [r for r in results if r.check_name == "ml_label_stability"]
    assert len(psi) == 1
    assert "skipping" not in psi[0].message
    assert psi[0].details["distribution"] == {"expired": 40, "hit_sl": 15, "hit_tp": 5}
    store.write_baseline.assert_called_once()


@pytest.mark.unit
@patch("heber.health_monitor.checks.ml_readiness._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.ml_readiness.GOLD_DATASETS_TO_AUDIT", [])
async def test_total_enrichment_outage_still_warns(mock_now: MagicMock, tmp_path: Path) -> None:
    """Not paging on a 44%-null Greek is the point; not paging on a 100%-null one is a hole.

    The Greek quarantine only catches rows where ALL five are null, so a single
    upstream going dark leaves no other signal.
    """
    from heber.reader import HeberReader

    _write_gold_day(tmp_path, "meta_label_features", _feature_frame(delta=1.0, gamma=0.44))

    ctx = _make_ctx(tmp_path, reader=HeberReader(tmp_path))
    results = await run_ml_readiness_checks(ctx, check_date=TRADING_DAY)

    warned = [r for r in results if r.check_name == "ml_feature_nulls" and r.status == Status.WARN]
    assert [r.details["column"] for r in warned] == ["delta"]
    assert warned[0].severity == Severity.P1_WARNING


@pytest.mark.unit
@patch("heber.health_monitor.checks.ml_readiness._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.ml_readiness.GOLD_DATASETS_TO_AUDIT", [])
async def test_psi_baseline_written_under_its_own_key(mock_now: MagicMock, tmp_path: Path) -> None:
    """Reads use baseline_key='label_dist' — writes must too, or the baseline
    freezes on day one and the rolling window never moves."""
    reader = MagicMock()
    reader.read_gold = MagicMock(return_value=_labels_df({1: 40, 0: 30, -1: 30}))
    store = MagicMock()
    store.read_baselines = MagicMock(
        return_value=pd.DataFrame(
            [
                {"label_value": 1, "count": 42, "proportion": 0.42},
                {"label_value": 0, "count": 28, "proportion": 0.28},
                {"label_value": -1, "count": 30, "proportion": 0.30},
            ]
        )
    )
    store.write_baseline = MagicMock()

    ctx = _make_ctx(tmp_path, reader=reader, store=store)
    await run_ml_readiness_checks(ctx, check_date=TRADING_DAY)

    assert store.write_baseline.call_args.kwargs["baseline_key"] == "label_dist"


@pytest.mark.unit
@patch("heber.health_monitor.checks.ml_readiness._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.ml_readiness.GOLD_DATASETS_TO_AUDIT", [])
async def test_psi_averages_a_multi_day_baseline(mock_now: MagicMock, tmp_path: Path) -> None:
    """read_baselines returns one row per label per day — a 30-day window is
    many rows per label, not one."""
    reader = MagicMock()
    reader.read_gold = MagicMock(return_value=_labels_df({1: 40, 0: 30, -1: 30}))
    store = MagicMock()
    store.read_baselines = MagicMock(
        return_value=pd.DataFrame(
            [
                {"label_value": 1, "count": 40, "proportion": 0.30, "dt": "2026-03-23"},
                {"label_value": 0, "count": 30, "proportion": 0.40, "dt": "2026-03-23"},
                {"label_value": -1, "count": 30, "proportion": 0.30, "dt": "2026-03-23"},
                {"label_value": 1, "count": 50, "proportion": 0.50, "dt": "2026-03-24"},
                {"label_value": 0, "count": 20, "proportion": 0.20, "dt": "2026-03-24"},
                {"label_value": -1, "count": 30, "proportion": 0.30, "dt": "2026-03-24"},
            ]
        )
    )
    store.write_baseline = MagicMock()

    ctx = _make_ctx(tmp_path, reader=reader, store=store)
    results = await run_ml_readiness_checks(ctx, check_date=TRADING_DAY)

    psi = [r for r in results if r.check_name == "ml_label_stability"]
    assert len(psi) == 1
    assert "psi" in psi[0].details, psi[0].message
    # Baseline for label 1 is mean(0.30, 0.50) = 0.40 against today's 0.40 -> stable.
    assert psi[0].status == Status.PASS
