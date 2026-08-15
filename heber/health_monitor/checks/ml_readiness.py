"""Tier 3 — ML Readiness Checks.

Four sub-checks ensuring Gold layer data is ready for ML consumption:
  a) Zero-leakage audit (ts_available >= ts_event)
  b) Label distribution stability (PSI)
  c) Feature null rates
  d) Cross-sectional completeness
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import structlog

from heber.health_monitor.metrics import health_leakage_violations, health_null_rate
from heber.health_monitor.models import CheckContext, CheckResult, Severity, Status

logger = structlog.get_logger(__name__)

ET = ZoneInfo("America/New_York")

# Gold datasets to audit for zero-leakage. Can be extended as more Gold datasets are added.
GOLD_DATASETS_TO_AUDIT: list[str] = [
    "alert_barrier_labels",
    "meta_label_features",
]

# Label dataset and column names
LABEL_DATASET = "labels_alert_barriers"
LABEL_COLUMN = "label"

# Feature datasets to check for null rates
FEATURE_DATASETS: list[str] = [
    "meta_label_features",
]

# Minimum expected distinct instrument keys (80% threshold applied on top)
DEFAULT_MIN_INSTRUMENTS = 50
COMPLETENESS_RATIO = 0.8


def _now_et() -> datetime:
    """Return the current time in US/Eastern.  Patchable in tests."""
    return datetime.now(tz=ET)


def compute_psi(expected: np.ndarray, actual: np.ndarray) -> float:
    """Compute Population Stability Index between two distributions."""
    eps = 1e-6
    expected = np.clip(expected.astype(float), eps, None)
    actual = np.clip(actual.astype(float), eps, None)
    # Normalize to proportions
    expected = expected / expected.sum()
    actual = actual / actual.sum()
    return float(np.sum((actual - expected) * np.log(actual / expected)))


def _safe_read_gold(ctx: CheckContext, dataset: str, today_str: str) -> pd.DataFrame | None:
    """Read a Gold dataset for the audit window.

    Returns an empty DataFrame when the read SUCCEEDS with no rows, and ``None``
    when the read FAILS. Callers must distinguish the two: a failed read is not
    "no data", and reporting it as a clean PASS hides the failure on a
    safety-critical check (the zero-leakage audit). Logged at WARNING so the
    failure is visible at the default INFO level.
    """
    try:
        df = ctx.reader.read_gold(dataset, time_range=(today_str, today_str))
        return pd.DataFrame() if df is None else df
    except Exception:
        logger.warning("ml_readiness_gold_read_failed", dataset=dataset, exc_info=True)
        return None


def _quarantine_only_partitions(ctx: CheckContext, dataset: str, today_str: str) -> list[Path]:
    """Quarantine partitions for a date whose canonical partition wrote nothing.

    Quarantined rows sitting beside a healthy partition are routine. The
    reportable case is a date where every row was diverted, which the reader
    can no longer surface because it excludes the quarantine subtree.
    Decided on the filesystem rather than from a read result — the audit read
    is bounded to midnight UTC and comes back empty on any ordinary day.
    """
    root = Path(ctx.settings.gold_path) / f"dataset={dataset}"
    quarantined = []
    for quarantine_dir in root.glob(f"project=*/version=*/_quarantine/*/dt={today_str}"):
        if not _has_rows(quarantine_dir):
            continue
        # <version=V>/_quarantine/<reason>/dt=X — compare against that same
        # version's canonical day, not any other project's healthy one.
        canonical = quarantine_dir.parents[2] / f"dt={today_str}"
        if not _has_rows(canonical):
            quarantined.append(quarantine_dir)
    return quarantined


def _has_rows(partition_dir: Path) -> bool:
    """True when a partition directory holds at least one Parquet row."""
    for f in partition_dir.rglob("*.parquet"):
        try:
            if pq.read_metadata(f).num_rows > 0:
                return True
        except Exception:
            logger.warning("ml_readiness_parquet_metadata_unreadable", path=str(f), exc_info=True)
    return False


async def _check_leakage(ctx: CheckContext, today_str: str, now: datetime) -> list[CheckResult]:
    """11a: Zero-leakage audit — check ts_available >= ts_event."""
    results: list[CheckResult] = []
    sample_size = ctx.settings.health_leakage_sample_size

    for dataset in GOLD_DATASETS_TO_AUDIT:
        df = _safe_read_gold(ctx, dataset, today_str)

        if df is None:
            # The read FAILED — the zero-leakage guardrail could not run. Never
            # report a clean PASS for an unverified safety-critical check.
            results.append(
                CheckResult(
                    check_name="ml_leakage_audit",
                    feed=dataset,
                    severity=Severity.P1_WARNING,
                    status=Status.ERROR,
                    message=f"Could not read dataset={dataset}; leakage audit did not run",
                    details={"dataset": dataset, "date": today_str},
                    ts_checked=now,
                )
            )
            continue

        if df.empty or "ts_available" not in df.columns or "ts_event" not in df.columns:
            results.append(
                CheckResult(
                    check_name="ml_leakage_audit",
                    feed=dataset,
                    severity=Severity.P2_INFO,
                    status=Status.PASS,
                    message=f"No data or missing timestamp columns for dataset={dataset}, skipping leakage audit",
                    details={"dataset": dataset, "date": today_str, "row_count": len(df)},
                    ts_checked=now,
                )
            )
            continue

        # Sample if configured
        check_df = df
        if sample_size > 0 and len(df) > sample_size:
            check_df = df.sample(n=sample_size, random_state=42)

        violations = check_df[check_df["ts_available"] < check_df["ts_event"]]
        violation_count = len(violations)

        # Update Prometheus gauge
        try:
            health_leakage_violations.labels(dataset=dataset).set(violation_count)
        except Exception:
            pass

        if violation_count > 0:
            severity = ctx.calendar.adjust_severity(Severity.P0_CRITICAL, now)
            results.append(
                CheckResult(
                    check_name="ml_leakage_audit",
                    feed=dataset,
                    severity=severity,
                    status=Status.FAIL,
                    message=(
                        f"Zero-leakage violation in dataset={dataset}: "
                        f"{violation_count} rows where ts_available < ts_event"
                    ),
                    details={
                        "dataset": dataset,
                        "date": today_str,
                        "violation_count": violation_count,
                        "rows_checked": len(check_df),
                        "total_rows": len(df),
                    },
                    ts_checked=now,
                )
            )
        else:
            results.append(
                CheckResult(
                    check_name="ml_leakage_audit",
                    feed=dataset,
                    severity=Severity.P2_INFO,
                    status=Status.PASS,
                    message=f"No leakage violations in dataset={dataset} ({len(check_df)} rows checked)",
                    details={
                        "dataset": dataset,
                        "date": today_str,
                        "rows_checked": len(check_df),
                        "total_rows": len(df),
                    },
                    ts_checked=now,
                )
            )

    return results


async def _check_label_stability(ctx: CheckContext, today_str: str, now: datetime) -> list[CheckResult]:
    """11b: Label distribution stability via PSI."""
    df = _safe_read_gold(ctx, LABEL_DATASET, today_str)

    if df is None or df.empty or LABEL_COLUMN not in df.columns:
        return [
            CheckResult(
                check_name="ml_label_stability",
                feed=LABEL_DATASET,
                severity=Severity.P2_INFO,
                status=Status.PASS,
                message=f"No label data for {LABEL_DATASET} today, skipping PSI check",
                details={"dataset": LABEL_DATASET, "date": today_str},
                ts_checked=now,
            )
        ]

    # Compute today's label distribution
    today_counts = df[LABEL_COLUMN].value_counts().sort_index()

    # Load baseline label distribution
    baseline_days = ctx.settings.health_stats_baseline_days
    baseline_end = date.fromisoformat(today_str) - timedelta(days=1)
    baseline_start = baseline_end - timedelta(days=baseline_days - 1)
    baselines_df = ctx.store.read_baselines(baseline_start, baseline_end, baseline_key="label_dist")

    if baselines_df.empty or "label_value" not in baselines_df.columns:
        # No baseline — store current and skip
        baseline_rows = [
            {"label_value": lbl, "count": int(cnt), "proportion": float(cnt / len(df))}
            for lbl, cnt in today_counts.items()
        ]
        ctx.store.write_baseline(pd.DataFrame(baseline_rows), date.fromisoformat(today_str), baseline_key="label_dist")
        return [
            CheckResult(
                check_name="ml_label_stability",
                feed=LABEL_DATASET,
                severity=Severity.P2_INFO,
                status=Status.PASS,
                message=f"Initial label baseline recorded for {LABEL_DATASET}",
                details={"dataset": LABEL_DATASET, "date": today_str, "distribution": today_counts.to_dict()},
                ts_checked=now,
            )
        ]

    # Build aligned distributions for PSI
    baseline_dist = baselines_df.set_index("label_value")["proportion"]
    all_labels = sorted(set(today_counts.index) | set(baseline_dist.index))

    expected = np.array([float(baseline_dist.get(lbl, 0)) for lbl in all_labels])
    actual_counts = np.array([float(today_counts.get(lbl, 0)) for lbl in all_labels])

    # Normalize actual to proportions
    total = actual_counts.sum()
    if total == 0:
        return [
            CheckResult(
                check_name="ml_label_stability",
                feed=LABEL_DATASET,
                severity=Severity.P2_INFO,
                status=Status.PASS,
                message=f"No labels found in {LABEL_DATASET} today",
                details={"dataset": LABEL_DATASET, "date": today_str},
                ts_checked=now,
            )
        ]

    actual_proportions = actual_counts / total
    psi = compute_psi(expected, actual_proportions)
    psi_threshold = ctx.settings.health_psi_threshold

    # Store today's distribution as baseline
    baseline_rows = [
        {"label_value": lbl, "count": int(today_counts.get(lbl, 0)), "proportion": float(actual_proportions[i])}
        for i, lbl in enumerate(all_labels)
    ]
    ctx.store.write_baseline(pd.DataFrame(baseline_rows), date.fromisoformat(today_str))

    if psi > psi_threshold:
        severity = ctx.calendar.adjust_severity(Severity.P1_WARNING, now)
        return [
            CheckResult(
                check_name="ml_label_stability",
                feed=LABEL_DATASET,
                severity=severity,
                status=Status.WARN,
                message=(f"Label distribution drift in {LABEL_DATASET}: PSI={psi:.3f} (threshold={psi_threshold})"),
                details={
                    "dataset": LABEL_DATASET,
                    "date": today_str,
                    "psi": round(psi, 4),
                    "threshold": psi_threshold,
                    "today_distribution": {
                        str(k): float(v) for k, v in zip(all_labels, actual_proportions, strict=False)
                    },
                    "baseline_distribution": {str(k): float(v) for k, v in zip(all_labels, expected, strict=False)},
                },
                ts_checked=now,
            )
        ]
    else:
        return [
            CheckResult(
                check_name="ml_label_stability",
                feed=LABEL_DATASET,
                severity=Severity.P2_INFO,
                status=Status.PASS,
                message=f"Label distribution stable in {LABEL_DATASET}: PSI={psi:.3f}",
                details={
                    "dataset": LABEL_DATASET,
                    "date": today_str,
                    "psi": round(psi, 4),
                    "threshold": psi_threshold,
                },
                ts_checked=now,
            )
        ]


async def _check_feature_nulls(ctx: CheckContext, today_str: str, now: datetime) -> list[CheckResult]:
    """11c: Feature null rates for Gold feature datasets."""
    results: list[CheckResult] = []
    null_threshold = ctx.settings.health_null_rate_threshold

    for dataset in FEATURE_DATASETS:
        df = _safe_read_gold(ctx, dataset, today_str)

        if df is None:
            # The read FAILED. Not the same as "no data" — reporting it as a
            # clean PASS hides exactly the outage this check exists to catch.
            results.append(
                CheckResult(
                    check_name="ml_feature_nulls",
                    feed=dataset,
                    severity=Severity.P1_WARNING,
                    status=Status.ERROR,
                    message=f"Could not read dataset={dataset}; null-rate check did not run",
                    details={"dataset": dataset, "date": today_str},
                    ts_checked=now,
                )
            )
            continue

        if df.empty:
            # Rows the writer diverted to _quarantine (enrichment returned no
            # Greeks) are excluded from reads, so a day lost entirely to a
            # gateway outage would otherwise look like a quiet day.
            quarantined = _quarantine_only_partitions(ctx, dataset, today_str)
            if quarantined:
                results.append(
                    CheckResult(
                        check_name="ml_feature_nulls",
                        feed=dataset,
                        severity=Severity.P1_WARNING,
                        status=Status.WARN,
                        message=(
                            f"No usable feature data for {dataset} today — every row was quarantined "
                            f"(enrichment returned no Greeks)"
                        ),
                        details={
                            "dataset": dataset,
                            "date": today_str,
                            "quarantine_paths": [str(p) for p in quarantined],
                        },
                        ts_checked=now,
                    )
                )
                continue
            results.append(
                CheckResult(
                    check_name="ml_feature_nulls",
                    feed=dataset,
                    severity=Severity.P2_INFO,
                    status=Status.PASS,
                    message=f"No feature data for {dataset} today, skipping null check",
                    details={"dataset": dataset, "date": today_str},
                    ts_checked=now,
                )
            )
            continue

        numeric_cols = [
            c
            for c in df.columns
            if str(df[c].dtype)
            in {
                "float64",
                "float32",
                "int64",
                "int32",
                "Float64",
                "Float32",
                "Int64",
                "Int32",
            }
        ]

        for col in numeric_cols:
            null_pct = float(df[col].isna().sum()) / len(df) if len(df) > 0 else 0.0

            # Update Prometheus gauge
            try:
                health_null_rate.labels(feed=dataset, column=col).set(null_pct)
            except Exception:
                pass

            if null_pct > null_threshold:
                severity = ctx.calendar.adjust_severity(Severity.P1_WARNING, now)
                results.append(
                    CheckResult(
                        check_name="ml_feature_nulls",
                        feed=dataset,
                        severity=severity,
                        status=Status.WARN,
                        message=(f"High null rate in {dataset}.{col}: {null_pct:.1%} (threshold {null_threshold:.1%})"),
                        details={
                            "dataset": dataset,
                            "column": col,
                            "date": today_str,
                            "null_pct": round(null_pct, 4),
                            "threshold": null_threshold,
                        },
                        ts_checked=now,
                    )
                )
            else:
                results.append(
                    CheckResult(
                        check_name="ml_feature_nulls",
                        feed=dataset,
                        severity=Severity.P2_INFO,
                        status=Status.PASS,
                        message=f"Feature null rate OK for {dataset}.{col}: {null_pct:.1%}",
                        details={
                            "dataset": dataset,
                            "column": col,
                            "date": today_str,
                            "null_pct": round(null_pct, 4),
                        },
                        ts_checked=now,
                    )
                )

    return results


async def _check_cross_sectional_completeness(ctx: CheckContext, today_str: str, now: datetime) -> list[CheckResult]:
    """11d: Cross-sectional completeness for multi-instrument Gold datasets."""
    results: list[CheckResult] = []

    for dataset in FEATURE_DATASETS + [LABEL_DATASET]:
        df = _safe_read_gold(ctx, dataset, today_str)

        if df is None or df.empty or "instrument_key" not in df.columns:
            continue

        distinct_keys = df["instrument_key"].nunique()
        min_expected = DEFAULT_MIN_INSTRUMENTS
        threshold = int(min_expected * COMPLETENESS_RATIO)

        if distinct_keys < threshold:
            severity = ctx.calendar.adjust_severity(Severity.P1_WARNING, now)
            results.append(
                CheckResult(
                    check_name="ml_cross_sectional",
                    feed=dataset,
                    severity=severity,
                    status=Status.WARN,
                    message=(
                        f"Low instrument coverage in {dataset}: {distinct_keys} instruments (expected >= {threshold})"
                    ),
                    details={
                        "dataset": dataset,
                        "date": today_str,
                        "distinct_instruments": distinct_keys,
                        "min_expected": min_expected,
                        "threshold": threshold,
                    },
                    ts_checked=now,
                )
            )
        else:
            results.append(
                CheckResult(
                    check_name="ml_cross_sectional",
                    feed=dataset,
                    severity=Severity.P2_INFO,
                    status=Status.PASS,
                    message=f"Instrument coverage OK in {dataset}: {distinct_keys} instruments",
                    details={
                        "dataset": dataset,
                        "date": today_str,
                        "distinct_instruments": distinct_keys,
                    },
                    ts_checked=now,
                )
            )

    return results


async def run_ml_readiness_checks(ctx: CheckContext, check_date: date | None = None) -> list[CheckResult]:
    """Run Tier 3 ML readiness checks across Gold layer datasets."""
    now = _now_et()
    today = check_date or now.date()

    if not ctx.calendar.is_trading_day(today):
        return []

    today_str = today.isoformat()
    results: list[CheckResult] = []

    # 11a: Zero-leakage audit
    results.extend(await _check_leakage(ctx, today_str, now))

    # 11b: Label distribution stability
    results.extend(await _check_label_stability(ctx, today_str, now))

    # 11c: Feature null rates
    results.extend(await _check_feature_nulls(ctx, today_str, now))

    # 11d: Cross-sectional completeness
    results.extend(await _check_cross_sectional_completeness(ctx, today_str, now))

    return results
