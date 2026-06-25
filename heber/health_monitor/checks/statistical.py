"""Tier 3 — Statistical Profiling Check.

Computes per-column statistics for each Silver feed and compares null rates
and mean values against stored baselines to detect anomalous shifts.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import structlog

from heber.health_monitor.metrics import health_null_rate
from heber.health_monitor.models import CheckContext, CheckResult, Severity, Status
from heber.schemas.silver import SILVER_SCHEMAS
from heber.writer.ingest_contracts import BRONZE_ONLY_SILVER_DATASETS

logger = structlog.get_logger(__name__)

ET = ZoneInfo("America/New_York")

NUMERIC_DTYPES = {"float64", "float32", "int64", "int32", "int16", "int8", "Float64", "Float32", "Int64", "Int32"}


def _now_et() -> datetime:
    """Return the current time in US/Eastern.  Patchable in tests."""
    return datetime.now(tz=ET)


def _silver_feeds() -> list[str]:
    """Return canonical Silver feed names (those with Arrow schemas, minus bronze-only)."""
    return sorted(name for name in SILVER_SCHEMAS if name not in BRONZE_ONLY_SILVER_DATASETS)


def _compute_column_stats(df: pd.DataFrame) -> list[dict]:
    """Compute stats for each numeric column in the DataFrame."""
    stats: list[dict] = []
    for col in df.columns:
        if str(df[col].dtype) not in NUMERIC_DTYPES:
            continue
        series = df[col]
        count = len(series)
        null_count = int(series.isna().sum())
        null_pct = null_count / count if count > 0 else 0.0
        non_null = series.dropna()
        stats.append(
            {
                "column": col,
                "count": count,
                "null_count": null_count,
                "null_pct": round(null_pct, 6),
                "min": float(non_null.min()) if len(non_null) > 0 else None,
                "max": float(non_null.max()) if len(non_null) > 0 else None,
                "mean": float(non_null.mean()) if len(non_null) > 0 else None,
                "stddev": float(non_null.std()) if len(non_null) > 1 else 0.0,
            }
        )
    return stats


async def run_statistical_checks(ctx: CheckContext, check_date: date | None = None) -> list[CheckResult]:
    """Run Tier 3 statistical profiling checks for all Silver feeds."""
    now = _now_et()
    today = check_date or now.date()

    if not ctx.calendar.is_trading_day(today):
        return []

    results: list[CheckResult] = []
    today_str = today.isoformat()
    null_threshold = ctx.settings.health_null_rate_threshold

    # Load trailing baselines
    baseline_days = ctx.settings.health_stats_baseline_days
    baseline_end = today - timedelta(days=1)
    baseline_start = baseline_end - timedelta(days=baseline_days - 1)
    baselines_df = ctx.store.read_baselines(baseline_start, baseline_end, baseline_key="stats")

    # Build baseline lookup: {(feed, column): {mean, stddev}}
    baseline_lookup: dict[tuple[str, str], dict] = {}
    if not baselines_df.empty and "feed" in baselines_df.columns and "column" in baselines_df.columns:
        for (feed, col), group in baselines_df.groupby(["feed", "column"]):
            baseline_lookup[(str(feed), str(col))] = {
                "mean": float(group["mean"].mean()),
                "stddev": float(group["stddev"].mean()),
            }

    all_stats_rows: list[dict] = []

    for feed in _silver_feeds():
        try:
            df = ctx.reader.read_silver(feed, time_range=(today_str, today_str))
        except Exception:
            # WARNING (not DEBUG): a read failure silently drops the feed from
            # the drift/null audit, and DEBUG is suppressed at the default INFO
            # level — the unreadable feed would otherwise vanish without a trace.
            logger.warning("statistical_read_failed", feed=feed, exc_info=True)
            continue

        if df is None or df.empty:
            continue

        col_stats = _compute_column_stats(df)
        if not col_stats:
            continue

        for cs in col_stats:
            col_name = cs["column"]

            # Store for baseline
            row = {"feed": feed, **cs}
            all_stats_rows.append(row)

            # Update Prometheus gauge
            try:
                health_null_rate.labels(feed=feed, column=col_name).set(cs["null_pct"])
            except Exception:
                pass

            # Check 1: Null rate threshold
            if cs["null_pct"] > null_threshold:
                severity = ctx.calendar.adjust_severity(Severity.P1_WARNING, now)
                results.append(
                    CheckResult(
                        check_name="statistical_null_rate",
                        feed=feed,
                        severity=severity,
                        status=Status.WARN,
                        message=(
                            f"High null rate for feed={feed} column={col_name}: "
                            f"{cs['null_pct']:.1%} (threshold {null_threshold:.1%})"
                        ),
                        details={
                            "feed": feed,
                            "column": col_name,
                            "date": today_str,
                            "null_pct": cs["null_pct"],
                            "null_count": cs["null_count"],
                            "count": cs["count"],
                            "threshold": null_threshold,
                        },
                        ts_checked=now,
                    )
                )
            else:
                results.append(
                    CheckResult(
                        check_name="statistical_null_rate",
                        feed=feed,
                        severity=Severity.P2_INFO,
                        status=Status.PASS,
                        message=f"Null rate OK for feed={feed} column={col_name}: {cs['null_pct']:.1%}",
                        details={
                            "feed": feed,
                            "column": col_name,
                            "date": today_str,
                            "null_pct": cs["null_pct"],
                        },
                        ts_checked=now,
                    )
                )

            # Check 2: Mean shift against baseline
            baseline = baseline_lookup.get((feed, col_name))
            if baseline is not None and baseline["stddev"] > 0 and cs["mean"] is not None:
                shift = abs(cs["mean"] - baseline["mean"]) / baseline["stddev"]
                if shift > 2.0:
                    severity = ctx.calendar.adjust_severity(Severity.P1_WARNING, now)
                    results.append(
                        CheckResult(
                            check_name="statistical_mean_shift",
                            feed=feed,
                            severity=severity,
                            status=Status.WARN,
                            message=(
                                f"Mean shift detected for feed={feed} column={col_name}: "
                                f"today={cs['mean']:.2f}, baseline={baseline['mean']:.2f} "
                                f"(shift={shift:.1f}σ)"
                            ),
                            details={
                                "feed": feed,
                                "column": col_name,
                                "date": today_str,
                                "today_mean": cs["mean"],
                                "baseline_mean": baseline["mean"],
                                "baseline_stddev": baseline["stddev"],
                                "shift_sigma": round(shift, 2),
                            },
                            ts_checked=now,
                        )
                    )
                else:
                    results.append(
                        CheckResult(
                            check_name="statistical_mean_shift",
                            feed=feed,
                            severity=Severity.P2_INFO,
                            status=Status.PASS,
                            message=(
                                f"Mean stable for feed={feed} column={col_name}: "
                                f"today={cs['mean']:.2f}, baseline={baseline['mean']:.2f} "
                                f"(shift={shift:.1f}σ)"
                            ),
                            details={
                                "feed": feed,
                                "column": col_name,
                                "date": today_str,
                                "today_mean": cs["mean"],
                                "baseline_mean": baseline["mean"],
                                "shift_sigma": round(shift, 2),
                            },
                            ts_checked=now,
                        )
                    )

    # Store today's stats as baseline
    if all_stats_rows:
        baseline_df = pd.DataFrame(all_stats_rows)
        ctx.store.write_baseline(baseline_df, today, baseline_key="stats")

    return results
