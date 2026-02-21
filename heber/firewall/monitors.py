"""Runtime monitors for leakage detection per PRD §10.12.

Provides monitoring utilities for tracking data availability lag
and late-arriving events in production.
"""

import polars as pl


def monitor_availability_lag(
    df: pl.DataFrame,
    event_col: str = "ts_event",
    available_col: str = "ts_available",
) -> dict[str, float]:
    """Monitor the distribution of (ts_available - ts_event) lag.

    Args:
        df: DataFrame with timestamp columns
        event_col: Event time column
        available_col: Availability time column

    Returns:
        Statistics about availability lag
    """
    lag_seconds = df.select(
        [((pl.col(available_col) - pl.col(event_col)).dt.total_seconds()).alias("lag_seconds")]
    ).get_column("lag_seconds")

    stats = {
        "mean_lag_seconds": lag_seconds.mean(),
        "median_lag_seconds": lag_seconds.median(),
        "p95_lag_seconds": lag_seconds.quantile(0.95),
        "max_lag_seconds": lag_seconds.max(),
        "min_lag_seconds": lag_seconds.min(),
    }

    return stats


def monitor_late_arrivals(
    df: pl.DataFrame,
    late_threshold_seconds: float = 60.0,
    available_col: str = "ts_available",
    ingest_col: str = "ts_ingest",
) -> dict[str, float]:
    """Monitor percent of late-arriving events.

    Args:
        df: DataFrame with timestamp columns
        late_threshold_seconds: Threshold for considering data "late"
        available_col: Availability time column
        ingest_col: Ingest time column

    Returns:
        Statistics about late arrivals
    """
    total = len(df)
    if total == 0:
        return {"late_percent": 0.0, "late_count": 0, "total_count": 0}

    late_mask = (pl.col(available_col) - pl.col(ingest_col)).dt.total_seconds() > late_threshold_seconds

    late_count = df.filter(late_mask).height

    return {
        "late_percent": (late_count / total) * 100,
        "late_count": late_count,
        "total_count": total,
    }
