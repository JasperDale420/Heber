"""Automated Leakage Tests for CI per PRD §10.12.

Provides test fixtures and utilities for validating anti-leakage behavior.
These should be run as part of CI to ensure leakage cannot creep in quietly.
"""

from datetime import UTC, datetime, timedelta

import polars as pl
import structlog

from heber.firewall.asof import asof_join, read_asof
from heber.firewall.validation import LeakageError, validate_asof_read

logger = structlog.get_logger(__name__)


def create_test_dataframe(
    n_rows: int = 100,
    start_time: datetime | None = None,
    availability_lag_seconds: int = 5,
) -> pl.DataFrame:
    """Create a test dataframe with realistic timestamp patterns.

    Args:
        n_rows: Number of rows to generate
        start_time: Start time for data (defaults to now - 1 hour)
        availability_lag_seconds: Simulated lag between ts_event and ts_available

    Returns:
        DataFrame with ts_event, ts_available, and sample data
    """
    if start_time is None:
        start_time = datetime.now(UTC) - timedelta(hours=1)

    data = {
        "event_id": [f"evt_{i:04d}" for i in range(n_rows)],
        "instrument_key": ["equity:AAPL"] * n_rows,
        "ts_event": [start_time + timedelta(seconds=i) for i in range(n_rows)],
        "ts_available": [start_time + timedelta(seconds=i + availability_lag_seconds) for i in range(n_rows)],
        "value": list(range(n_rows)),
    }

    return pl.DataFrame(data)


def test_asof_read_filters_future_data() -> bool:
    """Test that read_asof correctly filters out future data.

    This is a CRITICAL test - if this fails, we have leakage.

    Returns:
        True if test passes

    Raises:
        AssertionError: If leakage is detected
    """
    # Create data where some rows have ts_available in the future
    df = create_test_dataframe(n_rows=100, availability_lag_seconds=10)

    # Query as-of a time in the middle of the data
    # Should only get rows where ts_available <= asof_time
    asof_time = df["ts_available"][50]

    result = read_asof(df, asof_time).collect()

    # Verify: all returned rows must have ts_available <= asof_time
    max_available = result["ts_available"].max()
    assert max_available <= asof_time, (
        f"LEAKAGE: read_asof returned future data! max_ts_available={max_available}, asof_time={asof_time}"
    )

    # Verify: we should have approximately half the rows
    assert len(result) > 0, "read_asof returned no data when it should have"
    assert len(result) <= 51, f"Too many rows returned: {len(result)}"

    logger.info("test_asof_read_filters_future_data PASSED")
    return True


def test_asof_join_no_future_lookups() -> bool:
    """Test that asof_join never joins future data.

    This validates that the right table's availability is respected.

    Returns:
        True if test passes

    Raises:
        AssertionError: If leakage is detected
    """
    # Create two tables: trades (left) and quotes (right)
    start = datetime(2026, 1, 15, 10, 0, 0, tzinfo=UTC)

    trades = pl.DataFrame(
        {
            "event_id": ["t1", "t2", "t3"],
            "instrument_key": ["equity:AAPL"] * 3,
            "ts_event": [
                start + timedelta(seconds=30),
                start + timedelta(seconds=60),
                start + timedelta(seconds=90),
            ],
            "ts_available": [
                start + timedelta(seconds=31),
                start + timedelta(seconds=61),
                start + timedelta(seconds=91),
            ],
            "price": [150.0, 151.0, 152.0],
        }
    )

    # Quotes with different availability lags
    quotes = pl.DataFrame(
        {
            "event_id": ["q1", "q2", "q3", "q4"],
            "instrument_key": ["equity:AAPL"] * 4,
            "ts_event": [
                start + timedelta(seconds=10),
                start + timedelta(seconds=40),
                start + timedelta(seconds=70),
                start + timedelta(seconds=100),
            ],
            "ts_available": [
                start + timedelta(seconds=15),  # Available before t1
                start + timedelta(seconds=55),  # Available before t2 but after its event
                start + timedelta(seconds=85),  # Available before t3
                start + timedelta(seconds=110),  # Available after all trades
            ],
            "bid_px": [149.5, 150.5, 151.5, 152.5],
        }
    )

    # Perform as-of join
    result = asof_join(
        trades,
        quotes,
        left_on="ts_event",
        right_on="ts_event",
        by="instrument_key",
    ).collect()

    # For each trade, verify the joined quote was available at trade time
    # Check for ts_available_right or ts_available if suffix wasn't applied
    ts_avail_col = "ts_available_right" if "ts_available_right" in result.columns else "ts_available"

    for row in result.iter_rows(named=True):
        trade_time = row["ts_event"]
        quote_available = row.get(ts_avail_col)

        if quote_available is not None:
            assert quote_available <= trade_time, (
                f"LEAKAGE: Joined quote not available at trade time! "
                f"trade_time={trade_time}, quote_ts_available={quote_available}"
            )

    logger.info("test_asof_join_no_future_lookups PASSED")
    return True


def test_training_context_requires_asof() -> bool:
    """Test that training context requires as-of time.

    Per PRD §10.11, reading without asof_time in training should error.

    Returns:
        True if test passes

    Raises:
        AssertionError: If validation is not enforced
    """
    try:
        validate_asof_read(df_has_ts_available=True, asof_time=None, context="training")
        # If we get here, validation failed to catch the error
        raise AssertionError("LEAKAGE RISK: Training read without asof_time was allowed!")
    except LeakageError:
        # This is the expected behavior
        pass

    logger.info("test_training_context_requires_asof PASSED")
    return True


def run_all_leakage_tests() -> dict[str, bool]:
    """Run all automated leakage tests.

    This should be called from CI.

    Returns:
        Dict mapping test name to pass/fail
    """
    tests = [
        ("asof_read_filters_future", test_asof_read_filters_future_data),
        ("asof_join_no_future_lookups", test_asof_join_no_future_lookups),
        ("training_requires_asof", test_training_context_requires_asof),
    ]

    results = {}
    for name, test_fn in tests:
        try:
            results[name] = test_fn()
        except Exception as e:
            logger.error(f"Test {name} FAILED", error=str(e))
            results[name] = False

    passed = sum(1 for v in results.values() if v)
    total = len(results)
    logger.info(f"Leakage tests: {passed}/{total} passed")

    return results


# Runtime monitors (PRD §10.12)


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
