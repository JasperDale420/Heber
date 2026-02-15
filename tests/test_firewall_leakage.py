"""Automated leakage tests per PRD §10.12.

Validates anti-leakage behavior for as-of reads, as-of joins,
and training context enforcement.
"""

from datetime import UTC, datetime, timedelta

import polars as pl

from heber.firewall.asof import asof_join, read_asof
from heber.firewall.validation import LeakageError, validate_asof_read


def _create_test_dataframe(
    n_rows: int = 100,
    start_time: datetime | None = None,
    availability_lag_seconds: int = 5,
) -> pl.DataFrame:
    """Create a test dataframe with realistic timestamp patterns."""
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


def test_asof_read_filters_future_data() -> None:
    """Test that read_asof correctly filters out future data.

    This is a CRITICAL test - if this fails, we have leakage.
    """
    df = _create_test_dataframe(n_rows=100, availability_lag_seconds=10)

    # Query as-of a time in the middle of the data
    asof_time = df["ts_available"][50]

    result = read_asof(df, asof_time).collect()

    # All returned rows must have ts_available <= asof_time
    max_available = result["ts_available"].max()
    assert max_available <= asof_time, (
        f"LEAKAGE: read_asof returned future data! max_ts_available={max_available}, asof_time={asof_time}"
    )

    # We should have approximately half the rows
    assert len(result) > 0, "read_asof returned no data when it should have"
    assert len(result) <= 51, f"Too many rows returned: {len(result)}"


def test_asof_join_no_future_lookups() -> None:
    """Test that asof_join never joins future data.

    Validates that the right table's availability is respected.
    """
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
                start + timedelta(seconds=15),
                start + timedelta(seconds=55),
                start + timedelta(seconds=85),
                start + timedelta(seconds=110),
            ],
            "bid_px": [149.5, 150.5, 151.5, 152.5],
        }
    )

    result = asof_join(
        trades,
        quotes,
        left_on="ts_event",
        right_on="ts_event",
        by="instrument_key",
    ).collect()

    ts_avail_col = "ts_available_right" if "ts_available_right" in result.columns else "ts_available"

    for row in result.iter_rows(named=True):
        trade_time = row["ts_event"]
        quote_available = row.get(ts_avail_col)

        if quote_available is not None:
            assert quote_available <= trade_time, (
                f"LEAKAGE: Joined quote not available at trade time! "
                f"trade_time={trade_time}, quote_ts_available={quote_available}"
            )


def test_training_context_requires_asof() -> None:
    """Test that training context requires as-of time.

    Per PRD §10.11, reading without asof_time in training should error.
    """
    try:
        validate_asof_read(df_has_ts_available=True, asof_time=None, context="training")
        raise AssertionError("LEAKAGE RISK: Training read without asof_time was allowed!")
    except LeakageError:
        pass
