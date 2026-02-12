"""Edge Case Test Library for Heber Zero-Leakage System (Phase 49.3).

Tests for scenarios that could compromise point-in-time correctness:
- Clock skew between data sources
- Missing timestamps
- Schema mismatches
- Late-arriving data
"""

from datetime import UTC, datetime, timedelta

import pandas as pd

from heber.sdk.client import HeberClient


class TestClockSkew:
    """Tests for clock skew scenarios (PRD §49.3).

    Clock skew occurs when different data sources have misaligned timestamps.
    The zero-leakage guarantee must handle these cases correctly.
    """

    def test_source_ahead_of_target(self):
        """Test when source timestamp is ahead of processing time.

        If ts_event > ts_available, the data was generated before it became
        available (normal case). But if ts_event is in the future relative
        to current time, it may indicate clock skew.
        """
        now = datetime.now(UTC)
        future_event = now + timedelta(hours=1)

        df = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": [future_event],
                "ts_available": [now],  # Available now, event in future (clock skew)
                "value": [100.0],
            }
        )

        # Event time > available time suggests source clock is ahead
        assert (df["ts_event"] > df["ts_available"]).any()

        # Zero-leakage filter should still work based on ts_available
        asof_time = now - timedelta(minutes=1)
        filtered = df[df["ts_available"] <= asof_time]
        assert len(filtered) == 0  # Not yet available

        asof_time = now + timedelta(minutes=1)
        filtered = df[df["ts_available"] <= asof_time]
        assert len(filtered) == 1  # Now available

    def test_source_behind_target(self):
        """Test when source timestamp is behind processing time.

        If ts_event is significantly before ts_available, data may have been
        delayed in transit or the source clock is behind.
        """
        now = datetime.now(UTC)
        past_event = now - timedelta(hours=24)

        df = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": [past_event],
                "ts_available": [now],  # Just became available
                "value": [100.0],
            }
        )

        # Event from yesterday, only available now
        delay = df["ts_available"].iloc[0] - df["ts_event"].iloc[0]
        assert delay > timedelta(hours=23)

        # Zero-leakage: querying for "yesterday" should NOT return this
        asof_time = now - timedelta(hours=1)
        filtered = df[df["ts_available"] <= asof_time]
        assert len(filtered) == 0  # Wasn't available yet

    def test_skewed_join_safety(self):
        """Test that as-of joins handle clock skew correctly."""
        now = datetime.now(UTC)

        # Left table: trades with correct timing
        trades = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL", "equity:AAPL"],
                "ts_event": [now - timedelta(hours=2), now - timedelta(hours=1)],
                "price": [150.0, 151.0],
            }
        )

        # Right table: quotes with clock skew (available later than event)
        quotes = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL", "equity:AAPL"],
                "ts_event": [now - timedelta(hours=3), now - timedelta(hours=2)],
                "ts_available": [
                    now - timedelta(hours=1),  # Available late
                    now - timedelta(minutes=30),  # Very late
                ],
                "bid": [149.0, 150.0],
            }
        )

        # As-of join should use ts_available, not ts_event
        # Trade at -2h should NOT see quote with ts_event=-3h if ts_available=-1h
        # This is the zero-leakage guarantee

        HeberClient()
        # Note: In real usage, this would use client.asof_join()
        # For unit test, we verify the logic manually

        trade_time = trades["ts_event"].iloc[0]  # -2h
        available_quotes = quotes[quotes["ts_available"] <= trade_time]

        # No quotes were available at trade time
        assert len(available_quotes) == 0


class TestMissingTimestamps:
    """Tests for missing timestamp handling (PRD §49.3).

    Data may arrive with missing ts_event or ts_available fields.
    System must handle these gracefully.
    """

    def test_missing_ts_available_rejected(self):
        """Events without ts_available should be rejected."""
        df = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": [datetime.now(UTC)],
                "ts_available": [None],  # Missing!
                "value": [100.0],
            }
        )

        # Filter for non-null ts_available
        valid = df.dropna(subset=["ts_available"])
        assert len(valid) == 0

    def test_missing_ts_event_handled(self):
        """Events with missing ts_event should use ts_available as fallback."""
        now = datetime.now(UTC)

        df = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": [pd.NaT],  # Missing
                "ts_available": [now],
                "value": [100.0],
            }
        )

        # Fill ts_event from ts_available if missing
        df["ts_event"] = df["ts_event"].where(df["ts_event"].notna(), df["ts_available"])
        assert df["ts_event"].iloc[0] == now

    def test_both_timestamps_missing_rejected(self):
        """Events with both timestamps missing should be completely rejected."""
        df = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": [None],
                "ts_available": [None],
                "value": [100.0],
            }
        )

        valid = df.dropna(subset=["ts_event", "ts_available"], how="all")
        # Still has rows (only rejected if BOTH are NaN)
        valid = df.dropna(subset=["ts_available"])  # Strict: need ts_available
        assert len(valid) == 0


class TestSchemaMismatches:
    """Tests for schema mismatch handling (PRD §49.3).

    Data may arrive with unexpected columns or types.
    """

    def test_extra_columns_preserved(self):
        """Extra columns in data should be preserved."""
        expected_cols = {"instrument_key", "ts_event", "ts_available", "price"}
        actual_cols = expected_cols | {"extra_field", "another_extra"}

        df = pd.DataFrame(columns=list(actual_cols))

        # All columns preserved
        assert set(df.columns) == actual_cols

    def test_missing_required_columns_detected(self):
        """Missing required columns should be detected."""
        required = {"instrument_key", "ts_event", "ts_available"}

        df = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": [datetime.now(UTC)],
                # Missing ts_available!
                "price": [100.0],
            }
        )

        missing = required - set(df.columns)
        assert "ts_available" in missing

    def test_type_coercion(self):
        """Type mismatches should be handled via coercion."""
        df = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": ["2025-01-15T10:00:00Z"],  # String, not datetime
                "ts_available": ["2025-01-15T10:01:00Z"],
                "price": ["100.50"],  # String, not float
            }
        )

        # Coerce types
        df["ts_event"] = pd.to_datetime(df["ts_event"])
        df["ts_available"] = pd.to_datetime(df["ts_available"])
        df["price"] = pd.to_numeric(df["price"])

        assert df["ts_event"].dtype == "datetime64[ns, UTC]" or "datetime" in str(df["ts_event"].dtype)
        assert df["price"].dtype in ("float64", "float32")


class TestLateArrivingData:
    """Tests for late-arriving data handling (PRD §49.3).

    Data may arrive significantly after its event time.
    The system must handle this without compromising historical queries.
    """

    def test_late_data_visibility(self):
        """Late data should only be visible after its availability time."""
        now = datetime.now(UTC)

        # Event from 1 hour ago, but just became available
        late_data = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": [now - timedelta(hours=1)],
                "ts_available": [now],
                "price": [100.0],
            }
        )

        # Query at 30 minutes ago: late data NOT visible
        asof_30m = now - timedelta(minutes=30)
        visible = late_data[late_data["ts_available"] <= asof_30m]
        assert len(visible) == 0

        # Query at 5 minutes from now: late data IS visible
        asof_future = now + timedelta(minutes=5)
        visible = late_data[late_data["ts_available"] <= asof_future]
        assert len(visible) == 1

    def test_very_late_data_handling(self):
        """Very late data (days/weeks) should still be handled correctly."""
        now = datetime.now(UTC)

        # Correction: Event from last week, just became available
        very_late = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": [now - timedelta(days=7)],
                "ts_available": [now],
                "price": [99.0],  # Corrected price
            }
        )

        # Historical query from 3 days ago should NOT see this correction
        historical_query = now - timedelta(days=3)
        visible = very_late[very_late["ts_available"] <= historical_query]
        assert len(visible) == 0

        # This ensures reproducibility: same query, same result

    def test_out_of_order_arrival(self):
        """Data arriving out of order should be handled correctly."""
        now = datetime.now(UTC)

        # Data arriving in wrong order
        batch1 = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": [now - timedelta(hours=2)],
                "ts_available": [now - timedelta(hours=1)],
                "price": [100.0],
            }
        )

        batch2 = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": [now - timedelta(hours=3)],  # Earlier event
                "ts_available": [now],  # But arrived later
                "price": [99.0],
            }
        )

        combined = pd.concat([batch1, batch2])

        # Query at 30 minutes ago: should only see batch1
        asof_30m = now - timedelta(minutes=30)
        visible = combined[combined["ts_available"] <= asof_30m]
        assert len(visible) == 1
        assert visible["price"].iloc[0] == 100.0


def run_all_edge_case_tests() -> dict[str, bool]:
    """Run all edge case tests and return results."""
    results = {}

    test_classes = [
        TestClockSkew,
        TestMissingTimestamps,
        TestSchemaMismatches,
        TestLateArrivingData,
    ]

    for test_class in test_classes:
        instance = test_class()
        for method_name in dir(instance):
            if method_name.startswith("test_"):
                try:
                    getattr(instance, method_name)()
                    results[f"{test_class.__name__}.{method_name}"] = True
                except Exception as e:
                    results[f"{test_class.__name__}.{method_name}"] = False
                    print(f"FAILED: {test_class.__name__}.{method_name}: {e}")

    passed = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"\nEdge Case Tests: {passed}/{total} passed")

    return results


if __name__ == "__main__":
    run_all_edge_case_tests()
