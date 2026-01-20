"""Tests for Testing Module (PRD §45-50)."""

from datetime import datetime, timedelta, UTC

import pytest

from heber.testing.generators import (
    TestDataConfig,
    SyntheticDataGenerator,
    TestFixture,
    FixtureRegistry,
    SIMPLE_BARS_FIXTURE,
    LEAKAGE_TEST_FIXTURE,
)
from heber.testing.leakage import (
    LeakageTestResult,
    LeakageTestCase,
    LeakageTestRun,
    LeakageValidator,
    DEFAULT_LEAKAGE_TESTS,
)


class TestSyntheticDataGenerator:
    """Test SyntheticDataGenerator."""
    
    def test_generate_bar(self):
        gen = SyntheticDataGenerator(seed=42)
        
        bar = gen.generate_bar("AAPL", datetime.now(UTC), base_price=150.0)
        
        assert bar["symbol"] == "AAPL"
        assert "open" in bar
        assert "high" in bar
        assert "low" in bar
        assert "close" in bar
        assert bar["high"] >= bar["low"]
    
    def test_generate_trade(self):
        gen = SyntheticDataGenerator(seed=42)
        
        trade = gen.generate_trade("GOOGL", datetime.now(UTC))
        
        assert trade["symbol"] == "GOOGL"
        assert "price" in trade
        assert "size" in trade
        assert trade["size"] > 0
    
    def test_generate_quote(self):
        gen = SyntheticDataGenerator(seed=42)
        
        quote = gen.generate_quote("MSFT", datetime.now(UTC))
        
        assert quote["symbol"] == "MSFT"
        assert quote["ask_price"] > quote["bid_price"]  # Spread is positive
    
    def test_deterministic_with_seed(self):
        gen1 = SyntheticDataGenerator(seed=42)
        gen2 = SyntheticDataGenerator(seed=42)
        
        bar1 = gen1.generate_bar("AAPL", datetime(2025, 1, 15, 10, 0, 0, tzinfo=UTC))
        bar2 = gen2.generate_bar("AAPL", datetime(2025, 1, 15, 10, 0, 0, tzinfo=UTC))
        
        assert bar1["open"] == bar2["open"]
        assert bar1["close"] == bar2["close"]
    
    def test_generate_golden_dataset(self):
        gen = SyntheticDataGenerator(seed=42)
        
        data = gen.generate_golden_dataset("bars", num_records=50)
        
        assert len(data) == 50
        assert all("symbol" in d for d in data)


class TestTestDataConfig:
    """Test TestDataConfig."""
    
    def test_date_range(self):
        config = TestDataConfig(
            symbols=["AAPL"],
            start_date=datetime(2025, 1, 1, tzinfo=UTC),
            end_date=datetime(2025, 1, 5, tzinfo=UTC),
        )
        
        dates = config.date_range
        
        assert len(dates) == 5  # Jan 1-5


class TestFixtureRegistry:
    """Test FixtureRegistry."""
    
    def test_get_default_fixture(self):
        registry = FixtureRegistry()
        
        fixture = registry.get("simple_bars")
        
        assert fixture is not None
        assert len(fixture.data) == 5
    
    def test_list_all(self):
        registry = FixtureRegistry()
        
        names = registry.list_all()
        
        assert "simple_bars" in names
        assert "leakage_test" in names
    
    def test_add_fixture(self):
        registry = FixtureRegistry()
        new_fixture = TestFixture(
            name="custom_test",
            description="Custom test fixture",
            data=[{"id": 1}],
            expected_results={"count": 1},
        )
        
        registry.add(new_fixture)
        
        assert registry.get("custom_test") is not None


class TestLeakageValidator:
    """Test LeakageValidator."""
    
    def test_list_default_tests(self):
        validator = LeakageValidator()
        
        tests = validator.list_all()
        
        assert len(tests) >= 7
        assert any(t["test_id"] == "LK-001" for t in tests)
    
    def test_validate_no_future_data_pass(self):
        validator = LeakageValidator()
        asof_time = datetime(2025, 1, 15, 10, 5, 0, tzinfo=UTC)
        
        query_result = [
            {"symbol": "AAPL", "ts_available": "2025-01-15T10:01:00+00:00"},
            {"symbol": "AAPL", "ts_available": "2025-01-15T10:02:00+00:00"},
        ]
        
        result = validator.validate_no_future_data(query_result, asof_time)
        
        assert result.result == LeakageTestResult.PASSED
    
    def test_validate_no_future_data_fail(self):
        validator = LeakageValidator()
        asof_time = datetime(2025, 1, 15, 10, 0, 0, tzinfo=UTC)
        
        query_result = [
            {"symbol": "AAPL", "ts_available": "2025-01-15T09:00:00+00:00"},  # OK
            {"symbol": "AAPL", "ts_available": "2025-01-15T11:00:00+00:00"},  # FUTURE!
        ]
        
        result = validator.validate_no_future_data(query_result, asof_time)
        
        assert result.result == LeakageTestResult.FAILED
        assert result.actual_value == 1
    
    def test_validate_backfill_ts_available_pass(self):
        validator = LeakageValidator()
        ts_commit = datetime(2025, 1, 15, 10, 0, 0, tzinfo=UTC)
        ts_available = ts_commit + timedelta(seconds=2)  # Within tolerance
        
        result = validator.validate_backfill_ts_available(ts_available, ts_commit)
        
        assert result.result == LeakageTestResult.PASSED
    
    def test_validate_backfill_ts_available_fail(self):
        validator = LeakageValidator()
        ts_commit = datetime(2025, 1, 15, 10, 0, 0, tzinfo=UTC)
        ts_available = datetime(2025, 1, 16, 10, 0, 0, tzinfo=UTC)  # Day off!
        
        result = validator.validate_backfill_ts_available(ts_available, ts_commit)
        
        assert result.result == LeakageTestResult.FAILED
    
    def test_validate_gold_lineage_pass(self):
        validator = LeakageValidator()
        feature_ts = datetime(2025, 1, 15, 9, 0, 0, tzinfo=UTC)
        input_available = datetime(2025, 1, 15, 9, 30, 0, tzinfo=UTC)
        label_ts = datetime(2025, 1, 15, 10, 0, 0, tzinfo=UTC)  # After input available
        
        result = validator.validate_gold_lineage(feature_ts, input_available, label_ts)
        
        assert result.result == LeakageTestResult.PASSED
    
    def test_validate_gold_lineage_fail(self):
        validator = LeakageValidator()
        feature_ts = datetime(2025, 1, 15, 9, 0, 0, tzinfo=UTC)
        input_available = datetime(2025, 1, 15, 11, 0, 0, tzinfo=UTC)  # AFTER label!
        label_ts = datetime(2025, 1, 15, 10, 0, 0, tzinfo=UTC)
        
        result = validator.validate_gold_lineage(feature_ts, input_available, label_ts)
        
        assert result.result == LeakageTestResult.FAILED
    
    def test_generate_report(self):
        validator = LeakageValidator()
        asof_time = datetime(2025, 1, 15, 10, 0, 0, tzinfo=UTC)
        
        validator.validate_no_future_data([], asof_time)
        validator.validate_backfill_ts_available(asof_time, asof_time)
        
        report = validator.generate_report()
        
        assert report["summary"]["total"] == 2
        assert report["summary"]["passed"] == 2
        assert report["verdict"] == "PASSED"


def run_all_testing_tests() -> dict[str, bool]:
    """Run all testing module tests."""
    results = {}
    
    test_classes = [
        TestSyntheticDataGenerator,
        TestTestDataConfig,
        TestFixtureRegistry,
        TestLeakageValidator,
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
    print(f"\nTesting Module Tests: {passed}/{total} passed")
    
    return results


if __name__ == "__main__":
    run_all_testing_tests()
