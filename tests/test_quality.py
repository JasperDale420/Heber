"""Tests for Data Quality Contracts (PRD §33)."""

from datetime import UTC, datetime

import pandas as pd
import pytest

from heber.quality.contracts import (
    DataQualityValidator,
    QualityContract,
    QualityMetric,
    create_default_validator,
)


class TestQualityContract:
    """Test QualityContract dataclass."""

    def test_to_dict_roundtrip(self):
        contract = QualityContract(
            metric=QualityMetric.FILL_RATE,
            threshold=0.95,
            description="Test contract",
        )

        d = contract.to_dict()
        restored = QualityContract.from_dict(d)

        assert restored.metric == QualityMetric.FILL_RATE
        assert restored.threshold == pytest.approx(0.95)


class TestFillRateCheck:
    """Test fill rate validation."""

    def test_full_fill_rate(self):
        validator = DataQualityValidator()

        dates = pd.date_range("2024-01-01", periods=10, freq="D")
        symbols = [f"SYM{i}" for i in range(10)]

        rows = []
        for symbol in symbols:
            for date in dates:
                rows.append({"instrument_key": symbol, "ts_event": date})

        df = pd.DataFrame(rows)

        rate, affected_symbols, _ = validator.check_fill_rate(df, 10)

        assert rate == pytest.approx(1.0)
        assert len(affected_symbols) == 0

    def test_partial_fill_rate(self):
        validator = DataQualityValidator()

        dates = pd.date_range("2024-01-01", periods=10, freq="D")

        rows = []
        for date in dates:
            rows.append({"instrument_key": "SYM0", "ts_event": date})
        for date in dates[:5]:
            rows.append({"instrument_key": "SYM1", "ts_event": date})

        df = pd.DataFrame(rows)

        rate, affected_symbols, _ = validator.check_fill_rate(df, 10)

        assert rate < 1.0
        assert "SYM1" in affected_symbols


class TestNonNullRateCheck:
    """Test non-null rate validation."""

    def test_all_non_null(self):
        validator = DataQualityValidator()

        df = pd.DataFrame(
            {
                "open": [100, 101, 102],
                "close": [101, 102, 103],
            }
        )

        rate, cols = validator.check_non_null_rate(df, ["open", "close"])

        assert rate == pytest.approx(1.0)
        assert len(cols) == 0

    def test_some_nulls(self):
        validator = DataQualityValidator()

        df = pd.DataFrame(
            {
                "open": [100, None, 102],
                "close": [101, 102, None],
            }
        )

        rate, cols = validator.check_non_null_rate(df, ["open", "close"])

        assert rate < 1.0
        assert len(cols) == 2


class TestFreshnessCheck:
    """Test freshness validation."""

    def test_fresh_data(self):
        validator = DataQualityValidator()

        current_time = datetime(2024, 1, 10, 18, 0, tzinfo=UTC)

        df = pd.DataFrame(
            {
                "ts_event": [
                    datetime(2024, 1, 10, 16, 0, tzinfo=UTC),
                ]
            }
        )

        lag, is_fresh = validator.check_freshness(df, max_lag_hours=3, current_time=current_time)

        assert lag == pytest.approx(2.0)
        assert is_fresh

    def test_stale_data(self):
        validator = DataQualityValidator()

        current_time = datetime(2024, 1, 10, 18, 0, tzinfo=UTC)

        df = pd.DataFrame(
            {
                "ts_event": [
                    datetime(2024, 1, 9, 16, 0, tzinfo=UTC),
                ]
            }
        )

        lag, is_fresh = validator.check_freshness(df, max_lag_hours=2, current_time=current_time)

        assert lag > 2.0
        assert not is_fresh


class TestGapCheck:
    """Test gap duration validation."""

    def test_no_gaps(self):
        validator = DataQualityValidator()

        df = pd.DataFrame(
            {
                "ts_event": pd.date_range("2024-01-01", periods=10, freq="h"),
            }
        )

        gap, dates = validator.check_gaps(df, max_gap_seconds=7200)

        assert gap == pytest.approx(3600)
        assert len(dates) == 0

    def test_has_gap(self):
        validator = DataQualityValidator()

        df = pd.DataFrame(
            {
                "ts_event": [
                    datetime(2024, 1, 1, 10, 0),
                    datetime(2024, 1, 1, 11, 0),
                    datetime(2024, 1, 3, 10, 0),
                ]
            }
        )

        gap, dates = validator.check_gaps(df, max_gap_seconds=86400)

        assert gap > 86400
        assert len(dates) > 0


class TestFullValidation:
    """Test full validation workflow."""

    def test_validate_passing(self):
        validator = DataQualityValidator()
        validator.add_contract(
            "test_dataset",
            QualityContract(
                metric=QualityMetric.NON_NULL_RATE,
                threshold=0.95,
                columns=["value"],
            ),
        )

        df = pd.DataFrame(
            {
                "value": [1, 2, 3, 4, 5],
            }
        )

        report = validator.validate(
            dataset="test_dataset",
            layer="silver",
            df=df,
            time_range=(datetime(2024, 1, 1), datetime(2024, 1, 5)),
        )

        assert report.passed
        assert len(report.violations) == 0

    def test_validate_failing(self):
        validator = DataQualityValidator()
        validator.add_contract(
            "test_dataset",
            QualityContract(
                metric=QualityMetric.NON_NULL_RATE,
                threshold=0.99,
                columns=["value"],
            ),
        )

        df = pd.DataFrame(
            {
                "value": [1, None, 3, None, 5],
            }
        )

        report = validator.validate(
            dataset="test_dataset",
            layer="silver",
            df=df,
            time_range=(datetime(2024, 1, 1), datetime(2024, 1, 5)),
        )

        assert not report.passed
        assert len(report.violations) == 1


class TestDefaultValidator:
    """Test default validator creation."""

    def test_create_default_validator(self):
        validator = create_default_validator()

        assert "bars" in validator.contracts
        assert "trades" in validator.contracts
        assert len(validator.contracts["bars"]) >= 4
