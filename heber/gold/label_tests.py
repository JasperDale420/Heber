"""Tests for Label Management (PRD §29)."""

import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from heber.gold.labels import (
    LabelDataset,
    LabelMetadata,
    compute_availability_time,
    parse_duration,
    read_label,
    write_label,
)


class TestParseDuration:
    """Test duration string parsing."""

    def test_days(self):
        assert parse_duration("5d") == timedelta(days=5)
        assert parse_duration("1d") == timedelta(days=1)

    def test_hours(self):
        assert parse_duration("24h") == timedelta(hours=24)
        assert parse_duration("1h") == timedelta(hours=1)

    def test_minutes(self):
        assert parse_duration("30m") == timedelta(minutes=30)

    def test_seconds(self):
        assert parse_duration("0s") == timedelta(seconds=0)
        assert parse_duration("60s") == timedelta(seconds=60)

    def test_invalid_format(self):
        with pytest.raises(ValueError):
            parse_duration("invalid")


class TestLabelMetadata:
    """Test LabelMetadata serialization."""

    def test_to_dict(self):
        meta = LabelMetadata(
            forward_window="5d",
            label_horizon="close_to_close",
            availability_lag="5m",
        )
        d = meta.to_dict()
        assert d["dataset_type"] == "label"
        assert d["forward_window"] == "5d"
        assert d["label_horizon"] == "close_to_close"

    def test_from_dict_roundtrip(self):
        meta = LabelMetadata(forward_window="10d", availability_lag="1h")
        restored = LabelMetadata.from_dict(meta.to_dict())
        assert restored.forward_window == "10d"
        assert restored.availability_lag == "1h"

    def test_get_forward_window_delta(self):
        meta = LabelMetadata(forward_window="5d")
        assert meta.get_forward_window_delta() == timedelta(days=5)


class TestComputeAvailabilityTime:
    """Test availability time calculation (PRD §29.3)."""

    def test_5d_forward_window(self):
        label_time = datetime(2025, 1, 10, 9, 30, tzinfo=UTC)
        availability = compute_availability_time(label_time, "5d")

        assert availability.date() == datetime(2025, 1, 15).date()
        assert availability.hour == 16
        assert availability.minute == 5

    def test_1d_forward_window(self):
        label_time = datetime(2025, 1, 10, 9, 30, tzinfo=UTC)
        availability = compute_availability_time(label_time, "1d")

        assert availability.date() == datetime(2025, 1, 11).date()

    def test_with_availability_lag(self):
        label_time = datetime(2025, 1, 10, 9, 30, tzinfo=UTC)
        availability = compute_availability_time(label_time, "5d", "1h")

        assert availability.date() == datetime(2025, 1, 15).date()


class TestLabelDataset:
    """Test LabelDataset persistence."""

    def test_save_and_load(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            gold_root = Path(tmpdir)

            dataset = LabelDataset(
                name="returns_5d",
                metadata=LabelMetadata(forward_window="5d"),
                version="v1.0.0",
                created_by="test",
            )
            dataset.save(gold_root)

            loaded = LabelDataset.load(gold_root, "returns_5d")
            assert loaded.name == "returns_5d"
            assert loaded.metadata.forward_window == "5d"


class TestWriteAndReadLabel:
    """Test write_label and read_label functions (PRD §29.3-29.4)."""

    def test_write_label_computes_availability(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            gold_root = Path(tmpdir)

            df = pd.DataFrame(
                {
                    "instrument_key": ["AAPL", "MSFT"],
                    "ts_label": [
                        datetime(2025, 1, 10, 9, 30, tzinfo=UTC),
                        datetime(2025, 1, 10, 9, 30, tzinfo=UTC),
                    ],
                    "label": [0.05, 0.03],
                }
            )

            write_label(
                gold_root=gold_root,
                dataset="returns_5d",
                df=df,
                forward_window="5d",
            )

            written = pd.read_parquet(gold_root / "dataset=returns_5d/type=label/version=v1.0.0/data.parquet")

            assert "ts_available" in written.columns
            assert all(written["ts_available"] >= written["ts_event"])

    def test_read_label_filters_by_asof(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            gold_root = Path(tmpdir)

            df = pd.DataFrame(
                {
                    "instrument_key": ["AAPL", "AAPL"],
                    "ts_label": [
                        datetime(2025, 1, 5, 9, 30, tzinfo=UTC),
                        datetime(2025, 1, 12, 9, 30, tzinfo=UTC),
                    ],
                    "label": [0.02, 0.04],
                }
            )

            write_label(
                gold_root=gold_root,
                dataset="returns_5d",
                df=df,
                forward_window="5d",
            )

            asof_time = datetime(2025, 1, 15, 16, 10, tzinfo=UTC)
            result = read_label(gold_root, "returns_5d", asof_time)

            assert len(result) == 1
            assert result.iloc[0]["label"] == 0.02

    def test_read_label_filters_by_instrument(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            gold_root = Path(tmpdir)

            df = pd.DataFrame(
                {
                    "instrument_key": ["AAPL", "MSFT", "GOOGL"],
                    "ts_label": [datetime(2025, 1, 5, 9, 30, tzinfo=UTC)] * 3,
                    "label": [0.01, 0.02, 0.03],
                }
            )

            write_label(
                gold_root=gold_root,
                dataset="returns_5d",
                df=df,
                forward_window="5d",
            )

            asof_time = datetime(2025, 1, 20, 16, 10, tzinfo=UTC)
            result = read_label(
                gold_root,
                "returns_5d",
                asof_time,
                instrument_keys=["AAPL", "MSFT"],
            )

            assert len(result) == 2
            assert set(result["instrument_key"]) == {"AAPL", "MSFT"}


def run_all_label_tests() -> dict[str, bool]:
    """Run all label management tests.

    Returns:
        Dict mapping test name to pass/fail
    """
    results = {}

    test_classes = [
        TestParseDuration,
        TestLabelMetadata,
        TestComputeAvailabilityTime,
        TestLabelDataset,
        TestWriteAndReadLabel,
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
    print(f"\nLabel Management Tests: {passed}/{total} passed")

    return results


if __name__ == "__main__":
    run_all_label_tests()
