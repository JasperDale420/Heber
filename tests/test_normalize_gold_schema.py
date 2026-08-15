"""Tests for scripts/normalize_gold_schema.py.

The normalizer retypes existing training partitions in place, so the property
that matters is that it changes *typing only* — never values, rows, or columns.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from types import ModuleType

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "normalize_gold_schema.py"


def _load_script() -> ModuleType:
    spec = importlib.util.spec_from_file_location("normalize_gold_schema", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["normalize_gold_schema"] = module
    spec.loader.exec_module(module)
    return module


normalize_gold_schema = _load_script()


@pytest.mark.unit
class TestConvertColumn:
    def test_all_null_column_becomes_typed_nulls(self) -> None:
        column = pa.chunked_array([pa.nulls(3, type=pa.null())])

        result = normalize_gold_schema.convert_column(column, pa.float64())

        assert result.type == pa.float64()
        assert result.to_pylist() == [None, None, None]

    def test_integer_yyyymmdd_expiry_becomes_a_real_date(self) -> None:
        column = pa.chunked_array([pa.array([20260819, None], type=pa.int64())])

        result = normalize_gold_schema.convert_column(column, pa.date32())

        assert result.to_pylist() == [date(2026, 8, 19), None]

    def test_string_expiry_becomes_a_real_date(self) -> None:
        column = pa.chunked_array([pa.array(["2026-08-19", "20260511"], type=pa.string())])

        result = normalize_gold_schema.convert_column(column, pa.date32())

        assert result.to_pylist() == [date(2026, 8, 19), date(2026, 5, 11)]

    def test_microsecond_timestamps_become_nanosecond(self) -> None:
        moment = datetime(2026, 4, 14, 13, 30, tzinfo=UTC)
        column = pa.chunked_array([pa.array([moment], type=pa.timestamp("us", tz="UTC"))])

        result = normalize_gold_schema.convert_column(column, pa.timestamp("ns", tz="UTC"))

        assert result.to_pylist() == [moment]


@pytest.mark.unit
class TestValidate:
    def _table(self, expiry: pa.Array, extra: float | None = 1.0) -> pa.Table:
        return pa.table(
            {
                "alert_id": pa.array(["a-1"], type=pa.string()),
                "expiry": expiry,
                "delta": pa.array([extra], type=pa.float64() if extra is not None else pa.null()),
            }
        )

    def test_accepts_a_pure_retype(self) -> None:
        original = self._table(pa.array([20260819], type=pa.int64()))
        candidate = self._table(pa.array([date(2026, 8, 19)], type=pa.date32()))

        assert normalize_gold_schema.validate(original, candidate) == []

    def test_rejects_a_changed_value(self) -> None:
        original = self._table(pa.array([20260819], type=pa.int64()))
        candidate = self._table(pa.array([date(2026, 1, 1)], type=pa.date32()))

        assert any("values changed" in p for p in normalize_gold_schema.validate(original, candidate))

    def test_rejects_a_dropped_row(self) -> None:
        original = self._table(pa.array([20260819], type=pa.int64()))
        candidate = pa.table({"alert_id": pa.array([], type=pa.string())})

        assert any("row count changed" in p for p in normalize_gold_schema.validate(original, candidate))

    def test_rejects_a_wrong_target_type(self) -> None:
        original = self._table(pa.array([20260819], type=pa.int64()))
        candidate = self._table(pa.array([20260819], type=pa.int64()))

        assert any("expected date32" in p for p in normalize_gold_schema.validate(original, candidate))


@pytest.mark.integration
class TestPlanNormalization:
    def test_returns_none_when_the_partition_is_already_correct(self) -> None:
        table = pa.table(
            {
                "alert_id": pa.array(["a-1"], type=pa.string()),
                "expiry": pa.array([date(2026, 8, 19)], type=pa.date32()),
                "delta": pa.array([0.5], type=pa.float64()),
            }
        )

        assert normalize_gold_schema.plan_normalization(table) is None

    def test_undeclared_columns_are_passed_through_untouched(self) -> None:
        table = pa.table(
            {
                "alert_id": pa.array(["a-1"], type=pa.string()),
                "delta": pa.array([None], type=pa.null()),
                "some_new_feature": pa.array([7], type=pa.int32()),
            }
        )

        result = normalize_gold_schema.plan_normalization(table)

        assert result is not None
        assert result.column("delta").type == pa.float64()
        assert result.column("some_new_feature").type == pa.int32(), "an undeclared column must not be retyped"
        assert result.column("some_new_feature").to_pylist() == [7]

    def test_round_trips_through_parquet(self, tmp_path: Path) -> None:
        path = tmp_path / "dt=2026-04-14" / "data.parquet"
        path.parent.mkdir(parents=True)
        original = pa.table(
            {
                "alert_id": pa.array(["a-1", "a-2"], type=pa.string()),
                "expiry": pa.array(["2026-08-19", None], type=pa.string()),
                "delta": pa.array([None, None], type=pa.null()),
            }
        )
        pq.write_table(original, path)

        candidate = normalize_gold_schema.plan_normalization(pq.ParquetFile(path).read())
        assert candidate is not None
        assert normalize_gold_schema.validate(pq.ParquetFile(path).read(), candidate) == []

        pq.write_table(candidate, path)
        result = pq.ParquetFile(path).read()
        assert result.column("expiry").to_pylist() == [date(2026, 8, 19), None]
        assert result.column("delta").type == pa.float64()
        assert result.column("alert_id").to_pylist() == ["a-1", "a-2"]
