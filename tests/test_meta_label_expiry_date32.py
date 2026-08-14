"""Tests for the expiry date32 guard in persist_features_to_gold.

A pre-2026-06-30 backfill path wrote ``expiry`` as a raw ``YYYYMMDD`` integer.
When such a row landed in a partition whose other rows held ``datetime.date``
values, pandas inferred ``date32`` and pyarrow read the integer as a raw day
count — ``20260819`` became year 57442 and ``pd.read_parquet`` raised
``year must be in 1..9999`` for the entire partition. Three Gold partitions
(2026-04-14, 2026-05-06, 2026-06-17) were silently dropped from model training
that way.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.ml.datasets import _REQUIRED_GREEK_COLUMNS, normalize_expiry, persist_features_to_gold


def _make_row(*, alert_id: str, expiry: object) -> dict:
    base = {
        "alert_id": alert_id,
        "alert_time": datetime(2026, 4, 14, 13, 30, tzinfo=UTC),
        "symbol": "VIX",
        "underlying": "VIX",
        "occ_symbol": "VIX260819C00022000",
        "instrument_key": "option:VIX260819C00022000",
        "expiry": expiry,
        "strike": 22.0,
        "put_call": "C",
        "premium": 12000.0,
        "volume": 100.0,
    }
    for col in _REQUIRED_GREEK_COLUMNS:
        base[col] = 0.1
    return base


def _partition_file(out_path: Path, dt_str: str = "2026-04-14") -> Path:
    return out_path / f"dt={dt_str}" / "data.parquet"


@pytest.mark.unit
class TestNormalizeExpiry:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            (date(2026, 9, 18), date(2026, 9, 18)),
            (datetime(2026, 9, 18, 14, 30), date(2026, 9, 18)),
            (np.datetime64("2026-09-18"), date(2026, 9, 18)),
            ("2026-09-18", date(2026, 9, 18)),
            ("2026-09-18T14:30:00", date(2026, 9, 18)),
            (" 2026-09-18 ", date(2026, 9, 18)),
            ("20260918", date(2026, 9, 18)),
            (20260918, date(2026, 9, 18)),
            (np.int64(20260918), date(2026, 9, 18)),
            (20260918.0, date(2026, 9, 18)),
        ],
    )
    def test_accepts_known_expiry_forms(self, raw: object, expected: date) -> None:
        assert normalize_expiry(raw) == expected

    @pytest.mark.parametrize(
        "raw",
        [
            None,
            float("nan"),
            pd.NaT,
            np.datetime64("NaT"),  # must return None, not crash the partition write
            "",
            "   ",
            "not-a-date",
            "2026-09-18garbage",  # trailing garbage must not be silently truncated
            "20260918garbage",
            20260918.5,  # non-integral float is not a date
            202609,  # partial dates are ambiguous, not guessable
            2026,
            True,
        ],
    )
    def test_rejects_ambiguous_or_malformed_values(self, raw: object) -> None:
        assert normalize_expiry(raw) is None


@pytest.mark.integration
class TestExpiryWrittenAsDate32:
    def test_integer_yyyymmdd_expiry_is_stored_as_a_real_date(self, tmp_path: Path) -> None:
        """The reported bug: an int expiry became a raw day count (year 57442)."""
        persist_features_to_gold(pd.DataFrame([_make_row(alert_id="a-1", expiry=20260819)]), tmp_path)

        frame = pd.read_parquet(_partition_file(tmp_path))
        assert frame["expiry"].tolist() == [date(2026, 8, 19)]

    def test_partition_mixing_date_and_integer_expiry_stays_readable(self, tmp_path: Path) -> None:
        """Reproduces the exact corruption: a date-valued partition plus one int row.

        Writing the rows in two calls matches production, where the live writer
        created the partition and the backfill scanner appended to it later.
        """
        persist_features_to_gold(
            pd.DataFrame([_make_row(alert_id="live-1", expiry=date(2026, 8, 19))]),
            tmp_path,
        )
        persist_features_to_gold(
            pd.DataFrame([_make_row(alert_id="backfill-1", expiry=20260819)]),
            tmp_path,
        )

        frame = pd.read_parquet(_partition_file(tmp_path))
        assert sorted(frame["alert_id"]) == ["backfill-1", "live-1"]
        assert frame["expiry"].tolist() == [date(2026, 8, 19), date(2026, 8, 19)]

    def test_expiry_column_is_pinned_to_date32_even_when_all_null(self, tmp_path: Path) -> None:
        """An all-null column infers as Arrow ``null``, which conflicts with
        ``date32`` in sibling partitions and breaks whole-dataset reads."""
        persist_features_to_gold(pd.DataFrame([_make_row(alert_id="a-1", expiry=None)]), tmp_path)

        schema = pq.ParquetFile(_partition_file(tmp_path)).schema_arrow
        assert pa.types.is_date32(schema.field("expiry").type)

    def test_string_expiry_partition_is_normalized_on_append(self, tmp_path: Path) -> None:
        """Legacy string partitions become date32 the next time they are written."""
        partition = tmp_path / "dt=2026-04-14"
        partition.mkdir(parents=True)
        pd.DataFrame([_make_row(alert_id="legacy-1", expiry="2026-08-19")]).to_parquet(
            partition / "data.parquet", index=False
        )

        persist_features_to_gold(pd.DataFrame([_make_row(alert_id="new-1", expiry=date(2026, 8, 19))]), tmp_path)

        schema = pq.ParquetFile(_partition_file(tmp_path)).schema_arrow
        assert pa.types.is_date32(schema.field("expiry").type)
        frame = pd.read_parquet(_partition_file(tmp_path))
        assert sorted(frame["alert_id"]) == ["legacy-1", "new-1"]
        assert frame["expiry"].tolist() == [date(2026, 8, 19), date(2026, 8, 19)]

    def test_unparseable_expiry_becomes_null_rather_than_corrupting_the_partition(self, tmp_path: Path) -> None:
        persist_features_to_gold(
            pd.DataFrame(
                [
                    _make_row(alert_id="good-1", expiry=date(2026, 8, 19)),
                    _make_row(alert_id="bad-1", expiry="not-a-date"),
                ]
            ),
            tmp_path,
        )

        frame = pd.read_parquet(_partition_file(tmp_path)).set_index("alert_id")
        assert frame.loc["good-1", "expiry"] == date(2026, 8, 19)
        assert pd.isna(frame.loc["bad-1", "expiry"])
