"""Silver reads fail closed on cross-fragment type drift, like Gold reads (#50).

`pyarrow.dataset` infers a dataset's schema from one fragment and casts every
other fragment into it. When fragments disagree on a column's type and the
cast is one pyarrow considers safe — int64 to string, decimal to double —
there is no error at all: values are silently rewritten, and which way depends
on file enumeration order. `read_gold` and `read_parquet_dataset` already
refuse this. `read_silver` and `read_asof` now do too.

The check reads each scanned fragment's physical schema *after* the scan, from
the footer pyarrow already parsed and cached for that scan, so it adds no file
opens or reads to the hot Silver path.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.reader import core
from heber.reader.core import HeberReader, SchemaContractError

pytestmark = pytest.mark.unit

_TS = pa.timestamp("us", tz="UTC")


def _write(root: Path, dt: str, column: str, values: pa.Array, keys: list[str] | None = None) -> None:
    part = root / "silver" / "feed=drift" / "instrument_type=equity" / f"dt={dt}"
    part.mkdir(parents=True, exist_ok=True)
    n = len(values)
    ts = pd.Timestamp(f"{dt}T15:00:00", tz="UTC").to_pydatetime()
    pq.write_table(
        pa.table(
            {
                "ts_event": pa.array([ts] * n, _TS),
                "ts_available": pa.array([ts] * n, _TS),
                "instrument_key": keys or ["equity:AAPL"] * n,
                column: values,
            }
        ),
        str(part / "part-0.parquet"),
    )


# Both orderings: which fragment pyarrow samples first decides whether it raises
# or silently coerces, so the guarantee has to hold either way round.
_ORDERS = pytest.mark.parametrize("first_is_a", [True, False], ids=["a-first", "b-first"])


def _write_pair(root: Path, column: str, a: pa.Array, b: pa.Array, first_is_a: bool) -> None:
    early, late = (a, b) if first_is_a else (b, a)
    _write(root, "2026-08-01", column, early)
    _write(root, "2026-08-02", column, late)


@_ORDERS
def test_decimal_vs_double_drift_raises(tmp_path: Path, first_is_a: bool) -> None:
    _write_pair(
        tmp_path,
        "notional",
        pa.array([Decimal("12345678901234567.89")], pa.decimal128(20, 2)),
        pa.array([1.5], pa.float64()),
        first_is_a,
    )

    with pytest.raises(SchemaContractError, match="notional"):
        HeberReader(tmp_path).read_silver("drift")


@_ORDERS
def test_string_vs_numeric_drift_raises(tmp_path: Path, first_is_a: bool) -> None:
    _write_pair(
        tmp_path,
        "expiry",
        pa.array(["2026-09-18"], pa.string()),
        pa.array([20260918], pa.int64()),
        first_is_a,
    )

    with pytest.raises(SchemaContractError, match="expiry"):
        HeberReader(tmp_path).read_silver("drift")


@_ORDERS
def test_read_asof_raises_on_the_same_drift(tmp_path: Path, first_is_a: bool) -> None:
    _write_pair(
        tmp_path,
        "expiry",
        pa.array(["2026-09-18"], pa.string()),
        pa.array([20260918], pa.int64()),
        first_is_a,
    )

    with pytest.raises(SchemaContractError, match="expiry"):
        HeberReader(tmp_path).read_asof("drift", asof_time="2026-08-03T00:00:00Z")


@_ORDERS
def test_naive_vs_aware_timestamp_on_any_column_raises(tmp_path: Path, first_is_a: bool) -> None:
    """Arrow reads a naive value as a UTC instant without complaint, so a
    column written naive by one producer and UTC by another can shift by the
    producer's offset. The scan succeeding proves nothing here, and Silver's
    contract is timezone-aware UTC on every timestamp column."""
    when = pd.Timestamp("2026-08-01T09:30:00").to_pydatetime()
    _write_pair(
        tmp_path,
        "bar_start_ts",
        pa.array([when], pa.timestamp("us")),
        pa.array([when], pa.timestamp("us", tz="UTC")),
        first_is_a,
    )

    with pytest.raises(SchemaContractError, match="bar_start_ts"):
        HeberReader(tmp_path).read_silver("drift")


@_ORDERS
def test_timezone_drift_is_caught_when_a_widening_forces_the_recovery_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, first_is_a: bool
) -> None:
    """An int64-then-2.5 column makes the first scan raise, so the read recovers
    through the unified-schema path; the timezone check must still run there."""
    when = pd.Timestamp("2026-08-01T09:30:00").to_pydatetime()

    def write(dt: str, volume: pa.Array, start: pa.Array) -> None:
        part = tmp_path / "silver" / "feed=drift" / "instrument_type=equity" / f"dt={dt}"
        part.mkdir(parents=True, exist_ok=True)
        ts = pd.Timestamp(f"{dt}T15:00:00", tz="UTC").to_pydatetime()
        pq.write_table(
            pa.table(
                {
                    "ts_event": pa.array([ts], _TS),
                    "ts_available": pa.array([ts], _TS),
                    "instrument_key": ["equity:AAPL"],
                    "volume": volume,
                    "bar_start_ts": start,
                }
            ),
            str(part / "part-0.parquet"),
        )

    naive = (pa.array([1], pa.int64()), pa.array([when], pa.timestamp("us")))
    aware = (pa.array([2.5], pa.float64()), pa.array([when], pa.timestamp("us", tz="UTC")))
    early, late = (naive, aware) if first_is_a else (aware, naive)
    write("2026-08-01", *early)
    write("2026-08-02", *late)
    recovered: list[bool] = []
    real_unified = core._unified_dataset

    def spy(*args: Any, **kwargs: Any) -> Any:
        recovered.append(True)
        return real_unified(*args, **kwargs)

    monkeypatch.setattr(core, "_unified_dataset", spy)

    with pytest.raises(SchemaContractError, match="bar_start_ts"):
        HeberReader(tmp_path).read_silver("drift")
    if first_is_a:
        assert recovered, "the int64-first order must exercise the recovery path"


def test_timestamp_unit_difference_still_reads(tmp_path: Path) -> None:
    """ms vs us on the same timezone is a widening, not drift."""
    when = pd.Timestamp("2026-08-01T09:30:00", tz="UTC").to_pydatetime()
    _write(tmp_path, "2026-08-01", "bar_start_ts", pa.array([when], pa.timestamp("ms", tz="UTC")))
    _write(tmp_path, "2026-08-02", "bar_start_ts", pa.array([when], pa.timestamp("us", tz="UTC")))

    df = HeberReader(tmp_path).read_silver("drift")

    assert len(df) == 2


def test_drift_in_a_middle_fragment_is_caught(tmp_path: Path) -> None:
    """A string -> int64 -> string sequence; an endpoint-only probe misses it."""
    _write(tmp_path, "2026-08-01", "expiry", pa.array(["2026-09-18"], pa.string()))
    _write(tmp_path, "2026-08-02", "expiry", pa.array([20260918], pa.int64()))
    _write(tmp_path, "2026-08-03", "expiry", pa.array(["2026-09-25"], pa.string()))

    with pytest.raises(SchemaContractError, match="expiry"):
        HeberReader(tmp_path).read_silver("drift")


def test_drift_outside_a_scoped_read_does_not_fail_it(tmp_path: Path) -> None:
    """Only fragments the read actually scans are checked."""
    _write(tmp_path, "2026-08-01", "expiry", pa.array(["2026-09-18"], pa.string()))
    _write(tmp_path, "2026-08-05", "expiry", pa.array([20260918], pa.int64()))

    df = HeberReader(tmp_path).read_silver(
        "drift", time_range=("2026-08-01T00:00:00Z", "2026-08-01T23:59:59Z"), prune_by_dt=True
    )

    assert df["expiry"].tolist() == ["2026-09-18"]


def test_consistent_schemas_read_unchanged(tmp_path: Path) -> None:
    _write(tmp_path, "2026-08-01", "close", pa.array([1.0, 2.0]), keys=["equity:AAPL", "equity:MSFT"])
    _write(tmp_path, "2026-08-02", "close", pa.array([3.0]))

    df = HeberReader(tmp_path).read_silver("drift")

    assert sorted(df["close"].tolist()) == [1.0, 2.0, 3.0]
    assert df["close"].dtype == "float64"


def test_benign_numeric_widening_still_reads(tmp_path: Path) -> None:
    """int64 vs float64 is a widening the reader already settles, not drift."""
    _write(tmp_path, "2026-08-01", "volume", pa.array([1], pa.int64()))
    _write(tmp_path, "2026-08-02", "volume", pa.array([2.5], pa.float64()))

    df = HeberReader(tmp_path).read_silver("drift")

    assert sorted(df["volume"].tolist()) == [1.0, 2.5]


def test_asof_filter_is_still_pushed_into_the_scan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The cutoff reaches the pyarrow scan as a predicate, not a post-filter."""
    _write(tmp_path, "2026-08-01", "close", pa.array([1.0]))
    _write(tmp_path, "2026-08-03", "close", pa.array([3.0]))
    seen: list[Any] = []
    real_scan = core._scan_to_table

    def spy(*args: Any, **kwargs: Any) -> pa.Table:
        seen.append(kwargs.get("scan_filter"))
        return real_scan(*args, **kwargs)

    monkeypatch.setattr(core, "_scan_to_table", spy)

    df = HeberReader(tmp_path).read_asof("drift", asof_time="2026-08-02T00:00:00Z")

    assert df["close"].tolist() == [1.0]
    assert len(seen) == 1 and "ts_available" in str(seen[0])
