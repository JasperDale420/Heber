"""A feed that cannot be read must not look like a feed with no data.

`_open_dataset_safe` falls back to manual schema unification whenever fragments
disagree on string encoding, which is every feed the compactor has touched. That
fallback rebuilds the hive partition columns from `os.path.commonpath(file_list)`
— but pyarrow discovers partition fields from the full paths, so when a read is
not scoped to one `instrument_type` the two disagree and the open raises
`ArrowInvalid: No match for FieldRef.Name(...)`. The broad `except` turns that
into `None`, and `read_silver` turns `None` into an empty DataFrame.

The health monitor's liveness and statistical checks, the EOD reconcile, and
several Gold pipelines all read feeds without `instrument_type`. For them a
broken read is indistinguishable from "this feed produced nothing today".
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from heber.reader.core import HeberReader


def _schema(string_type: pa.DataType) -> pa.Schema:
    return pa.schema(
        [
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("feed", string_type),
            ("instrument_type", string_type),
            ("instrument_key", string_type),
            ("close", pa.float64()),
        ]
    )


def _write(base: Path, instrument_type: str, hours: int, *, name: str, string_type: pa.DataType) -> None:
    ts = datetime(2026, 3, 2, tzinfo=UTC) + timedelta(hours=hours)
    row = {
        "ts_event": ts,
        "ts_available": ts,
        "feed": "bars",
        "instrument_type": instrument_type,
        "instrument_key": f"{instrument_type}:AAPL",
        "close": 100.0 + hours,
    }
    part = base / "silver" / "feed=bars" / f"instrument_type={instrument_type}" / f"dt={ts.strftime('%Y-%m-%d')}"
    part.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(pd.DataFrame([row]), schema=_schema(string_type))
    pq.write_table(table, str(part / name))


def _conflicted_feed(base: Path) -> Path:
    """Two instrument_types, fragments disagreeing on string encoding."""
    _write(base, "equity", 0, name="plain.parquet", string_type=pa.string())
    _write(base, "equity", 24, name="large.parquet", string_type=pa.large_string())
    _write(base, "crypto", 0, name="plain.parquet", string_type=pa.string())
    _write(base, "crypto", 24, name="large.parquet", string_type=pa.large_string())
    return base


def test_scoped_read_of_a_conflicted_feed_works(tmp_path: Path) -> None:
    """Baseline: scoping to one instrument_type has always worked."""
    reader = HeberReader(_conflicted_feed(tmp_path))

    assert len(reader.read_silver("bars", instrument_type="equity")) == 2


def test_unscoped_read_of_a_conflicted_feed_returns_rows(tmp_path: Path) -> None:
    """The whole feed must be readable without naming an instrument_type.

    Returning an empty frame here is the dangerous failure: the caller cannot
    tell it apart from a feed that genuinely produced nothing.
    """
    reader = HeberReader(_conflicted_feed(tmp_path))

    rows = reader.read_silver("bars")

    assert len(rows) == 4, "unscoped read of a conflicted feed came back short or empty"
    assert set(rows["instrument_type"]) == {"equity", "crypto"}


def test_unscoped_read_exposes_the_partition_columns(tmp_path: Path) -> None:
    """dt and instrument_type must still be usable as predicates after unification."""
    reader = HeberReader(_conflicted_feed(tmp_path))

    rows = reader.read_silver("bars", time_range=("2026-03-03", "2026-03-04"), prune_by_dt=True)

    assert len(rows) == 2, "dt pruning over a unified schema dropped everything"
