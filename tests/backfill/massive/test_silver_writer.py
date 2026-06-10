"""Tests for the schema-enforcing atomic Silver bars parquet writer."""

from datetime import UTC, datetime

import pyarrow.parquet as pq

from heber.backfill.massive.silver_writer import make_bars_parquet_writer
from heber.schemas.silver import get_silver_schema


def _record():
    ts = datetime(2024, 1, 2, tzinfo=UTC)
    return {
        "event_id": "a" * 32,
        "provider": "massive",
        "feed": "bars",
        "instrument_type": "equity",
        "instrument_key": "equity:AAPL",
        "symbol": "AAPL",
        "ts_event": ts,
        "ts_ingest": ts,
        "ts_available": ts,
        "source": "backfill",
        "schema_version": "v1",
        "quality_flags": [],
        "timeframe": "1d",
        "bar_start_ts": ts,
        "open": 100.5,
        "high": 102.0,
        "low": 99.5,
        "close": 101.0,
        "volume": 1000.0,
        "trade_count": 42,
        "vwap": None,
        # extra keys that must be dropped by the explicit schema
        "backfill_id": "job-123",
        "raw_ticker": "AAPL",
    }


def test_writer_enforces_schema_and_drops_extras(tmp_path):
    writer = make_bars_parquet_writer()
    path = tmp_path / "bars.parquet"
    writer([_record()], path)

    assert path.exists()
    table = pq.read_table(str(path))
    expected = get_silver_schema("bars")
    assert table.schema.equals(expected)
    assert table.num_rows == 1

    cols = set(table.schema.names)
    assert "backfill_id" not in cols
    assert "raw_ticker" not in cols
    assert table.column("vwap").to_pylist() == [None]


def test_writer_leaves_no_tmp_files(tmp_path):
    writer = make_bars_parquet_writer()
    path = tmp_path / "bars.parquet"
    writer([_record()], path)
    leftover = list(tmp_path.glob("*.tmp"))
    assert leftover == []
