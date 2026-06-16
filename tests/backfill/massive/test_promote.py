"""Tests for crash-safe streaming promotion (DuckDB event_id dedup)."""

from datetime import UTC, datetime

import pyarrow.parquet as pq

from heber.backfill.massive.promote import promote_date
from heber.backfill.massive.silver_writer import make_bars_parquet_writer


def _record(event_id: str, ts_ingest: datetime):
    ts = datetime(2024, 1, 2, tzinfo=UTC)
    return {
        "event_id": event_id,
        "provider": "massive",
        "feed": "bars",
        "instrument_type": "equity",
        "instrument_key": "equity:AAPL",
        "symbol": "AAPL",
        "ts_event": ts,
        "ts_ingest": ts_ingest,
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
    }


def _seed_temp(part, subdir, records):
    writer = make_bars_parquet_writer()
    d = part / subdir
    d.mkdir(parents=True, exist_ok=True)
    writer(records, d / "bars.parquet")


def _make_part(tmp_path, dataset_dir="bars", dt_iso="2024-01-02"):
    part = tmp_path / "silver" / dataset_dir / f"dt={dt_iso}"
    part.mkdir(parents=True, exist_ok=True)
    return part


def test_promote_unions_temp_files_and_cleans_up(tmp_path):
    part = _make_part(tmp_path)
    ts = datetime(2024, 1, 2, 12, 0, tzinfo=UTC)
    _seed_temp(part, "_backfill_a", [_record("a" * 32, ts), _record("b" * 32, ts)])
    _seed_temp(part, "_backfill_b", [_record("c" * 32, ts)])

    rows = promote_date(str(tmp_path), "bars", "2024-01-02")

    assert rows == 3
    canonical = part / "part-00000.parquet"
    assert canonical.exists()
    assert pq.read_metadata(str(canonical)).num_rows == 3
    assert not list(part.glob("_backfill_*"))


def test_promote_is_idempotent_dedup_by_event_id(tmp_path):
    part = _make_part(tmp_path)
    ts1 = datetime(2024, 1, 2, 12, 0, tzinfo=UTC)
    _seed_temp(part, "_backfill_a", [_record("a" * 32, ts1), _record("b" * 32, ts1)])
    assert promote_date(str(tmp_path), "bars", "2024-01-02") == 2

    # Re-seed the SAME event_ids (newer ts_ingest) and re-promote: no duplicates.
    ts2 = datetime(2024, 1, 2, 13, 0, tzinfo=UTC)
    _seed_temp(part, "_backfill_c", [_record("a" * 32, ts2), _record("b" * 32, ts2)])
    rows = promote_date(str(tmp_path), "bars", "2024-01-02")

    assert rows == 2
    canonical = part / "part-00000.parquet"
    assert pq.read_metadata(str(canonical)).num_rows == 2


def test_promote_no_temp_returns_existing_count(tmp_path):
    # No part dir at all -> 0
    assert promote_date(str(tmp_path), "bars", "2024-01-02") == 0

    # Existing canonical, no temp files -> existing count
    part = _make_part(tmp_path, dt_iso="2024-01-03")
    ts = datetime(2024, 1, 2, 12, 0, tzinfo=UTC)
    make_bars_parquet_writer()([_record("a" * 32, ts)], part / "part-00000.parquet")
    assert promote_date(str(tmp_path), "bars", "2024-01-03") == 1
