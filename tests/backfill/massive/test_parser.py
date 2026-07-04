"""Tests for Massive aggregate CSV parsing into Silver bars records."""

from datetime import UTC, datetime

import pytest

from heber.backfill.massive.parser import (
    EXPECTED_HEADER,
    CsvHeaderError,
    RejectCollector,
    iter_record_chunks,
    row_to_record,
    validate_header,
)

# 1704153600000000000 ns == 2024-01-02T00:00:00Z
NS = 1704153600000000000


def _row(**over):
    row = {
        "ticker": "AAPL",
        "volume": "1000",
        "open": "100.5",
        "close": "101.0",
        "high": "102.0",
        "low": "99.5",
        "window_start": str(NS),
        "transactions": "42",
    }
    row.update(over)
    return row


def test_row_to_record_maps_all_fields():
    rec = row_to_record(_row(), "1d")
    assert rec["instrument_key"] == "equity:AAPL"
    assert rec["symbol"] == "AAPL"
    assert rec["raw_ticker"] == "AAPL"
    assert rec["ts_event"] == datetime(2024, 1, 2, tzinfo=UTC)
    assert rec["bar_start_ts"] == datetime(2024, 1, 2, tzinfo=UTC)
    assert rec["timeframe"] == "1d"
    assert rec["provider"] == "massive"
    assert rec["feed"] == "bars"
    assert rec["instrument_type"] == "equity"
    assert rec["source"] == "backfill"
    assert rec["schema_version"] == "v1"
    assert rec["open"] == 100.5
    assert rec["high"] == 102.0
    assert rec["low"] == 99.5
    assert rec["close"] == 101.0
    assert rec["volume"] == 1000.0
    assert rec["trade_count"] == 42
    assert isinstance(rec["trade_count"], int)
    assert rec["vwap"] is None
    assert len(rec["event_id"]) == 32


def test_row_to_record_brk_a_passes():
    rec = row_to_record(_row(ticker="BRK.A"), "1d")
    assert rec["instrument_key"] == "equity:BRK.A"
    assert rec["symbol"] == "BRK.A"


def test_bad_symbol_routed_to_rejects_good_row_yields():
    rows = [_row(ticker="BAD$SYM"), _row(ticker="AAPL")]
    rejects = RejectCollector()
    chunks = list(iter_record_chunks(rows, "1d", rejects))
    records = [r for c in chunks for r in c]
    assert len(records) == 1
    assert records[0]["symbol"] == "AAPL"
    assert rejects.count == 1
    assert rejects.items[0]["reason"] == "instrument_key_regex_failed"
    assert rejects.items[0]["ticker"] == "BAD$SYM"
    assert rejects.total_rows == 2


def test_malformed_numeric_routed_to_rejects():
    rows = [_row(open="x")]
    rejects = RejectCollector()
    list(iter_record_chunks(rows, "1d", rejects))
    assert rejects.count == 1
    assert rejects.items[0]["reason"].startswith("parse_error")


def test_reject_item_carries_correct_line_number():
    # line 1 is header, so first data row is line 2, second is line 3
    rows = [_row(ticker="AAPL"), _row(ticker="BAD$SYM")]
    rejects = RejectCollector()
    list(iter_record_chunks(rows, "1d", rejects))
    assert rejects.count == 1
    assert rejects.items[0]["line"] == 3


def test_validate_header_raises_on_wrong_header():
    with pytest.raises(CsvHeaderError):
        validate_header(["ticker", "open", "close"])


def test_validate_header_passes_on_expected():
    validate_header(list(EXPECTED_HEADER))  # should not raise


def test_reject_collector_flush_writes_jsonl(tmp_path):
    rejects = RejectCollector()
    rejects.add("BAD$SYM", "instrument_key_regex_failed", {"ticker": "BAD$SYM"}, line=2)
    out = tmp_path / "sub" / "rejects.jsonl"
    rejects.flush(out)
    assert out.exists()
    lines = out.read_text().strip().splitlines()
    assert len(lines) == 1
    assert "BAD$SYM" in lines[0]


def test_reject_collector_flush_noop_when_empty(tmp_path):
    rejects = RejectCollector()
    out = tmp_path / "rejects.jsonl"
    rejects.flush(out)
    assert not out.exists()
