from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime
from pathlib import Path

from heber.writer.transformer import BronzeToSilverTransformer

NOW = datetime(2026, 2, 12, 15, 30, tzinfo=UTC)


def _write_bronze_file(path: Path, envelope: dict) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps(envelope) + "\n")


def _base_envelope(*, feed: str, payload: dict) -> dict:
    return {
        "event_id": f"evt-{feed}-1",
        "provider": "alpaca",
        "feed": feed,
        "source": "rest",
        "instrument_type": "equity",
        "instrument_key": "equity:AAPL",
        "symbol": "AAPL",
        "ts_event": NOW.isoformat(),
        "ts_ingest": NOW.isoformat(),
        "payload": payload,
    }


def test_transformer_expands_bars_aggregate_payload_to_multiple_rows(tmp_path: Path) -> None:
    transformer = BronzeToSilverTransformer(bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver")
    bronze_file = tmp_path / "bars.jsonl.gz"

    _write_bronze_file(
        bronze_file,
        _base_envelope(
            feed="bars",
            payload={
                "symbol": "AAPL",
                "timeframe": "1Min",
                "bars": [
                    {
                        "timestamp": "2026-02-12T15:30:00Z",
                        "open": 100.0,
                        "high": 101.0,
                        "low": 99.5,
                        "close": 100.5,
                        "volume": 1200,
                        "trade_count": 10,
                        "vwap": 100.2,
                    },
                    {
                        "timestamp": "2026-02-12T15:31:00Z",
                        "open": 100.5,
                        "high": 101.2,
                        "low": 100.2,
                        "close": 101.0,
                        "volume": 950,
                        "trade_count": 8,
                        "vwap": 100.8,
                    },
                ],
            },
        ),
    )

    rows = transformer._read_bronze_file(bronze_file, "bars")  # noqa: SLF001

    assert len(rows) == 2
    assert all(row["feed"] == "bars" for row in rows)
    assert [row["bar_start_ts"] for row in rows] == [
        datetime(2026, 2, 12, 15, 30, tzinfo=UTC),
        datetime(2026, 2, 12, 15, 31, tzinfo=UTC),
    ]
    assert [row["volume"] for row in rows] == [1200, 950]
    assert all(row["timeframe"] == "1Min" for row in rows)


def test_transformer_expands_trades_aggregate_and_inherits_symbol_context(tmp_path: Path) -> None:
    transformer = BronzeToSilverTransformer(bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver")
    bronze_file = tmp_path / "trades.jsonl.gz"

    _write_bronze_file(
        bronze_file,
        _base_envelope(
            feed="trades",
            payload={
                "symbol": "AAPL",
                "trades": [
                    {"timestamp": "2026-02-12T15:30:00Z", "price": 100.2, "size": 50, "exchange": "XNAS"},
                    {"timestamp": "2026-02-12T15:30:01Z", "price": 100.3, "size": 10, "exchange": "XNAS"},
                ],
            },
        ),
    )

    rows = transformer._read_bronze_file(bronze_file, "trades")  # noqa: SLF001

    assert len(rows) == 2
    assert all(row["feed"] == "trades" for row in rows)
    assert [row["price"] for row in rows] == [100.2, 100.3]
    assert [row["size"] for row in rows] == [50, 10]
    assert all(row["symbol"] == "AAPL" for row in rows)


def test_transformer_skips_non_dict_aggregate_items_and_keeps_valid_items(tmp_path: Path) -> None:
    transformer = BronzeToSilverTransformer(bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver")
    bronze_file = tmp_path / "bars-mixed.jsonl.gz"

    _write_bronze_file(
        bronze_file,
        _base_envelope(
            feed="bars",
            payload={
                "symbol": "AAPL",
                "timeframe": "1Min",
                "bars": [
                    {
                        "timestamp": "2026-02-12T15:30:00Z",
                        "open": 100.0,
                        "high": 101.0,
                        "low": 99.5,
                        "close": 100.5,
                        "volume": 1200,
                        "trade_count": 10,
                        "vwap": 100.2,
                    },
                    "bad-item",
                ],
            },
        ),
    )

    rows = transformer._read_bronze_file(bronze_file, "bars")  # noqa: SLF001

    assert len(rows) == 1
    assert rows[0]["bar_start_ts"] == datetime(2026, 2, 12, 15, 30, tzinfo=UTC)


def test_transformer_skips_aggregate_items_missing_required_fields(tmp_path: Path) -> None:
    transformer = BronzeToSilverTransformer(bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver")
    bronze_file = tmp_path / "bars-required-fields.jsonl.gz"

    _write_bronze_file(
        bronze_file,
        _base_envelope(
            feed="bars",
            payload={
                "symbol": "AAPL",
                "timeframe": "1Min",
                "bars": [
                    {
                        "timestamp": "2026-02-12T15:30:00Z",
                        "open": 100.0,
                        "high": 101.0,
                        "low": 99.5,
                        "close": 100.5,
                        "volume": 1200,
                        "trade_count": 10,
                        "vwap": 100.2,
                    },
                    {
                        "timestamp": "2026-02-12T15:31:00Z",
                        "high": 101.2,
                        "low": 100.2,
                        "close": 101.0,
                        "volume": 950,
                        "trade_count": 8,
                        "vwap": 100.8,
                    },
                ],
            },
        ),
    )

    rows = transformer._read_bronze_file(bronze_file, "bars")  # noqa: SLF001

    assert len(rows) == 1
    assert rows[0]["bar_start_ts"] == datetime(2026, 2, 12, 15, 30, tzinfo=UTC)
