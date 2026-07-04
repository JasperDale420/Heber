"""Parse Massive aggregate CSV rows into Silver-`bars` record dicts, with durable reject capture."""

import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from heber.backfill.massive.event_id import compute_bar_event_id
from heber.models.envelope import INSTRUMENT_KEY_PATTERNS

PROVIDER = "massive"
FEED = "bars"
INSTRUMENT_TYPE = "equity"
SOURCE = "backfill"
SCHEMA_VERSION = "v1"
TIMEFRAME_BY_DATASET = {"day_aggs_v1": "1d", "minute_aggs_v1": "1m"}
EXPECTED_HEADER = ["ticker", "volume", "open", "close", "high", "low", "window_start", "transactions"]

_EQUITY_KEY = INSTRUMENT_KEY_PATTERNS["equity"]


class InvalidTickerError(ValueError):
    """Ticker cannot form a valid equity instrument_key."""


class CsvHeaderError(ValueError):
    """The CSV header does not match the expected Massive aggregate schema."""


@dataclass
class RejectCollector:
    """Accumulates rows that failed parsing/validation (never silently dropped)."""

    items: list[dict[str, Any]] = field(default_factory=list)
    total_rows: int = 0

    @property
    def count(self) -> int:
        return len(self.items)

    @property
    def rate(self) -> float:
        return self.count / self.total_rows if self.total_rows else 0.0

    def add(self, ticker: str, reason: str, row: dict[str, Any], *, line: int) -> None:
        self.items.append({"ticker": ticker, "reason": reason, "line": line, "row": row})

    def flush(self, path: Path) -> None:
        if not self.items:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fh:
            for item in self.items:
                fh.write(json.dumps(item, default=str) + "\n")


def validate_header(fieldnames: list[str] | None) -> None:
    if list(fieldnames or []) != EXPECTED_HEADER:
        raise CsvHeaderError(f"unexpected CSV header: {fieldnames!r} (expected {EXPECTED_HEADER})")


def row_to_record(row: dict[str, str], timeframe: str) -> dict[str, Any]:
    raw_ticker = row["ticker"]
    symbol = raw_ticker.strip().upper()
    instrument_key = f"equity:{symbol}"
    if not _EQUITY_KEY.match(instrument_key):
        raise InvalidTickerError(symbol)
    ts = datetime.fromtimestamp(int(row["window_start"]) / 1_000_000_000, tz=UTC)
    txns = row.get("transactions")
    return {
        "event_id": compute_bar_event_id(PROVIDER, FEED, instrument_key, ts, timeframe),
        "provider": PROVIDER,
        "feed": FEED,
        "instrument_type": INSTRUMENT_TYPE,
        "instrument_key": instrument_key,
        "symbol": symbol,
        "raw_ticker": raw_ticker,
        "ts_event": ts,
        "source": SOURCE,
        "schema_version": SCHEMA_VERSION,
        "timeframe": timeframe,
        "bar_start_ts": ts,
        "open": float(row["open"]),
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": float(row["close"]),
        "volume": float(row["volume"]),
        "trade_count": int(txns) if txns not in (None, "") else None,
        "vwap": None,
    }


def iter_record_chunks(
    rows: Iterable[dict[str, str]], timeframe: str, rejects: RejectCollector, chunk_rows: int = 200_000
) -> Iterator[list[dict[str, Any]]]:
    """Yield lists of <= chunk_rows records; malformed rows go to `rejects`, never aborting the chunk."""
    chunk: list[dict[str, Any]] = []
    for line, row in enumerate(rows, start=2):  # line 1 is the header
        rejects.total_rows += 1
        try:
            chunk.append(row_to_record(row, timeframe))
        except InvalidTickerError:
            rejects.add(row.get("ticker", ""), "instrument_key_regex_failed", row, line=line)
        except (KeyError, ValueError, TypeError) as e:
            rejects.add(row.get("ticker", ""), f"parse_error:{e}", row, line=line)
        if len(chunk) >= chunk_rows:
            yield chunk
            chunk = []
    if chunk:
        yield chunk
