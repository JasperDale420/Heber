"""BackfillWriter subclass: trade-date publish-time ts_available + injected atomic parquet writer."""

from datetime import UTC, datetime, time
from typing import Any

import exchange_calendars as xcals

from heber.backfill import BackfillWriter
from heber.backfill.massive.silver_writer import make_bars_parquet_writer

PUBLISH_HOUR_UTC = 16  # ~11:00 ET the next morning
_XNYS = xcals.get_calendar("XNYS")


def publish_ts_for(trade_date_iso: str) -> datetime:
    import pandas as pd

    d = datetime.fromisoformat(trade_date_iso).date()
    nxt = _XNYS.next_session(pd.Timestamp(d)).date()  # next trading session (Fri->Mon, skips holidays)
    return datetime.combine(nxt, time(PUBLISH_HOUR_UTC, 0), tzinfo=UTC)


class MassiveBackfillWriter(BackfillWriter):
    def set_ts_available(self, record: dict[str, Any], ts_commit: datetime) -> dict[str, Any]:
        trade_date = record.get("trade_date")
        if trade_date:
            record["ts_available"] = publish_ts_for(str(trade_date))
        else:  # fallback: derive from the bar timestamp's date
            bar = record.get("bar_start_ts") or record.get("ts_event")
            record["ts_available"] = publish_ts_for(bar.date().isoformat())
        return record


def massive_writer_factory(**kwargs) -> MassiveBackfillWriter:
    return MassiveBackfillWriter(parquet_writer=make_bars_parquet_writer(), **kwargs)
