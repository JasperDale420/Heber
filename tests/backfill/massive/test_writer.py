"""Tests for the trade-date ts_available MassiveBackfillWriter."""

from datetime import UTC, datetime

from heber.backfill.massive.writer import (
    MassiveBackfillWriter,
    massive_writer_factory,
    publish_ts_for,
)


def test_publish_ts_next_trading_session_weekday():
    # Tuesday 2024-01-02 -> Wednesday 2024-01-03 16:00 UTC
    assert publish_ts_for("2024-01-02") == datetime(2024, 1, 3, 16, 0, tzinfo=UTC)


def test_publish_ts_friday_rolls_to_monday():
    # Friday 2024-01-05 -> Monday 2024-01-08 16:00 UTC (skips weekend)
    assert publish_ts_for("2024-01-05") == datetime(2024, 1, 8, 16, 0, tzinfo=UTC)


def test_set_ts_available_shared_across_records_for_same_trade_date():
    w = massive_writer_factory()
    assert isinstance(w, MassiveBackfillWriter)

    commit = datetime(2024, 1, 3, 9, 30, tzinfo=UTC)
    r1 = {"trade_date": "2024-01-02", "bar_start_ts": datetime(2024, 1, 2, 14, 30, tzinfo=UTC)}
    r2 = {"trade_date": "2024-01-02", "bar_start_ts": datetime(2024, 1, 2, 20, 55, tzinfo=UTC)}

    out1 = w.set_ts_available(r1, commit)
    out2 = w.set_ts_available(r2, commit)

    assert out1["ts_available"] == out2["ts_available"]
    assert out1["ts_available"] == datetime(2024, 1, 3, 16, 0, tzinfo=UTC)


def test_set_ts_available_falls_back_to_bar_timestamp():
    w = MassiveBackfillWriter()
    commit = datetime(2024, 1, 3, 9, 30, tzinfo=UTC)
    r = {"bar_start_ts": datetime(2024, 1, 2, 14, 30, tzinfo=UTC)}  # no trade_date
    out = w.set_ts_available(r, commit)
    assert out["ts_available"] == datetime(2024, 1, 3, 16, 0, tzinfo=UTC)
