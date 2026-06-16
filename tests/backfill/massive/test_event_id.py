"""Tests for deterministic bar event_id."""

from datetime import UTC, datetime

from heber.backfill.massive.event_id import compute_bar_event_id


def _base_args():
    return {
        "provider": "massive",
        "feed": "bars",
        "instrument_key": "equity:AAPL",
        "ts_event": datetime(2024, 1, 2, tzinfo=UTC),
        "timeframe": "1d",
    }


def test_deterministic_same_inputs_same_id():
    args = _base_args()
    id1 = compute_bar_event_id(**args)
    id2 = compute_bar_event_id(**args)
    assert id1 == id2
    assert len(id1) == 32
    assert all(c in "0123456789abcdef" for c in id1)


def test_id_changes_with_instrument_key():
    args = _base_args()
    base = compute_bar_event_id(**args)
    args["instrument_key"] = "equity:MSFT"
    assert compute_bar_event_id(**args) != base


def test_id_changes_with_timeframe():
    args = _base_args()
    base = compute_bar_event_id(**args)
    args["timeframe"] = "1m"
    assert compute_bar_event_id(**args) != base


def test_id_changes_with_ts_event():
    args = _base_args()
    base = compute_bar_event_id(**args)
    args["ts_event"] = datetime(2024, 1, 3, tzinfo=UTC)
    assert compute_bar_event_id(**args) != base
