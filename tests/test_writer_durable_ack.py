"""``_flush_layers`` must report whether the buffers are actually empty.

The consumer acknowledges a Redis batch when ``_flush_layers()`` returns True,
and the docstring claimed that meant the events were persisted. It did not: it
meant "no exception was raised". ``flush_if_needed()`` only writes partitions
past a size or elapsed-time threshold, so on any iteration where nothing was due
it wrote nothing, returned normally, and the batch was acknowledged with the
events still in RAM. A container kill then lost them, and an acknowledged
message is never redelivered.

The predicate that matters is "did every buffered event get written", which is
exactly "are the buffers now empty". Note the deliberate limit of that claim:
the writers finish with an atomic rename and no ``fsync``, so this establishes
"survives process death and container kill", not "survives host power loss".
"""

from datetime import UTC, datetime

import pytest

from heber.models.envelope import EventEnvelope
from heber.writer.bronze import BronzeWriter
from heber.writer.consumer import EventConsumer
from heber.writer.silver import SilverWriter

pytestmark = pytest.mark.unit

NOW = datetime(2026, 1, 15, 14, 30, tzinfo=UTC)


def _make_envelope(**overrides) -> EventEnvelope:
    defaults = {
        "event_id": "evt-durable-001",
        "provider": "alpaca",
        "feed": "bars",
        "source": "websocket",
        "instrument_type": "equity",
        "instrument_key": "equity:AAPL",
        "symbol": "AAPL",
        "ts_event": NOW,
        "ts_ingest": NOW,
        "ts_available": NOW,
        "payload": {"t": NOW.isoformat(), "o": 100.0, "h": 101.0, "l": 99.0, "c": 100.5, "v": 1000},
    }
    defaults.update(overrides)
    return EventEnvelope(**defaults)


def _never_flush(monkeypatch) -> None:
    """Raise every flush threshold so ``flush_if_needed`` is a genuine no-op.

    Monkeypatching the real settings rather than mocking ``flush_if_needed``
    keeps the test honest — a mock cannot drift out of sync with the thresholds
    that produce this in production.
    """
    from heber.config import settings

    monkeypatch.setattr(settings, "bronze_max_batch_size", 99_999)
    monkeypatch.setattr(settings, "bronze_flush_interval_seconds", 9_999)
    monkeypatch.setattr(settings, "silver_min_rows_per_flush", 9_999)
    monkeypatch.setattr(settings, "silver_max_rows_per_file", 999_999)
    monkeypatch.setattr(settings, "silver_max_flush_time_seconds", 9_999)


def _always_flush(monkeypatch) -> None:
    from heber.config import settings

    monkeypatch.setattr(settings, "bronze_max_batch_size", 1)
    monkeypatch.setattr(settings, "silver_min_rows_per_flush", 1)


def test_flush_layers_is_false_when_events_are_still_buffered(tmp_path, monkeypatch) -> None:
    from heber.config import settings

    monkeypatch.setattr(settings, "data_root", tmp_path)
    _never_flush(monkeypatch)

    consumer = EventConsumer()
    consumer.bronze_writer.write(_make_envelope())

    assert consumer.bronze_writer.has_buffered() is True
    # No exception was raised, but nothing reached disk — this must not read as
    # "safe to acknowledge".
    assert consumer._flush_layers() is False


def test_flush_layers_is_true_once_the_buffers_drain(tmp_path, monkeypatch) -> None:
    from heber.config import settings

    monkeypatch.setattr(settings, "data_root", tmp_path)
    _always_flush(monkeypatch)

    consumer = EventConsumer()
    consumer.bronze_writer.write(_make_envelope())

    assert consumer._flush_layers() is True
    assert consumer.bronze_writer.has_buffered() is False
    assert list((tmp_path / "bronze").rglob("*.jsonl.gz"))


def test_flush_layers_is_true_when_nothing_was_buffered(tmp_path, monkeypatch) -> None:
    """An idle iteration has nothing to lose, so it may acknowledge."""
    from heber.config import settings

    monkeypatch.setattr(settings, "data_root", tmp_path)
    _never_flush(monkeypatch)

    assert EventConsumer()._flush_layers() is True


def test_has_buffered_ignores_emptied_partition_keys() -> None:
    """A leftover empty list is not buffered data."""
    bronze = BronzeWriter()
    bronze.buffers["provider=t/feed=bars/dt=2026-01-01/hour=00"] = []
    assert bronze.has_buffered() is False

    silver = SilverWriter()
    silver.buffers["feed=bars/instrument_type=equity/dt=2026-01-01"] = []
    assert silver.has_buffered() is False


def test_flush_if_needed_reports_whether_it_wrote(tmp_path, monkeypatch) -> None:
    from heber.config import settings

    monkeypatch.setattr(settings, "data_root", tmp_path)

    _never_flush(monkeypatch)
    bronze = BronzeWriter()
    bronze.write(_make_envelope())
    assert bronze.flush_if_needed() is False

    _always_flush(monkeypatch)
    assert bronze.flush_if_needed() is True
