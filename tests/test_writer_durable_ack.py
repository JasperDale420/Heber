"""``_flush_layers`` must report whether the buffers are actually empty.

The consumer acknowledges a Redis batch when ``_flush_layers()`` returns True,
and the docstring claimed that meant the events were persisted. It did not: it
meant "no exception was raised". ``flush_if_needed()`` only writes partitions
past a size or elapsed-time threshold, so on any iteration where nothing was due
it wrote nothing, returned normally, and the batch was acknowledged with the
events still in RAM. A container kill then lost them, and an acknowledged
message is never redelivered.

The predicate that matters is "did every buffered event get written", which is
exactly "are the buffers now empty". Published files and their directory entries
are fsynced before the consumer can acknowledge the batch.
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from heber.models.envelope import EventEnvelope
from heber.writer.bronze import BronzeWriter
from heber.writer.consumer import EventConsumer
from heber.writer.durability import create_durable_directory
from heber.writer.silver import SilverWriter
from heber.writer.utils import durable_replace, write_batch_commit_marker

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


def test_durable_replace_fsyncs_file_and_parent_directory(tmp_path, monkeypatch) -> None:
    tmp_file = tmp_path / "event.tmp"
    target = tmp_path / "event.json"
    tmp_file.write_text("durable", encoding="utf-8")
    fsync_calls: list[int] = []
    monkeypatch.setattr("heber.writer.utils.os.fsync", fsync_calls.append)

    durable_replace(tmp_file, target)

    assert target.read_text(encoding="utf-8") == "durable"
    assert len(fsync_calls) == 2


def test_create_durable_directory_fsyncs_each_new_parent_link(tmp_path, monkeypatch) -> None:
    root = tmp_path / "data"
    root.mkdir()
    fsynced: list[Path] = []
    monkeypatch.setattr("heber.writer.durability._fsync_directory", fsynced.append)

    target = root / "silver" / "feed=bars" / "dt=2026-07-29"
    create_durable_directory(target, root=root)

    assert target.is_dir()
    assert fsynced == [root, root / "silver", root / "silver" / "feed=bars"]


def test_create_durable_directory_rejects_path_outside_root(tmp_path) -> None:
    root = tmp_path / "data"
    root.mkdir()

    with pytest.raises(ValueError, match="outside durable root"):
        create_durable_directory(tmp_path / "other" / "partition", root=root)


def test_commit_marker_persists_first_write_directory_chain(tmp_path, monkeypatch) -> None:
    data_root = tmp_path / "data"
    data_root.mkdir()
    fsynced: list[Path] = []
    monkeypatch.setattr("heber.writer.durability._fsync_directory", fsynced.append)
    monkeypatch.setattr("heber.writer.utils._fsync_directory", fsynced.append)

    marker = write_batch_commit_marker(
        data_root,
        stream="HEBER_LIVE",
        group="heber-live",
        consumer="writer-1",
        message_ids=["1"],
    )

    assert marker.exists()
    assert fsynced == [data_root, data_root / "_ingest_commits", marker.parent]
