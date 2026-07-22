"""Shard-safety invariants for running multiple consumer containers on one group.

To clear the chronic live-stream backlog, several ``heber-consumer`` containers
can run against the same Redis consumer group (``heber-writers`` on
``heber:events``). Redis delivers each message to exactly one consumer, so that
split is safe by design — but two prerequisites must hold first:

1. Each consumer needs a *unique* group-consumer name. The old
   second-resolution name collides when two containers start in the same second.
2. Bronze/Silver writers must never generate the same output filename from two
   processes. Both stage to a ``.tmp`` sibling and rename; identical names let
   concurrent shards clobber each other's partial write (corruption on a live
   partition). The repo already uses a per-writer uuid suffix in its other
   concurrent write paths (watch/gold) — the realtime writers must match.
"""

from __future__ import annotations

from datetime import UTC, datetime

from heber.config import settings
from heber.writer.bronze import BronzeWriter
from heber.writer.consumer import EventConsumer
from heber.writer.silver import SilverWriter


class _FrozenDatetime:
    """``datetime`` stand-in whose ``now()`` is fixed, forcing identical timestamps."""

    _fixed = datetime(2026, 7, 21, 20, 0, 0, 123456, tzinfo=UTC)

    @classmethod
    def now(cls, tz=None):  # noqa: ANN001, ANN206
        return cls._fixed


def test_bronze_filenames_unique_across_writers_same_timestamp(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("heber.writer.bronze.datetime", _FrozenDatetime)
    monkeypatch.setattr(settings, "data_root", tmp_path)

    pk = "provider=uw/feed=flow_alerts/dt=2026-07-21/hour=20"
    path_a = BronzeWriter()._get_file_path(pk)
    path_b = BronzeWriter()._get_file_path(pk)

    assert path_a != path_b, "two Bronze shards produced the same filename — clobber risk"


def test_silver_filenames_unique_across_writers_same_timestamp(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("heber.writer.silver.datetime", _FrozenDatetime)
    monkeypatch.setattr(settings, "data_root", tmp_path)

    pk = "feed=bars/instrument_type=equity/dt=2026-07-21"
    path_a = SilverWriter()._get_file_path(pk)
    path_b = SilverWriter()._get_file_path(pk)

    assert path_a != path_b, "two Silver shards produced the same filename — clobber risk"


def test_consumer_names_unique_within_same_second(monkeypatch) -> None:
    monkeypatch.setattr("heber.writer.consumer.datetime", _FrozenDatetime)

    name_a = EventConsumer().consumer_name
    name_b = EventConsumer().consumer_name

    assert name_a != name_b, "two shards starting the same second share a group-consumer identity"
