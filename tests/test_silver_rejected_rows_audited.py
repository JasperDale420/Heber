"""Silver rows rejected by Arrow validation must leave a durable audit record.

``write_silver_parquet`` salvages a partition row-by-row when the batch fails
Arrow type coercion. Rows that fail individually were discarded — the
all-invalid case logged one ERROR line and returned normally, and the partial
case logged the survivors at DEBUG. Either way the caller saw success, dropped
the buffer, and acknowledged the Redis message, so the rejected rows existed
nowhere afterwards.

Bronze still holds the raw envelopes, so this is recoverable in principle — but
only if something records *which* rows were rejected. These tests pin that the
rejected rows land in the DLQ fallback directory.
"""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.writer.utils import write_silver_parquet

pytestmark = pytest.mark.unit


SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("size", pa.int64()),
    ]
)


def _bad_row(event_id: str) -> dict:
    """A row Arrow cannot coerce: ``size`` is int64, this is a nested dict."""
    return {"event_id": event_id, "size": {"nope": "not an int"}}


def _good_row(event_id: str) -> dict:
    return {"event_id": event_id, "size": 42}


def _audit_payloads(fallback_root) -> list[dict]:
    return [json.loads(p.read_text()) for p in sorted(fallback_root.rglob("*.json"))]


def test_all_invalid_silver_rows_are_audited_not_silently_dropped(tmp_path, monkeypatch) -> None:
    from heber.config import settings

    fallback = tmp_path / "dlq_fallback"
    monkeypatch.setattr(settings, "dlq_fallback_dir", fallback)

    rows = [_bad_row("evt-1"), _bad_row("evt-2")]
    write_silver_parquet(rows, SCHEMA, tmp_path / "part.parquet", "feed=bars/dt=2026-01-01", "bars")

    # Nothing writable, so no parquet — but the rows must not simply vanish.
    assert not (tmp_path / "part.parquet").exists()

    payloads = _audit_payloads(fallback)
    audited_ids = {row["event_id"] for p in payloads for row in p["rejected_rows"]}
    assert audited_ids == {"evt-1", "evt-2"}


def test_partially_invalid_silver_rows_are_audited(tmp_path, monkeypatch) -> None:
    """The salvage path must audit what it drops, not just log the survivors."""
    from heber.config import settings

    fallback = tmp_path / "dlq_fallback"
    monkeypatch.setattr(settings, "dlq_fallback_dir", fallback)

    rows = [_good_row("keep-1"), _bad_row("drop-1"), _good_row("keep-2")]
    target = tmp_path / "part.parquet"
    write_silver_parquet(rows, SCHEMA, target, "feed=bars/dt=2026-01-01", "bars")

    # The valid rows were salvaged to Parquet.
    assert target.exists()
    salvaged = pq.read_table(target).to_pylist()
    assert {r["event_id"] for r in salvaged} == {"keep-1", "keep-2"}

    # The rejected row is recorded rather than dropped at DEBUG level.
    payloads = _audit_payloads(fallback)
    audited_ids = {row["event_id"] for p in payloads for row in p["rejected_rows"]}
    assert audited_ids == {"drop-1"}


def test_rejected_rows_do_not_poison_the_caller(tmp_path, monkeypatch) -> None:
    """An unwritable partition must still return, so it cannot block ACK forever.

    Raising here would make a permanently-bad partition a poison pill: it would
    stay buffered, keep the durability predicate false, and halt the consumer.
    """
    from heber.config import settings

    monkeypatch.setattr(settings, "dlq_fallback_dir", tmp_path / "dlq_fallback")

    write_silver_parquet(
        [_bad_row("evt-1")],
        SCHEMA,
        tmp_path / "part.parquet",
        "feed=bars/dt=2026-01-01",
        "bars",
    )  # must not raise


def test_audit_failure_does_not_break_the_flush(tmp_path, monkeypatch) -> None:
    """If the audit write itself fails, the flush still completes."""
    from heber.config import settings
    from heber.writer import utils as writer_utils

    monkeypatch.setattr(settings, "dlq_fallback_dir", tmp_path / "dlq_fallback")

    def boom(*args, **kwargs):
        raise OSError(f"fallback volume unavailable ({len(args)} args)")

    monkeypatch.setattr(writer_utils, "write_dlq_fallback_file", boom)

    write_silver_parquet(
        [_bad_row("evt-1")],
        SCHEMA,
        tmp_path / "part.parquet",
        "feed=bars/dt=2026-01-01",
        "bars",
    )  # must not raise
