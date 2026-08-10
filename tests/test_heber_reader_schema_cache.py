"""The unified-schema fallback must be computed once per file set, not per read.

Silver fragments disagree on string encodings — the compactor writes
``large_string`` where the real-time writer wrote ``string`` — so
``ds.dataset()`` always fails to infer a merged schema and
``_open_dataset_safe`` falls back to reading every fragment's physical schema
individually. That fallback is a fixed cost per read that does not depend on
rows returned: measured at 1-5 minutes per read on the 2,599-fragment
``feed=bars/instrument_type=equity`` with a cold page cache.

market_regime reads that path five times in one run, so it paid the cost five
times and took 2058s against an 1800s timeout. Caching the unified schema per
file set makes it once.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.reader import core as reader_core
from heber.reader.core import HeberReader


# Faithful to the real trigger: writers persist `feed` as a DATA column that
# duplicates the hive partition key, and disagree on its encoding — the
# compactor writes large_string where the real-time writer wrote string. The
# conflict only surfaces once hive partitioning has to reconcile the two, which
# is why a plain ds.dataset() over the same files merges fine.
def _schema(string_type: pa.DataType) -> pa.Schema:
    return pa.schema(
        [
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("feed", string_type),
            ("instrument_type", string_type),
            ("instrument_key", string_type),
            ("label", string_type),
        ]
    )


_PLAIN = _schema(pa.string())
_LARGE = _schema(pa.large_string())


def _row(hours: int) -> dict:
    base = datetime(2026, 3, 2, tzinfo=UTC)
    return {
        "ts_event": base + timedelta(hours=hours),
        "ts_available": base + timedelta(hours=hours + 1),
        "feed": "tags",
        "instrument_type": "equity",
        "instrument_key": "equity:AAPL",
        "label": f"tag-{hours}",
    }


def _write(base: Path, rows: list[dict], *, name: str, schema: pa.Schema) -> Path:
    table = pa.Table.from_pandas(pd.DataFrame(rows), schema=schema)
    dt = pd.to_datetime(rows[0]["ts_event"]).strftime("%Y-%m-%d")
    part = base / "silver" / "feed=tags" / "instrument_type=equity" / f"dt={dt}"
    part.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, str(part / name))
    return part


@pytest.fixture
def conflicted(tmp_path: Path) -> Path:
    """A feed whose two fragments disagree on string encoding."""
    _write(tmp_path, [_row(0)], name="plain.parquet", schema=_PLAIN)
    _write(tmp_path, [_row(24)], name="large.parquet", schema=_LARGE)
    return tmp_path


@pytest.fixture(autouse=True)
def _clear_cache():
    reader_core.clear_unified_schema_cache()
    yield
    reader_core.clear_unified_schema_cache()


def _conflict_count(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record each time the expensive unification fallback is entered."""
    seen: list[str] = []
    real = reader_core.logger.info

    def _spy(event: str, *args, **kwargs):
        if event == "heber_reader_schema_conflict_detected":
            seen.append(event)
        return real(event, *args, **kwargs)

    monkeypatch.setattr(reader_core.logger, "info", _spy)
    return seen


def test_unification_runs_once_for_repeated_reads(conflicted: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Five reads of an unchanged path must pay the fallback once."""
    seen = _conflict_count(monkeypatch)
    reader = HeberReader(conflicted)

    for _ in range(5):
        assert len(reader.read_silver("tags", instrument_type="equity")) == 2

    assert len(seen) == 1, f"fallback ran {len(seen)}x; the cache is not being used"


def test_cache_invalidates_when_a_fragment_appears(conflicted: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A new file must not be served from a stale schema.

    Live writers append to Silver constantly; a cache keyed only on the path
    would keep serving a schema that predates the new fragment's columns.
    """
    seen = _conflict_count(monkeypatch)
    reader = HeberReader(conflicted)

    assert len(reader.read_silver("tags", instrument_type="equity")) == 2
    assert len(seen) == 1

    _write(conflicted, [_row(48)], name="third.parquet", schema=_LARGE)

    assert len(reader.read_silver("tags", instrument_type="equity")) == 3, "new fragment not picked up"
    assert len(seen) == 2, "file set changed but the cached schema was reused"


def test_cache_invalidates_when_a_fragment_is_replaced(conflicted: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Compaction deletes source files and writes a new one — same story."""
    seen = _conflict_count(monkeypatch)
    reader = HeberReader(conflicted)
    assert len(reader.read_silver("tags", instrument_type="equity")) == 2

    part = conflicted / "silver" / "feed=tags" / "instrument_type=equity" / "dt=2026-03-02"
    (part / "plain.parquet").unlink()
    _write(conflicted, [_row(0), _row(24)], name="compacted.parquet", schema=_LARGE)

    assert len(reader.read_silver("tags", instrument_type="equity")) == 3
    assert len(seen) == 2, "compaction changed the file set but the schema was reused"


def test_cached_read_returns_identical_data(conflicted: Path) -> None:
    """A cache hit must not change values, column set, or dtypes."""
    reader = HeberReader(conflicted)
    first = reader.read_silver("tags", instrument_type="equity").sort_values("ts_event").reset_index(drop=True)
    second = reader.read_silver("tags", instrument_type="equity").sort_values("ts_event").reset_index(drop=True)

    pd.testing.assert_frame_equal(first, second)
    assert set(second["label"]) == {"tag-0", "tag-24"}
    assert all(isinstance(v, str) for v in second["label"])


def test_cache_is_bounded_to_one_entry_per_path(conflicted: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Appending fragments must not grow the cache without bound.

    The gold-poller is a long-running daemon reading paths that gain files all
    day; keying on (path, fingerprint) without eviction would leak an entry per
    write.
    """
    reader = HeberReader(conflicted)
    reader.read_silver("tags", instrument_type="equity")
    for i in range(4):
        _write(conflicted, [_row(72 + i * 24)], name=f"extra{i}.parquet", schema=_LARGE)
        reader.read_silver("tags", instrument_type="equity")

    assert reader_core.unified_schema_cache_size() == 1, "one entry per path, replaced on change"
