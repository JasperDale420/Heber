"""One zero-byte parquet file must not empty a whole feed.

Found live while rebuilding Gold: a 0-byte
`feed=darkpool/.../dt=2026-08-07/part-*.parquet`, almost certainly a writer
killed mid-write during one of the Docker outages, made every read of the
darkpool feed return nothing:

    ArrowInvalid: Parquet file size is 0 bytes
    -> heber_reader_read_failed -> Loaded darkpool rows=0

`read_silver` catches ArrowInvalid and returns an empty frame, so months of
healthy partitions became invisible with only a warning. The regeneration that
hit this reported `no_data` and would have written nothing at all.

`_collect_parquet_files` already filters AppleDouble sidecars and `.tmp`
partial writes for exactly this reason; a zero-byte file is the same class of
artefact. The compactor has skipped them since it was written
(`compactor_skip_empty_file`) — the reader simply never learned to.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.reader.core import HeberReader, HeberReadError, _collect_parquet_files


def _partition(base: Path, dt: str) -> Path:
    part = base / "silver" / "feed=darkpool" / "instrument_type=equity" / f"dt={dt}"
    part.mkdir(parents=True, exist_ok=True)
    return part


def _write_rows(part: Path, name: str, rows: int = 3) -> None:
    ts = pd.Timestamp("2026-08-01", tz="UTC")
    table = pa.Table.from_pandas(
        pd.DataFrame(
            {
                "ts_event": [ts] * rows,
                "ts_available": [ts] * rows,
                "instrument_key": ["equity:AAPL"] * rows,
                "underlying": ["AAPL"] * rows,
                "notional": [1.0] * rows,
            }
        )
    )
    pq.write_table(table, str(part / name))


def test_zero_byte_file_is_not_collected(tmp_path: Path) -> None:
    part = _partition(tmp_path, "2026-08-01")
    _write_rows(part, "good.parquet")
    (part / "part-truncated.parquet").write_bytes(b"")

    collected = _collect_parquet_files(str(tmp_path / "silver" / "feed=darkpool"))

    assert len(collected) == 1
    assert collected[0].endswith("good.parquet")


def test_healthy_partitions_still_read_past_a_zero_byte_file(tmp_path: Path) -> None:
    """The live failure: one truncated file, the whole feed reads as empty."""
    _write_rows(_partition(tmp_path, "2026-08-01"), "a.parquet")
    _write_rows(_partition(tmp_path, "2026-08-05"), "b.parquet")
    (_partition(tmp_path, "2026-08-07") / "part-truncated.parquet").write_bytes(b"")

    rows = HeberReader(tmp_path).read_silver("darkpool")

    assert len(rows) == 6, "a single zero-byte file emptied the feed"


def test_zero_byte_file_is_skipped_when_the_walk_is_scoped(tmp_path: Path) -> None:
    """dt-scoped walks take a different code path and need the same filter."""
    _write_rows(_partition(tmp_path, "2026-08-01"), "a.parquet")
    (_partition(tmp_path, "2026-08-01") / "part-truncated.parquet").write_bytes(b"")

    collected = _collect_parquet_files(
        str(tmp_path / "silver" / "feed=darkpool"), dt_range=("2026-08-01", "2026-08-31")
    )

    assert len(collected) == 1


class TestCorruptionIsRaisedNotSwallowed:
    """The deeper defect the zero-byte filter alone does not close.

    `read_silver`, `read_gold` and `read_parquet` all caught
    `(ArrowInvalid, OSError)` and returned an empty frame. Skipping zero-byte
    files fixes the one artefact seen live, but any *other* unreadable file —
    truncated after a few bytes, half-flushed, a bad block — still turns a
    failed read into "there is no data here", which is indistinguishable from
    an absent partition. That is what hid this bug for months: the darkpool
    regeneration reported `no_data` and exited 0.

    A read that failed must say so. Absence still returns empty, because an
    unwritten partition is a legitimate answer, not a failure.
    """

    @staticmethod
    def _corrupt(part: Path, name: str = "part-halfwritten.parquet") -> None:
        """A file with real bytes that is not valid Parquet — the zero-byte
        filter cannot catch this one."""
        (part / name).write_bytes(b"PAR1\x00\x00\x00\x00garbage")

    def test_silver_read_raises_on_a_corrupt_file(self, tmp_path: Path) -> None:
        _write_rows(_partition(tmp_path, "2026-08-01"), "a.parquet")
        self._corrupt(_partition(tmp_path, "2026-08-07"))

        with pytest.raises(HeberReadError) as excinfo:
            HeberReader(tmp_path).read_silver("darkpool")

        assert "darkpool" in str(excinfo.value)

    def test_gold_read_raises_on_a_corrupt_file(self, tmp_path: Path) -> None:
        part = tmp_path / "gold" / "dataset=darkpool_features" / "project=heber" / "version=v1" / "dt=2026-08-01"
        part.mkdir(parents=True)
        _write_rows(part, "a.parquet")
        self._corrupt(part)

        with pytest.raises(HeberReadError):
            HeberReader(tmp_path).read_gold("darkpool_features", project="heber")

    def test_absent_data_still_returns_empty(self, tmp_path: Path) -> None:
        """Nothing written is not a failure — only unreadable bytes are."""
        _write_rows(_partition(tmp_path, "2026-08-01"), "a.parquet")

        rows = HeberReader(tmp_path).read_silver("darkpool", time_range=("2020-01-01", "2020-01-31"))

        assert rows.empty

    def test_a_missing_feed_still_returns_empty(self, tmp_path: Path) -> None:
        rows = HeberReader(tmp_path).read_silver("feed_that_was_never_written")

        assert rows.empty

    def test_the_collector_treats_a_missing_root_as_empty_not_a_failure(self, tmp_path: Path) -> None:
        """`read_silver` short-circuits on a missing feed before it reaches the
        collector, so the collector's own contract needs pinning directly."""
        assert _collect_parquet_files(str(tmp_path / "silver" / "feed=never_written")) == []
