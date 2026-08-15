"""Tests for the shared durable-publish helper.

The lakehouse volume is exFAT with no journaling, so a rename can be recorded
before the bytes it points at. A file published that way is zero bytes, which
fails the pyarrow scan for its whole dataset — one such file made all 194,630
rows of ``labels_alert_barriers`` unreadable.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.utils import durable_write
from heber.utils.durable_write import publish_atomically


@pytest.mark.unit
class TestPublishAtomically:
    def test_publishes_the_written_file(self, tmp_path: Path) -> None:
        out = tmp_path / "data.parquet"

        publish_atomically(lambda staged: pq.write_table(pa.table({"n": [1, 2]}), staged), out)

        assert out.exists()
        assert out.stat().st_size > 0
        assert pq.ParquetFile(out).read().column("n").to_pylist() == [1, 2]

    def test_a_failed_write_publishes_nothing(self, tmp_path: Path) -> None:
        out = tmp_path / "data.parquet"

        def explode(staged: Path) -> None:
            staged.write_bytes(b"partial")
            raise OSError("disk full")

        with pytest.raises(OSError, match="disk full"):
            publish_atomically(explode, out)

        assert not out.exists(), "a partial write must never appear under the destination name"
        assert list(tmp_path.iterdir()) == [], "the staging file must be cleaned up"

    def test_an_existing_file_survives_a_failed_replacement(self, tmp_path: Path) -> None:
        out = tmp_path / "data.parquet"
        pq.write_table(pa.table({"n": [1]}), out)

        def explode(staged: Path) -> None:
            raise OSError("write failed")

        with pytest.raises(OSError):
            publish_atomically(explode, out)

        assert pq.ParquetFile(out).read().column("n").to_pylist() == [1]

    def test_the_directory_is_flushed_after_the_rename(self, tmp_path: Path, monkeypatch) -> None:
        """Without this the rename itself can be lost, taking the file with it."""
        synced: list[Path] = []
        monkeypatch.setattr(durable_write, "fsync_dir", lambda d: synced.append(d))
        out = tmp_path / "sub" / "data.parquet"
        out.parent.mkdir()

        publish_atomically(lambda staged: pq.write_table(pa.table({"n": [1]}), staged), out)

        assert synced == [out.parent]
