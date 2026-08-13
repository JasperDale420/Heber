"""The compactor must make forward progress on both shapes of partition it meets.

Two distinct stalls, both measured on the live lakehouse:

1. Files individually larger than the batch byte budget. The first file is
   appended unconditionally, so a single 80-139 MB file fills the batch alone,
   `len(small_files) <= 1` discards it, and nothing on disk changes — so the
   next scan repeats it. 297 `massive_taq_*` partitions were capped this way
   every day for a week with no progress.

2. Partitions with hundreds of tiny files. `MAX_FILES_PER_BATCH = 50` caps a
   batch by count regardless of size, and the loop runs hourly, so a day that
   lands 274 x 12 KB files drains at 50/hour while the writer keeps adding.
   `dt=2026-08-07` still held 273 uncompacted parts five days on, which is why
   reads of recent data are slow.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from heber.writer import compactor as compactor_module
from heber.writer.compactor import MAX_BATCH_COMPRESSED_BYTES, Compactor


def _write(part: Path, name: str, *, rows: int = 1, pad: int = 0) -> Path:
    part.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(
        {
            "event_id": [f"{name}-{i}" for i in range(rows)],
            "value": [1.0] * rows,
            # Distinct payloads: identical strings dedupe to almost nothing on disk.
            "blob": [f"{i:08d}" + "".join(chr(65 + (i + j) % 26) for j in range(pad)) for i in range(rows)]
            if pad
            else [""] * rows,
        }
    )
    path = part / name
    pq.write_table(pa.Table.from_pandas(frame), str(path), compression="none")
    return path


def test_many_tiny_files_compact_in_one_pass(tmp_path: Path) -> None:
    """A day's worth of small files must not drain 50 per hour.

    The byte budget is the memory guard; a file-count cap that ignores size
    just throttles the common case — 274 files of 12 KB is 3.3 MB, two orders
    of magnitude inside the budget.
    """
    part = tmp_path / "dt=2026-08-07"
    for i in range(274):
        _write(part, f"part-{i:04d}.parquet")

    selected = Compactor()._collect_small_files(part)

    assert selected is not None, "nothing selected from a partition of 274 small files"
    files, _ = selected
    assert len(files) == 274, f"only {len(files)} of 274 selected; a full day still needs many passes"


def test_batch_still_bounded_by_bytes(tmp_path: Path) -> None:
    """Raising the file cap must not remove the memory guard."""
    part = tmp_path / "dt=2026-08-07"
    # ~4 MB each, so the 50 MB budget binds well before any file-count limit.
    for i in range(30):
        _write(part, f"part-{i:02d}.parquet", rows=200, pad=20_000)

    selected = Compactor()._collect_small_files(part)

    assert selected is not None
    files, sizes = selected
    assert sum(sizes[f] for f in files) <= MAX_BATCH_COMPRESSED_BYTES
    assert len(files) < 30, "byte budget did not bind"


def test_oversized_files_do_not_stall_the_partition(tmp_path: Path, monkeypatch) -> None:
    """A file bigger than the whole batch budget must not poison selection.

    Previously the first file was added unconditionally, so one oversized file
    filled the batch, the batch collapsed to a single entry, and the partition
    was discarded on every cycle forever. The compactable remainder must still
    be selected.
    """
    monkeypatch.setattr(compactor_module, "MAX_BATCH_COMPRESSED_BYTES", 10_000)
    part = tmp_path / "dt=2024-06-01"
    _write(part, "part-huge.parquet", rows=400, pad=2_000)  # far over the budget
    for i in range(3):
        _write(part, f"part-small-{i}.parquet")

    selected = Compactor()._collect_small_files(part)

    assert selected is not None, "oversized file stalled the whole partition"
    files, _ = selected
    assert all("huge" not in f.name for f in files), "oversized file must be excluded, not batched"
    assert len(files) == 3


def test_partition_of_only_oversized_files_is_skipped_quietly(tmp_path: Path, monkeypatch) -> None:
    """Nothing to do is a legitimate answer — but it must not look like work pending."""
    monkeypatch.setattr(compactor_module, "MAX_BATCH_COMPRESSED_BYTES", 10_000)
    part = tmp_path / "dt=2024-06-02"
    for i in range(2):
        _write(part, f"part-huge-{i}.parquet", rows=400, pad=2_000)

    assert Compactor()._collect_small_files(part) is None
