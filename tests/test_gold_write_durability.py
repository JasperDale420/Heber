"""Durability regression tests for Gold ``expiry`` write paths on exFAT.

``/Volumes/heber`` is a non-journaling exFAT mount: a rename makes a filename
visible immediately, but the bytes behind it can still be sitting in the page
cache. An unclean shutdown between the rename and the flush can leave a
zero-byte file at that path. These tests pin that both
``heber.ml.datasets._atomic_write_parquet`` (the live watch-consumer write
path) and ``scripts/normalize_gold_expiry.py``'s ``_rewrite_expiry`` (the
expiry migration script) route their promotion through
``heber.writer.utils.publish_file_atomically`` instead of a bare
``os.replace`` — see that module's docstring for the fsync-before-rename
contract this exists to satisfy.
"""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import pandas as pd
import pytest

from heber.ml.datasets import _atomic_write_parquet

pytestmark = pytest.mark.unit

# scripts/ is not an importable package — load the migration tool by path,
# same as tests/test_normalize_gold_expiry.py.
_TOOL = Path(__file__).resolve().parents[1] / "scripts" / "normalize_gold_expiry.py"
_spec = importlib.util.spec_from_file_location("normalize_gold_expiry", _TOOL)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
normalize_expiry = _mod.normalize_expiry


def _version_root(tmp_path: Path) -> Path:
    return tmp_path / "gold" / "dataset=meta_label_features" / "project=watch" / "version=v1"


def _write_partition(root: Path, day: str, expiry_values: list) -> Path:
    part = root / f"dt={day}"
    part.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {
            "alert_id": [f"a{i}" for i in range(len(expiry_values))],
            "expiry": expiry_values,
            "delta": [0.5] * len(expiry_values),
        }
    )
    out = part / "data.parquet"
    df.to_parquet(out, index=False)
    return out


class TestAtomicWriteParquetDurability:
    """``_atomic_write_parquet`` is the live watch-consumer write funnel."""

    def test_empty_write_is_never_published(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """A staged write that ends up empty must fail, not become the live file.

        Reproduces the exFAT risk directly: a temp file that never got its
        bytes flushed reads as zero bytes, exactly like an unclean shutdown
        between the old bare ``os.replace`` and the actual disk flush would.
        """

        def empty_to_parquet(self: pd.DataFrame, path: str | Path, **_kwargs: object) -> None:
            Path(path).write_bytes(b"")

        monkeypatch.setattr(pd.DataFrame, "to_parquet", empty_to_parquet)

        out_file = tmp_path / "data.parquet"
        df = pd.DataFrame({"alert_id": ["a1"], "delta": [0.5]})

        with pytest.raises(OSError):
            _atomic_write_parquet(df, out_file)

        assert not out_file.exists(), "an empty staged file was published as the live partition"
        assert list(tmp_path.iterdir()) == [], "staging file left behind"

    def test_truncated_write_is_never_published(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """A staged file that is non-empty but unreadable must not be published.

        Size alone does not prove a Parquet file is complete, and one
        unreadable fragment fails the whole dataset scan, not just this file.
        """
        real_to_parquet = pd.DataFrame.to_parquet

        def truncating_to_parquet(self: pd.DataFrame, path: str | Path, **kwargs: object) -> None:
            real_to_parquet(self, path, **kwargs)  # type: ignore[arg-type]
            complete = Path(path).read_bytes()
            Path(path).write_bytes(complete[: len(complete) // 2])

        monkeypatch.setattr(pd.DataFrame, "to_parquet", truncating_to_parquet)

        out_file = tmp_path / "data.parquet"
        df = pd.DataFrame({"alert_id": ["a1"], "delta": [0.5]})

        with pytest.raises(OSError):
            _atomic_write_parquet(df, out_file)

        assert not out_file.exists(), "a truncated staged file was published as the live partition"

    def test_bytes_are_flushed_and_directory_entry_durable_around_the_publish(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The staged bytes must be fsynced before the rename, and the rename's
        own directory entry must be flushed afterward — the merge-and-rewrite
        callers of this function displace an existing partition file, so a
        lost rename on this non-journaling volume is not just a replayable gap.
        """
        events: list[str] = []
        real_fsync = os.fsync
        real_replace = os.replace

        def tracked_fsync(fd: int) -> None:
            events.append("fsync_dir" if os.path.isdir(f"/dev/fd/{fd}") else "fsync_file")
            real_fsync(fd)

        def tracked_replace(src: str | Path, dst: str | Path) -> None:
            events.append("replace")
            real_replace(src, dst)

        monkeypatch.setattr(os, "fsync", tracked_fsync)
        monkeypatch.setattr(os, "replace", tracked_replace)

        out_file = tmp_path / "data.parquet"
        df = pd.DataFrame({"alert_id": ["a1"], "delta": [0.5]})
        _atomic_write_parquet(df, out_file)

        replace_idx = events.index("replace")
        assert "fsync_file" in events[:replace_idx], f"expected a data fsync before publish, saw {events}"
        assert "fsync_dir" in events[replace_idx + 1 :], (
            f"the publish's own directory entry was never flushed after the rename: {events}"
        )
        assert out_file.exists()
        assert pd.read_parquet(out_file)["alert_id"].tolist() == ["a1"]

    def test_failed_write_leaves_no_staging_file(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        def failing_to_parquet(self: pd.DataFrame, path: str | Path, **_kwargs: object) -> None:
            Path(path).write_bytes(b"PAR1truncated")
            raise OSError("no space left on device")

        monkeypatch.setattr(pd.DataFrame, "to_parquet", failing_to_parquet)

        out_file = tmp_path / "data.parquet"
        df = pd.DataFrame({"alert_id": ["a1"], "delta": [0.5]})

        with pytest.raises(OSError):
            _atomic_write_parquet(df, out_file)

        assert not out_file.exists()
        assert list(tmp_path.iterdir()) == [], "staging file left behind"


class TestNormalizeGoldExpiryDurability:
    """``_rewrite_expiry`` rewrites live Gold partitions in place."""

    def test_bytes_are_flushed_to_disk_before_the_rewrite_is_published(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        root = _version_root(tmp_path)
        _write_partition(root, "2026-03-01", ["2026-03-20"])

        calls: list[str] = []
        real_fsync = os.fsync
        real_replace = os.replace

        def tracked_fsync(fd: int) -> None:
            calls.append("fsync")
            real_fsync(fd)

        def tracked_replace(src: str | Path, dst: str | Path) -> None:
            calls.append("replace")
            real_replace(src, dst)

        monkeypatch.setattr(os, "fsync", tracked_fsync)
        monkeypatch.setattr(os, "replace", tracked_replace)

        normalize_expiry(root, apply=True)

        assert "replace" in calls
        assert "fsync" in calls[: calls.index("replace")], f"expected a data fsync before publish, saw {calls}"

    def test_backup_is_fully_durable_before_original_is_replaced(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The backup is the only surviving copy of the original once the
        replace below lands. ``shutil.copy2`` does not fsync on its own, so
        both the backup's own bytes and its directory entry must be flushed
        before the replace — otherwise a crash right after the replace can
        leave the replace done and the backup missing or truncated, with no
        way to recover the pre-migration partition. The replace's own
        directory entry must then be flushed too, so the rename itself
        cannot be lost on this non-journaling volume once success is logged.
        """
        root = _version_root(tmp_path)
        _write_partition(root, "2026-03-01", ["2026-03-20"])

        events: list[str] = []
        real_fsync = os.fsync
        real_replace = os.replace

        def tracked_fsync(fd: int) -> None:
            events.append("fsync_dir" if os.path.isdir(f"/dev/fd/{fd}") else "fsync_file")
            real_fsync(fd)

        def tracked_replace(src: str | Path, dst: str | Path) -> None:
            events.append("replace")
            real_replace(src, dst)

        monkeypatch.setattr(os, "fsync", tracked_fsync)
        monkeypatch.setattr(os, "replace", tracked_replace)

        normalize_expiry(root, apply=True)

        replace_idx = events.index("replace")
        before, after = events[:replace_idx], events[replace_idx + 1 :]

        assert "fsync_file" in before, f"the backup's own bytes were never flushed before the replace: {events}"
        assert "fsync_dir" in before, f"the backup's directory entry was never flushed before the replace: {events}"
        assert before.index("fsync_file") < before.index("fsync_dir"), (
            f"backup directory entry flushed before the backup's own bytes were durable: {events}"
        )
        assert "fsync_dir" in after, f"the replace's own directory entry was never flushed: {events}"
