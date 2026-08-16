"""Unit tests for scripts/normalize_gold_expiry — the one-off expiry type migration.

This tool rewrites live Gold partitions in place, so the tests cover recovery
and preservation as much as the coercion itself. No real lakehouse data is
touched; every case builds a throwaway Gold tree under ``tmp_path``.
"""

import importlib.util
import os
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from filelock import FileLock, Timeout

# The tool lives in scripts/ (not an importable package), so load it by path.
_TOOL = Path(__file__).resolve().parents[1] / "scripts" / "normalize_gold_expiry.py"
_spec = importlib.util.spec_from_file_location("normalize_gold_expiry", _TOOL)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
normalize_expiry = _mod.normalize_expiry
resolve_dataset_root = _mod.resolve_dataset_root
BACKUP_SUFFIX = _mod.BACKUP_SUFFIX

pytestmark = pytest.mark.unit


def _version_root(tmp_path: Path) -> Path:
    return tmp_path / "gold" / "dataset=meta_label_features" / "project=watch" / "version=v1"


def _write_partition(root: Path, day: str, expiry_values: list, *, quarantine: bool = False) -> Path:
    if quarantine:
        part = root / "_quarantine" / "all_greeks_null" / f"dt={day}"
    else:
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


def _expiry_type(path: Path) -> pa.DataType:
    return pq.read_schema(path).field("expiry").type


class TestDryRun:
    def test_reports_non_date32_partitions_and_changes_nothing(self, tmp_path: Path) -> None:
        root = _version_root(tmp_path)
        strings = _write_partition(root, "2026-03-01", ["2026-03-20", "2026-04-17"])
        before = strings.read_bytes()

        report = normalize_expiry(root, apply=False)

        assert [r["action"] for r in report] == ["would-rewrite"]
        assert report[0]["from_type"] == "string"
        assert report[0]["rows"] == 2
        assert strings.read_bytes() == before
        assert not list(strings.parent.glob(f"*{BACKUP_SUFFIX}"))

    def test_already_date32_partition_skipped(self, tmp_path: Path) -> None:
        root = _version_root(tmp_path)
        _write_partition(root, "2026-03-01", [date(2026, 3, 20)])

        report = normalize_expiry(root, apply=False)

        assert [r["action"] for r in report] == ["ok"]

    def test_partition_without_expiry_column_skipped(self, tmp_path: Path) -> None:
        root = _version_root(tmp_path)
        part = root / "dt=2026-03-01"
        part.mkdir(parents=True)
        pd.DataFrame({"alert_id": ["a1"]}).to_parquet(part / "data.parquet", index=False)

        report = normalize_expiry(root, apply=False)

        assert [r["action"] for r in report] == ["no-expiry-column"]


class TestApply:
    def test_string_partition_rewritten_to_date32(self, tmp_path: Path) -> None:
        root = _version_root(tmp_path)
        path = _write_partition(root, "2026-03-01", ["2026-03-20", "2026-04-17"])

        report = normalize_expiry(root, apply=True)

        assert [r["action"] for r in report] == ["rewritten"]
        assert _expiry_type(path) == pa.date32()
        assert list(pd.read_parquet(path)["expiry"]) == [date(2026, 3, 20), date(2026, 4, 17)]

    def test_int_partition_rewritten_to_date32(self, tmp_path: Path) -> None:
        root = _version_root(tmp_path)
        path = _write_partition(root, "2026-03-02", [20260320, 20260417])

        normalize_expiry(root, apply=True)

        assert _expiry_type(path) == pa.date32()
        assert list(pd.read_parquet(path)["expiry"]) == [date(2026, 3, 20), date(2026, 4, 17)]

    def test_other_columns_preserved(self, tmp_path: Path) -> None:
        root = _version_root(tmp_path)
        path = _write_partition(root, "2026-03-01", ["2026-03-20"])

        normalize_expiry(root, apply=True)

        df = pd.read_parquet(path)
        assert list(df["alert_id"]) == ["a0"]
        assert list(df["delta"]) == [0.5]

    def test_quarantine_subtree_skipped_by_default(self, tmp_path: Path) -> None:
        """Readers exclude _quarantine, so rewriting it is 10k files of dead work."""
        root = _version_root(tmp_path)
        path = _write_partition(root, "2026-03-01", ["2026-03-20"], quarantine=True)

        report = normalize_expiry(root, apply=True)

        assert report == []
        assert _expiry_type(path) == pa.string()

    def test_quarantine_subtree_included_on_request(self, tmp_path: Path) -> None:
        root = _version_root(tmp_path)
        path = _write_partition(root, "2026-03-01", ["2026-03-20"], quarantine=True)

        normalize_expiry(root, apply=True, include_quarantine=True)

        assert _expiry_type(path) == pa.date32()

    def test_rerun_is_idempotent(self, tmp_path: Path) -> None:
        root = _version_root(tmp_path)
        _write_partition(root, "2026-03-01", ["2026-03-20"])

        normalize_expiry(root, apply=True)
        second = normalize_expiry(root, apply=True)

        assert [r["action"] for r in second] == ["ok"]


class TestSchemaPreservation:
    """Only ``expiry`` may change. Everything else must survive byte-for-byte in type."""

    def _write_typed(self, root: Path, day: str) -> Path:
        part = root / f"dt={day}"
        part.mkdir(parents=True, exist_ok=True)
        table = pa.table(
            {
                "alert_id": pa.array(["a0"], type=pa.large_string()),
                "expiry": pa.array(["2026-03-20"], type=pa.string()),
                "alert_time": pa.array([datetime(2026, 3, 1, 15, 0, tzinfo=UTC)], type=pa.timestamp("ns", tz="UTC")),
                "side": pa.array(["ask"]).dictionary_encode(),
                "market_tide_direction": pa.array([None], type=pa.null()),
            }
        )
        out = part / "data.parquet"
        pq.write_table(table, out)
        return out

    def test_non_expiry_column_types_unchanged(self, tmp_path: Path) -> None:
        root = _version_root(tmp_path)
        path = self._write_typed(root, "2026-03-01")
        before = pq.read_schema(path)

        normalize_expiry(root, apply=True)

        after = pq.read_schema(path)
        assert after.field("expiry").type == pa.date32()
        for name in ("alert_id", "alert_time", "side", "market_tide_direction"):
            assert after.field(name).type == before.field(name).type, name

    def test_non_expiry_values_unchanged(self, tmp_path: Path) -> None:
        root = _version_root(tmp_path)
        path = self._write_typed(root, "2026-03-01")
        before = pq.read_table(path).drop_columns(["expiry"])

        normalize_expiry(root, apply=True)

        assert pq.read_table(path).drop_columns(["expiry"]).equals(before)

    def test_rewritten_file_still_reads_through_pandas(self, tmp_path: Path) -> None:
        root = _version_root(tmp_path)
        path = self._write_typed(root, "2026-03-01")

        normalize_expiry(root, apply=True)

        df = pd.read_parquet(path)
        assert df["expiry"].iloc[0] == date(2026, 3, 20)
        assert df["alert_id"].iloc[0] == "a0"


class TestRecovery:
    def test_backup_kept_alongside_rewritten_file(self, tmp_path: Path) -> None:
        root = _version_root(tmp_path)
        path = _write_partition(root, "2026-03-01", ["2026-03-20"])
        original = path.read_bytes()

        normalize_expiry(root, apply=True)

        backup = path.with_name(path.name + BACKUP_SUFFIX)
        assert backup.exists()
        assert backup.read_bytes() == original

    def test_backup_is_not_picked_up_as_a_parquet_file(self, tmp_path: Path) -> None:
        root = _version_root(tmp_path)
        _write_partition(root, "2026-03-01", ["2026-03-20"])
        normalize_expiry(root, apply=True)

        second = normalize_expiry(root, apply=False)

        assert [r["action"] for r in second] == ["ok"]

    def test_unreadable_file_aborts_the_run(self, tmp_path: Path) -> None:
        root = _version_root(tmp_path)
        _write_partition(root, "2026-03-01", ["2026-03-20"])
        broken = root / "dt=2026-03-02"
        broken.mkdir(parents=True)
        (broken / "data.parquet").write_bytes(b"not parquet")

        with pytest.raises(RuntimeError, match="unreadable"):
            normalize_expiry(root, apply=True)


class TestConcurrency:
    def test_refuses_a_partition_the_live_writer_holds(self, tmp_path: Path) -> None:
        """The writer appends under data.parquet.lock — never rewrite behind it."""
        root = _version_root(tmp_path)
        path = _write_partition(root, "2026-03-01", ["2026-03-20"])
        original = path.read_bytes()

        with FileLock(str(path.with_suffix(".parquet.lock")), timeout=5):
            with pytest.raises(Timeout):
                normalize_expiry(root, apply=True, lock_timeout=0.2)

        assert path.read_bytes() == original


class TestLossyValues:
    def test_unparseable_values_counted_and_nulled(self, tmp_path: Path) -> None:
        root = _version_root(tmp_path)
        path = _write_partition(root, "2026-03-01", ["2026-03-20", "not-a-date"])

        report = normalize_expiry(root, apply=True)

        assert report[0]["unparseable"] == 1
        written = pd.read_parquet(path)["expiry"]
        assert written[0] == date(2026, 3, 20)
        assert pd.isna(written[1])

    def test_fractional_numeric_is_rejected_not_truncated(self, tmp_path: Path) -> None:
        """20260320.5 is not a date — fabricating one during a rewrite is data loss."""
        root = _version_root(tmp_path)
        path = _write_partition(root, "2026-03-01", [20260320.5, 20260417.0])

        report = normalize_expiry(root, apply=True)

        assert report[0]["unparseable"] == 1
        written = pd.read_parquet(path)["expiry"]
        assert pd.isna(written[0])
        assert written[1] == date(2026, 4, 17)

    def test_nulls_are_not_counted_as_unparseable(self, tmp_path: Path) -> None:
        root = _version_root(tmp_path)
        _write_partition(root, "2026-03-01", ["2026-03-20", None])

        report = normalize_expiry(root, apply=False)

        assert report[0]["unparseable"] == 0


class TestGuardrails:
    def test_root_outside_gold_refused(self, tmp_path: Path) -> None:
        outside = tmp_path / "not-gold" / "dataset=meta_label_features" / "project=watch" / "version=v1"
        outside.mkdir(parents=True)

        with pytest.raises(ValueError, match="expected"):
            resolve_dataset_root(outside, _version_root(tmp_path))

    def test_sibling_gold_dataset_refused(self, tmp_path: Path) -> None:
        sibling = tmp_path / "gold" / "dataset=labels_alert_barriers" / "project=watch" / "version=v1"
        sibling.mkdir(parents=True)

        with pytest.raises(ValueError, match="expected"):
            resolve_dataset_root(sibling, _version_root(tmp_path))

    def test_expected_root_accepted(self, tmp_path: Path) -> None:
        root = _version_root(tmp_path)
        root.mkdir(parents=True)

        assert resolve_dataset_root(root, root) == root.resolve()


class TestPromotionFailure:
    def test_existing_backup_is_never_overwritten(self, tmp_path: Path) -> None:
        root = _version_root(tmp_path)
        path = _write_partition(root, "2026-03-01", ["2026-03-20"])
        stale = path.with_name(path.name + BACKUP_SUFFIX)
        stale.write_bytes(b"an earlier migration attempt")

        with pytest.raises(RuntimeError, match="backup"):
            normalize_expiry(root, apply=True)

        assert stale.read_bytes() == b"an earlier migration attempt"

    def test_partition_survives_a_failed_promotion(self, tmp_path: Path, monkeypatch) -> None:
        """A crash mid-promotion must never leave the partition without a readable file."""
        root = _version_root(tmp_path)
        path = _write_partition(root, "2026-03-01", ["2026-03-20"])
        original = path.read_bytes()

        def boom(src, dst):
            raise OSError("simulated crash during promotion")

        monkeypatch.setattr(_mod.os, "replace", boom)

        with pytest.raises(OSError, match="simulated crash"):
            normalize_expiry(root, apply=True)

        assert path.exists()
        assert path.read_bytes() == original

    def test_verification_failure_keeps_the_rewritten_file_for_autopsy(self, tmp_path: Path, monkeypatch) -> None:
        """An unexplained row-count mismatch on the rewrite must not destroy the
        only evidence of what pyarrow actually wrote — same as the established
        publish_parquet_atomically pattern used elsewhere in this codebase."""
        root = _version_root(tmp_path)
        path = _write_partition(root, "2026-03-01", ["2026-03-20"])
        original = path.read_bytes()

        real_read_metadata = _mod.pq.read_metadata

        def lying_read_metadata(temp_path):
            metadata = real_read_metadata(temp_path)
            return SimpleNamespace(num_rows=metadata.num_rows + 1)

        monkeypatch.setattr(_mod.pq, "read_metadata", lying_read_metadata)

        with pytest.raises(RuntimeError, match="failed verification"):
            normalize_expiry(root, apply=True)

        assert path.exists()
        assert path.read_bytes() == original, "original partition was replaced despite a failed verification"
        kept = list(path.parent.glob(".corrupt-*"))
        assert len(kept) == 1, f"verification-failure evidence was not preserved, found {list(path.parent.iterdir())}"
        # The kept file's dotted name keeps it invisible to every *.parquet
        # glob in the codebase regardless of what else is in the name.
        assert kept[0].name.startswith(".corrupt-")

    def test_verification_failure_survives_a_failed_autopsy_rename(self, tmp_path: Path, monkeypatch) -> None:
        """Even if renaming the staged file aside for autopsy itself fails, the
        original bare unlink() must not run — that would delete the only
        evidence for the exact reason the rename was attempted in the first
        place."""
        root = _version_root(tmp_path)
        path = _write_partition(root, "2026-03-01", ["2026-03-20"])
        original = path.read_bytes()

        real_read_metadata = _mod.pq.read_metadata

        def lying_read_metadata(temp_path):
            metadata = real_read_metadata(temp_path)
            return SimpleNamespace(num_rows=metadata.num_rows + 1)

        monkeypatch.setattr(_mod.pq, "read_metadata", lying_read_metadata)

        real_rename = Path.rename

        def failing_rename(self, target):
            # Only fail the specific autopsy rename this test cares about — a
            # temp file being renamed to a .corrupt-* name.
            if Path(target).name.startswith(".corrupt-"):
                raise OSError("simulated volume error during rename")
            return real_rename(self, target)

        monkeypatch.setattr(Path, "rename", failing_rename)

        with pytest.raises(OSError, match="simulated volume error"):
            normalize_expiry(root, apply=True)

        assert path.exists()
        assert path.read_bytes() == original, "original partition was replaced despite a failed verification"
        lock_file = path.with_suffix(".parquet.lock")
        staging = [p for p in path.parent.iterdir() if p.name.startswith(".") and p != lock_file]
        assert len(staging) == 1, f"verification-failure evidence was lost, found {list(path.parent.iterdir())}"

    def test_autopsy_evidence_is_flushed_before_verification_error_propagates(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """A successful autopsy rename must be made durable before the failure
        is raised — otherwise the evidence a crash-recovery reader relies on
        can itself be lost to the exact page-cache window this whole script
        exists to close for the primary write."""
        root = _version_root(tmp_path)
        _write_partition(root, "2026-03-01", ["2026-03-20"])

        real_read_metadata = _mod.pq.read_metadata

        def lying_read_metadata(temp_path):
            metadata = real_read_metadata(temp_path)
            return SimpleNamespace(num_rows=metadata.num_rows + 1)

        monkeypatch.setattr(_mod.pq, "read_metadata", lying_read_metadata)

        events: list[str] = []
        real_fsync = os.fsync
        real_rename = Path.rename

        def tracked_fsync(fd: int) -> None:
            events.append("fsync_dir" if os.path.isdir(f"/dev/fd/{fd}") else "fsync_file")
            real_fsync(fd)

        def tracked_rename(self, target):
            events.append("rename")
            return real_rename(self, target)

        monkeypatch.setattr(os, "fsync", tracked_fsync)
        monkeypatch.setattr(Path, "rename", tracked_rename)

        with pytest.raises(RuntimeError, match="failed verification"):
            normalize_expiry(root, apply=True)

        assert "rename" in events
        assert "fsync_file" in events, f"the renamed autopsy file's bytes were never flushed: {events}"
        assert "fsync_dir" in events, f"the rename's own directory entry was never flushed: {events}"
        rename_idx = events.index("rename")
        file_fsync_idx = events.index("fsync_file")
        dir_fsync_idx = events.index("fsync_dir")
        # A crash between the file fsync and the directory fsync can persist
        # the renamed entry (visible) before its bytes (durable) — same
        # ordering requirement as the primary publish elsewhere in this file.
        assert rename_idx < file_fsync_idx < dir_fsync_idx, (
            f"expected rename, then file fsync, then directory fsync, saw {events}"
        )

    def test_unreadable_rewrite_is_preserved_for_autopsy_not_discarded(self, tmp_path: Path, monkeypatch) -> None:
        """The writer returned cleanly, so a staged file that can't even be
        read back is just as unexplained as a row-count mismatch and must be
        kept for autopsy too, not silently discarded."""
        root = _version_root(tmp_path)
        path = _write_partition(root, "2026-03-01", ["2026-03-20"])
        original = path.read_bytes()

        real_read_schema = _mod.pq.read_schema

        def raising_read_schema(target_path):
            # Only fail the inner _rewrite_expiry readback (against the
            # temp staging file), not normalize_expiry's own outer schema
            # check against the original partition file.
            if ".tmp-" in Path(target_path).name:
                raise OSError("simulated unreadable staged file")
            return real_read_schema(target_path)

        monkeypatch.setattr(_mod.pq, "read_schema", raising_read_schema)

        with pytest.raises(RuntimeError, match="failed verification"):
            normalize_expiry(root, apply=True)

        assert path.exists()
        assert path.read_bytes() == original, "original partition was replaced despite a failed verification"
        kept = list(path.parent.glob(".corrupt-*"))
        assert len(kept) == 1, f"unreadable-rewrite evidence was not preserved, found {list(path.parent.iterdir())}"


class TestStrictStringParsing:
    """A rewrite may not invent a date the source did not contain."""

    @pytest.mark.parametrize("raw", ["2026-03-20junk", "20260320.5", "March 20 2026", "2026-03", "202603201"])
    def test_malformed_strings_are_rejected(self, tmp_path: Path, raw: str) -> None:
        root = _version_root(tmp_path)
        path = _write_partition(root, "2026-03-01", [raw])

        report = normalize_expiry(root, apply=True)

        assert report[0]["unparseable"] == 1
        assert pd.isna(pd.read_parquet(path)["expiry"].iloc[0])

    @pytest.mark.parametrize("raw", ["2026-03-20", "20260320"])
    def test_well_formed_strings_accepted(self, tmp_path: Path, raw: str) -> None:
        root = _version_root(tmp_path)
        path = _write_partition(root, "2026-03-01", [raw])

        report = normalize_expiry(root, apply=True)

        assert report[0]["unparseable"] == 0
        assert pd.read_parquet(path)["expiry"].iloc[0] == date(2026, 3, 20)

    def test_out_of_range_integer_rejected(self, tmp_path: Path) -> None:
        root = _version_root(tmp_path)
        path = _write_partition(root, "2026-03-01", [202603201])

        report = normalize_expiry(root, apply=True)

        assert report[0]["unparseable"] == 1
        assert pd.isna(pd.read_parquet(path)["expiry"].iloc[0])
