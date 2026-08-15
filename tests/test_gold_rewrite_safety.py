"""Failure-path tests for the shared Gold partition rewrite protocol.

These files are training data on a non-journaling volume, so the interesting
cases are the ones where something fails *after* the replacement may already be
live. Every such path must end with the original back in place, or say loudly
that it could not put it back.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.ml import gold_rewrite
from heber.ml.gold_rewrite import rewrite_partition


def _write(path: Path, values: list[int]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"n": pa.array(values, type=pa.int64())}), path)
    return path


def _double_it(table: pa.Table) -> pa.Table:
    return pa.table({"n": pa.array([v * 2 for v in table.column("n").to_pylist()], type=pa.int64())})


def _accept_anything(original: pa.Table, candidate: pa.Table) -> list[str]:
    return []


def _reject_everything(original: pa.Table, candidate: pa.Table) -> list[str]:
    return ["injected failure"]


@pytest.fixture
def partition(tmp_path: Path) -> Path:
    return _write(tmp_path / "dt=2026-04-14" / "data.parquet", [1, 2, 3])


@pytest.mark.integration
class TestRewriteSafety:
    def test_successful_rewrite_publishes_the_candidate(self, partition: Path, tmp_path: Path) -> None:
        result = rewrite_partition(partition, plan=_double_it, validate=_accept_anything, backup_dir=tmp_path / "b")

        assert result["status"] == "rewritten"
        assert pq.ParquetFile(partition).read().column("n").to_pylist() == [2, 4, 6]

    def test_plan_returning_none_leaves_the_file_alone(self, partition: Path, tmp_path: Path) -> None:
        before = gold_rewrite.sha256(partition)

        result = rewrite_partition(partition, plan=lambda t: None, validate=_accept_anything, backup_dir=tmp_path / "b")

        assert result["status"] == "skipped"
        assert gold_rewrite.sha256(partition) == before

    def test_a_raising_planner_never_touches_the_file(self, partition: Path, tmp_path: Path) -> None:
        before = gold_rewrite.sha256(partition)

        def explode(table: pa.Table) -> pa.Table:
            raise RuntimeError("planner blew up")

        result = rewrite_partition(partition, plan=explode, validate=_accept_anything, backup_dir=tmp_path / "b")

        assert result["status"] == "plan_failed"
        assert "planner blew up" in result["problems"][0]
        assert gold_rewrite.sha256(partition) == before

    def test_staged_validation_failure_never_publishes(self, partition: Path, tmp_path: Path) -> None:
        before = gold_rewrite.sha256(partition)

        result = rewrite_partition(partition, plan=_double_it, validate=_reject_everything, backup_dir=tmp_path / "b")

        assert result["status"] == "staging_failed"
        assert gold_rewrite.sha256(partition) == before, "a rejected candidate must never reach the live file"

    def test_failure_after_publish_restores_the_original(self, partition: Path, tmp_path: Path, monkeypatch) -> None:
        """The published file is live by now, so this must roll back."""
        before = gold_rewrite.sha256(partition)
        calls = {"n": 0}

        def fail_second_call(original: pa.Table, candidate: pa.Table) -> list[str]:
            calls["n"] += 1
            return [] if calls["n"] == 1 else ["post-publish failure"]

        result = rewrite_partition(partition, plan=_double_it, validate=fail_second_call, backup_dir=tmp_path / "b")

        assert result["status"] == "verify_failed"
        assert "original restored from backup" in result["problems"]
        assert gold_rewrite.sha256(partition) == before
        assert pq.ParquetFile(partition).read().column("n").to_pylist() == [1, 2, 3]

    @pytest.mark.parametrize("failing_step", ["fsync_dir", "replace_then_fail"])
    def test_an_exception_during_publish_restores_the_original(
        self, partition: Path, tmp_path: Path, monkeypatch, failing_step: str
    ) -> None:
        """A raised exception, not just a returned problem, must also roll back.

        Each injection fires once so the rollback path itself still works —
        these primitives are shared with restore().
        """
        before = gold_rewrite.sha256(partition)
        fired = {"n": 0}

        if failing_step == "replace_then_fail":
            # os.replace succeeds and *then* the step fails — the worst case,
            # because the replacement is already the live file.
            real_replace = gold_rewrite.os.replace

            def replace_then_fail(src: object, dst: object) -> None:
                real_replace(src, dst)
                fired["n"] += 1
                if fired["n"] == 1:
                    raise OSError("publish failed")

            monkeypatch.setattr(gold_rewrite.os, "replace", replace_then_fail)
        else:
            real_fsync = gold_rewrite._fsync_dir

            def fsync_once(directory: Path) -> None:
                fired["n"] += 1
                if fired["n"] == 1:
                    raise OSError("fsync failed")
                real_fsync(directory)

            monkeypatch.setattr(gold_rewrite, "_fsync_dir", fsync_once)

        result = rewrite_partition(partition, plan=_double_it, validate=_accept_anything, backup_dir=tmp_path / "b")

        assert result["status"] == "verify_failed"
        assert "original restored from backup" in result["problems"]
        assert gold_rewrite.sha256(partition) == before
        assert pq.ParquetFile(partition).read().column("n").to_pylist() == [1, 2, 3]

    def test_failure_without_a_backup_says_the_bad_file_is_live(self, partition: Path, monkeypatch) -> None:
        """Without a backup there is nothing to restore — say so, do not imply success."""
        calls = {"n": 0}

        def fail_second_call(original: pa.Table, candidate: pa.Table) -> list[str]:
            calls["n"] += 1
            return [] if calls["n"] == 1 else ["post-publish failure"]

        result = rewrite_partition(partition, plan=_double_it, validate=fail_second_call, backup_dir=None)

        assert result["status"] == "verify_failed"
        assert any("NO BACKUP TAKEN" in p for p in result["problems"])

    def test_a_failed_restore_is_reported_rather_than_swallowed(
        self, partition: Path, tmp_path: Path, monkeypatch
    ) -> None:
        calls = {"n": 0}

        def fail_second_call(original: pa.Table, candidate: pa.Table) -> list[str]:
            calls["n"] += 1
            return [] if calls["n"] == 1 else ["post-publish failure"]

        def broken_restore(backup_path: Path, out_file: Path) -> None:
            raise OSError("backup drive vanished")

        monkeypatch.setattr(gold_rewrite, "restore", broken_restore)

        result = rewrite_partition(partition, plan=_double_it, validate=fail_second_call, backup_dir=tmp_path / "b")

        assert result["status"] == "restore_failed"
        assert any("RESTORE FAILED" in p for p in result["problems"])
        assert any("the original is at" in p for p in result["problems"])

    def test_does_not_read_or_write_while_another_writer_holds_the_lock(self, partition: Path) -> None:
        from filelock import FileLock

        before = gold_rewrite.sha256(partition)
        with FileLock(str(partition.with_suffix(".parquet.lock")), timeout=5):
            result = rewrite_partition(
                partition, plan=_double_it, validate=_accept_anything, backup_dir=None, lock_timeout=0.5
            )

        assert result["status"] == "locked"
        assert gold_rewrite.sha256(partition) == before
