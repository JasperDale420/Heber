"""Behavioral tests for scripts/backup_lakehouse.sh (archive-based bronze backup).

Bronze is millions of tiny immutable .jsonl.gz files; mirroring them file-by-file
onto the exFAT backup drive never completes (per-file create/fsync/rename cost).
The backup bundles each provider/feed/dt day-partition into a single .tar and only
re-archives day-partitions that changed (append-only bronze may still receive
late backfill into an old day). The larger Silver/Gold Parquet layers stay as
rsync mirrors.
"""

import gzip
import os
import subprocess
import tarfile
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "backup_lakehouse.sh"
BRONZE_TAR = Path("data/bronze/provider=uw/feed=flow/dt=2026-01-01.tar")


def _make_src(src: Path) -> None:
    (src / "data").mkdir(parents=True)
    (src / "data" / ".heber-sentinel").write_text("ok\n")
    part = src / "data/bronze/provider=uw/feed=flow/dt=2026-01-01/hour=00"
    part.mkdir(parents=True)
    (part / "events-1.jsonl.gz").write_bytes(gzip.compress(b'{"a":1}\n'))
    sfile = src / "data/silver/feed=flow/dt=2026-01-01/part-0.parquet"
    sfile.parent.mkdir(parents=True)
    sfile.write_bytes(b"PAR1silver")
    gfile = src / "data/gold/dataset=x/part-0.parquet"
    gfile.parent.mkdir(parents=True)
    gfile.write_bytes(b"PAR1gold")


def _run(tmp_path: Path, src: Path, dst: Path) -> subprocess.CompletedProcess:
    env = {
        "HEBER_VOLUME_ROOT": str(src),
        "HEBER_BACKUP_ROOT": str(dst),
        "HEBER_PROJECT_DIR": str(tmp_path),
        "HEBER_BACKUP_SKIP_POSTGRES": "1",
        "PATH": os.environ["PATH"],
    }
    return subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True)


def _log(tmp_path: Path) -> str:
    logs = sorted((tmp_path / "logs").glob("heber-backup_*.log"))
    return logs[-1].read_text() if logs else "(no log)"


def _prep(tmp_path: Path):
    src = tmp_path / "heber"
    dst = tmp_path / "backup" / "heber-backup"
    dst.parent.mkdir(parents=True)  # the "drive" must be mounted (dirname exists)
    _make_src(src)
    return src, dst


def test_backup_bundles_bronze_and_mirrors_silver_gold(tmp_path):
    src, dst = _prep(tmp_path)

    r = _run(tmp_path, src, dst)
    assert r.returncode == 0, _log(tmp_path)

    tar_path = dst / BRONZE_TAR
    assert tar_path.is_file(), _log(tmp_path)
    with tarfile.open(tar_path) as tf:
        names = tf.getnames()
    assert any(n.endswith("hour=00/events-1.jsonl.gz") for n in names), names

    assert (dst / "data/silver/feed=flow/dt=2026-01-01/part-0.parquet").is_file()
    assert (dst / "data/gold/dataset=x/part-0.parquet").is_file()

    assert (dst / ".last-backup-ok").is_file()
    assert (src / "data/ops/backup-last-ok").is_file()


def test_bronze_archive_is_incremental(tmp_path):
    src, dst = _prep(tmp_path)
    tar_path = dst / BRONZE_TAR

    assert _run(tmp_path, src, dst).returncode == 0, _log(tmp_path)
    first = tar_path.stat().st_mtime_ns

    time.sleep(1.1)
    assert _run(tmp_path, src, dst).returncode == 0, _log(tmp_path)
    # an unchanged day-partition must not be re-archived
    assert tar_path.stat().st_mtime_ns == first


def test_backfill_into_old_partition_retriggers_archive(tmp_path):
    src, dst = _prep(tmp_path)
    tar_path = dst / BRONZE_TAR

    assert _run(tmp_path, src, dst).returncode == 0, _log(tmp_path)
    first = tar_path.stat().st_mtime_ns

    time.sleep(1.1)
    backfill = src / "data/bronze/provider=uw/feed=flow/dt=2026-01-01/hour=01/events-2.jsonl.gz"
    backfill.parent.mkdir(parents=True)
    backfill.write_bytes(gzip.compress(b'{"a":2}\n'))

    assert _run(tmp_path, src, dst).returncode == 0, _log(tmp_path)
    assert tar_path.stat().st_mtime_ns > first
    with tarfile.open(tar_path) as tf:
        names = tf.getnames()
    assert any(n.endswith("hour=01/events-2.jsonl.gz") for n in names), names
