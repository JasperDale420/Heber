from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from heber.watch.models import WatchOutcome, WatchStatus
from heber.watch.writer import LabelWriter


def _sample_outcome(alert_id: str, alert_time: datetime | None = None) -> WatchOutcome:
    now = alert_time or datetime(2026, 2, 7, 14, 0, tzinfo=UTC)
    return WatchOutcome(
        watch_id=f"w-{alert_id}",
        alert_id=alert_id,
        occ_symbol="AAPL260220C00100000",
        underlying="AAPL",
        put_call="C",
        horizon="intraday",
        status=WatchStatus.HIT_TP,
        outcome_time=now,
        outcome_return=0.25,
        bars_to_hit=3,
        mfe=0.3,
        mae=-0.1,
        mfe_adj=0.28,
        mae_adj=-0.12,
        hit_tp_first=1,
        entry_price=2.5,
        spot_at_alert=150.0,
        alert_time=now,
        window_duration_hours=4.0,
        trading_minutes_to_hit=45,
    )


def test_label_writer_flushes_do_not_overwrite_same_second_partition_files(tmp_path) -> None:
    """Two different watches, even flushed back to back, land as two rows —
    filenames are keyed by watch_id, not by a timestamp that could collide.
    """
    writer = LabelWriter(output_path=tmp_path)
    writer.write_outcomes([_sample_outcome("a1")])
    writer.write_outcomes([_sample_outcome("a2")])

    partition_path = tmp_path / "dataset=labels_alert_barriers" / "project=watch" / "version=v1" / "dt=2026-02-07"
    files = list(partition_path.glob("*.parquet"))

    assert len(files) == 2
    total_rows = sum(len(pd.read_parquet(file_path)) for file_path in files)
    assert total_rows == 2


def test_label_writer_retry_of_same_watch_id_overwrites_not_duplicates(tmp_path) -> None:
    """A watch_id written twice — as if retried after a crash between a
    successful write and clearing its pending state — must overwrite its
    own file rather than create a second one. This is what makes it safe
    for a caller to retry a pending outcome without knowing whether it
    already landed.
    """
    writer = LabelWriter(output_path=tmp_path)
    outcome = _sample_outcome("a1")

    writer.write_outcomes([outcome])
    writer.write_outcomes([outcome])

    partition_path = tmp_path / "dataset=labels_alert_barriers" / "project=watch" / "version=v1" / "dt=2026-02-07"
    files = list(partition_path.glob("*.parquet"))

    assert len(files) == 1
    assert len(pd.read_parquet(files[0])) == 1


def test_label_writer_rejects_unsafe_watch_id(tmp_path) -> None:
    writer = LabelWriter(output_path=tmp_path)
    outcome = _sample_outcome("a1")
    outcome.watch_id = "../../etc/passwd"

    with pytest.raises(ValueError, match="Unsafe watch_id"):
        writer.write_outcomes([outcome])


def test_label_writer_failure_does_not_delete_a_prior_successful_row(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    """A retried watch_id that already has a durable file must survive even
    if a *different*, unrelated row in the same flush fails afterward —
    rolling back this flush's own new contribution must not delete data
    that was already committed before this flush started.
    """
    writer = LabelWriter(output_path=tmp_path)
    already_written = _sample_outcome("a1")
    writer.write_outcomes([already_written])

    partition_path = tmp_path / "dataset=labels_alert_barriers" / "project=watch" / "version=v1" / "dt=2026-02-07"
    assert len(list(partition_path.glob("*.parquet"))) == 1

    # Simulate a retry batch that re-includes the already-written outcome
    # (as if it were still pending) alongside a brand-new one, where the
    # brand-new row's promote fails.
    new_outcome = _sample_outcome("a2")

    original_replace = Path.replace
    replace_calls = {"value": 0}

    def _flaky_replace(self, target):  # noqa: ANN001, ANN002
        replace_calls["value"] += 1
        if replace_calls["value"] == 2:
            raise OSError("simulated promote failure")
        return original_replace(self, target)

    monkeypatch.setattr(Path, "replace", _flaky_replace)

    with pytest.raises(OSError, match="simulated promote failure"):
        writer.write_outcomes([already_written, new_outcome])

    files = {f.name: f for f in partition_path.glob("*.parquet")}
    assert "outcome-w-a1.parquet" in files, "prior successful row must not be deleted by an unrelated failure"
    assert "outcome-w-a2.parquet" not in files, "the row that actually failed must not be committed"


def test_label_writer_flush_is_atomic_across_partitions_on_failure(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    writer = LabelWriter(output_path=tmp_path)
    first_time = datetime(2026, 2, 7, 14, 0, tzinfo=UTC)
    second_time = first_time + timedelta(days=1)
    outcomes = [
        _sample_outcome("a1", alert_time=first_time),
        _sample_outcome("a2", alert_time=second_time),
    ]

    original_to_parquet = pd.DataFrame.to_parquet
    call_count = {"value": 0}

    def _flaky_to_parquet(self, path, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        call_count["value"] += 1
        if call_count["value"] == 2:
            raise OSError("simulated write failure")
        return original_to_parquet(self, path, *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "to_parquet", _flaky_to_parquet)

    with pytest.raises(OSError, match="simulated write failure"):
        writer.write_outcomes(outcomes)

    assert len(writer._buffer) == 2
    committed = list((tmp_path / "dataset=labels_alert_barriers").rglob("*.parquet"))
    temp_files = list((tmp_path / "dataset=labels_alert_barriers").rglob("*.tmp"))
    assert committed == []
    assert temp_files == []


def test_label_writer_promote_failure_rolls_back_committed_partitions(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    writer = LabelWriter(output_path=tmp_path)
    first_time = datetime(2026, 2, 7, 14, 0, tzinfo=UTC)
    second_time = first_time + timedelta(days=1)
    outcomes = [
        _sample_outcome("a1", alert_time=first_time),
        _sample_outcome("a2", alert_time=second_time),
    ]

    original_replace = Path.replace
    replace_calls = {"value": 0}

    def _flaky_replace(self, target):  # noqa: ANN001, ANN002
        replace_calls["value"] += 1
        if replace_calls["value"] == 2:
            raise OSError("simulated promote failure")
        return original_replace(self, target)

    monkeypatch.setattr(Path, "replace", _flaky_replace)

    with pytest.raises(OSError, match="simulated promote failure"):
        writer.write_outcomes(outcomes)

    assert len(writer._buffer) == 2
    committed = list((tmp_path / "dataset=labels_alert_barriers").rglob("*.parquet"))
    temp_files = list((tmp_path / "dataset=labels_alert_barriers").rglob("*.tmp"))
    assert committed == []
    assert temp_files == []
