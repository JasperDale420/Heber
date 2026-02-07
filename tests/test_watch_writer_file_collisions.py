from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from heber.watch import writer as writer_module
from heber.watch.models import WatchOutcome, WatchStatus
from heber.watch.writer import LabelWriter


def _sample_outcome(alert_id: str) -> WatchOutcome:
    now = datetime(2026, 2, 7, 14, 0, tzinfo=UTC)
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


def test_label_writer_flushes_do_not_overwrite_same_second_partition_files(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    frozen_now = datetime(2026, 2, 7, 16, 30, 45, tzinfo=UTC)

    class _FrozenDateTime:
        @classmethod
        def now(cls, tz=None):  # noqa: ANN001
            if tz is None:
                return frozen_now
            return frozen_now.astimezone(tz)

    monkeypatch.setattr(writer_module, "datetime", _FrozenDateTime)

    writer = LabelWriter(output_path=tmp_path)
    writer.write_outcomes([_sample_outcome("a1")])
    writer.write_outcomes([_sample_outcome("a2")])

    partition_path = tmp_path / "dataset=labels_alert_barriers" / "project=watch" / "version=v1" / "dt=2026-02-07"
    files = list(partition_path.glob("*.parquet"))

    assert len(files) >= 2
    total_rows = sum(len(pd.read_parquet(file_path)) for file_path in files)
    assert total_rows == 2
