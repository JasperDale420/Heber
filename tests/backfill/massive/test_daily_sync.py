"""Tests for the unattended daily Massive raw archive sync."""

from __future__ import annotations

from datetime import UTC, date, datetime

from heber.backfill.massive.daily import MassiveDailySync, latest_publishable_trading_day
from heber.backfill.massive.paths import archive_path


class _FakeFlatDownloader:
    def __init__(self, archive_root):
        self.archive_root = str(archive_root)
        self.calls: list[tuple[str, date]] = []

    def sync_date(self, dataset: str, target_date: date) -> int:
        self.calls.append((dataset, target_date))
        path = archive_path(self.archive_root, dataset, target_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"ticker,volume,open,close,high,low,window_start,transactions\n")
        return 1


class _FakeCorporateActions:
    def __init__(self):
        self.calls: list[date] = []

    def sync_date(self, target_date: date):
        self.calls.append(target_date)
        return {"splits": 1, "dividends": 2, "tickers_active": 3}


def test_latest_publishable_trading_day_waits_until_massive_flat_files_are_available():
    before_publish = datetime(2024, 1, 3, 15, 30, tzinfo=UTC)  # 10:30 ET
    after_publish = datetime(2024, 1, 3, 16, 45, tzinfo=UTC)  # 11:45 ET

    assert latest_publishable_trading_day(before_publish) == date(2023, 12, 29)
    assert latest_publishable_trading_day(after_publish) == date(2024, 1, 2)


def test_daily_sync_downloads_all_raw_stock_aggregate_files_and_daily_corporate_actions(tmp_path):
    flat = _FakeFlatDownloader(tmp_path)
    corporate_actions = _FakeCorporateActions()
    sync = MassiveDailySync(
        archive_root=tmp_path,
        flat_file_downloader=flat,
        corporate_actions_downloader=corporate_actions,
    )

    result = sync.run(target_date=date(2024, 1, 2))

    assert result.dates == [date(2024, 1, 2)]
    assert flat.calls == [
        ("day_aggs_v1", date(2024, 1, 2)),
        ("minute_aggs_v1", date(2024, 1, 2)),
    ]
    assert corporate_actions.calls == [date(2024, 1, 2)]
    assert result.raw_files[date(2024, 1, 2)] == {
        "day_aggs_v1": archive_path(str(tmp_path), "day_aggs_v1", date(2024, 1, 2)),
        "minute_aggs_v1": archive_path(str(tmp_path), "minute_aggs_v1", date(2024, 1, 2)),
    }
    assert result.corporate_action_counts[date(2024, 1, 2)] == {
        "splits": 1,
        "dividends": 2,
        "tickers_active": 3,
    }


def test_daily_sync_catches_up_missing_trading_days_after_a_restart(tmp_path):
    for dataset in ("day_aggs_v1", "minute_aggs_v1"):
        existing = archive_path(str(tmp_path), dataset, date(2024, 1, 2))
        existing.parent.mkdir(parents=True, exist_ok=True)
        existing.write_bytes(b"already archived")

    flat = _FakeFlatDownloader(tmp_path)
    corporate_actions = _FakeCorporateActions()
    sync = MassiveDailySync(
        archive_root=tmp_path,
        flat_file_downloader=flat,
        corporate_actions_downloader=corporate_actions,
    )

    result = sync.run(end_date=date(2024, 1, 4))

    assert result.dates == [date(2024, 1, 3), date(2024, 1, 4)]
    assert corporate_actions.calls == [date(2024, 1, 3), date(2024, 1, 4)]
    assert flat.calls == [
        ("day_aggs_v1", date(2024, 1, 3)),
        ("minute_aggs_v1", date(2024, 1, 3)),
        ("day_aggs_v1", date(2024, 1, 4)),
        ("minute_aggs_v1", date(2024, 1, 4)),
    ]
