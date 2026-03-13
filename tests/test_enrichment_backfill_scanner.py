"""Tests for heber.watch.backfill_scanner — enrichment backfill scanning."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from heber.watch.backfill_scanner import ENRICHABLE_FIELDS, EnrichmentBackfillScanner

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_feature_row(
    alert_id: str = "alert_1",
    underlying: str = "AAPL",
    *,
    complete: bool = False,
    partial_fields: dict | None = None,
) -> dict:
    """Build a minimal feature row dict."""
    row = {
        "alert_id": alert_id,
        "alert_time": datetime(2026, 2, 17, 15, 0, 0, tzinfo=UTC),
        "symbol": underlying,
        "occ_symbol": f"{underlying}260320C00200000",
        "underlying": underlying,
        "strike": 200.0,
        "expiry": date(2026, 3, 20),
        "put_call": "C",
        "days_to_expiry": 31,
        "premium": 5000.0,
        "volume": 100.0,
        "open_interest": 500.0,
        "volume_oi_ratio": 0.2,
        "alert_type": "SWEEP",
        "side": "ask",
        "aggressor": "buyer",
        "spot_price": 195.0,
        "contract_price": 8.5,
        "moneyness": 1.026,
        "log_moneyness": 0.026,
        "hour_of_day": 15,
        "minute_of_hour": 0,
        "day_of_week": 1,
        "minutes_since_open": 330,
        "minutes_to_close": 60,
        "is_bullish": 1,
        "is_bearish": 0,
        "is_sweep": 1,
        "is_block": 0,
        "is_unusual": 0,
        "underlying_30d_return": None,
        "underlying_5d_return": None,
        "underlying_1d_return": None,
        "realized_vol_20d": None,
    }
    # Enrichable fields: null by default
    for f in ENRICHABLE_FIELDS:
        row[f] = None

    if complete:
        row.update(
            delta=0.55,
            gamma=0.03,
            theta=-0.12,
            vega=0.25,
            iv=0.35,
            iv_rank=45.0,
            gex=1200000.0,
            vex=500000.0,
            max_pain_strike=195.0,
            max_pain_distance_pct=0.025,
            market_tide_net_premium=150000.0,
            market_tide_direction="bullish",
        )

    if partial_fields:
        row.update(partial_fields)

    return row


@pytest.fixture()
def tmp_features_dir(tmp_path: Path) -> Path:
    """Create a temp features output directory."""
    return tmp_path / "dataset=meta_label_features" / "project=watch" / "version=v1"


@pytest.fixture()
def mock_extractor() -> AsyncMock:
    """Mock AlertFeatureExtractor with an extract method."""
    extractor = AsyncMock()
    return extractor


@pytest.fixture()
def mock_calendar() -> MagicMock:
    cal = MagicMock()
    cal.is_market_open.return_value = True
    return cal


def _write_partition(features_dir: Path, dt: date, rows: list[dict]) -> Path:
    """Write a test partition parquet file."""
    dt_str = dt.strftime("%Y-%m-%d")
    partition = features_dir / f"dt={dt_str}"
    partition.mkdir(parents=True, exist_ok=True)
    out = partition / "data.parquet"
    pl.DataFrame(rows).write_parquet(out)
    return out


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFindIncompleteRows:
    """Test _find_incomplete_rows static method."""

    def test_finds_rows_with_null_enrichment_fields(self) -> None:
        incomplete_row = _make_feature_row("a1", complete=False)
        complete_row = _make_feature_row("a2", complete=True)
        df = pl.DataFrame([incomplete_row, complete_row])

        result = EnrichmentBackfillScanner._find_incomplete_rows(df)

        assert len(result) == 1
        assert result["alert_id"][0] == "a1"

    def test_all_complete_returns_empty(self) -> None:
        rows = [_make_feature_row(f"a{i}", complete=True) for i in range(5)]
        df = pl.DataFrame(rows)

        result = EnrichmentBackfillScanner._find_incomplete_rows(df)
        assert result.is_empty()

    def test_partial_nulls_detected(self) -> None:
        row = _make_feature_row("a1", complete=True, partial_fields={"gex": None})
        df = pl.DataFrame([row])

        result = EnrichmentBackfillScanner._find_incomplete_rows(df)
        assert len(result) == 1

    def test_empty_df_returns_empty(self) -> None:
        df = pl.DataFrame({"alert_id": [], "delta": []}).cast({"delta": pl.Float64})
        result = EnrichmentBackfillScanner._find_incomplete_rows(df)
        assert result.is_empty()


class TestReadRecentPartitions:
    """Test _read_recent_partitions method."""

    def test_reads_partitions_within_lookback(
        self, tmp_features_dir: Path, mock_extractor: AsyncMock, mock_calendar: MagicMock
    ) -> None:
        today = date.today()
        rows_today = [_make_feature_row("a1")]
        rows_yesterday = [_make_feature_row("a2")]
        _write_partition(tmp_features_dir, today, rows_today)
        _write_partition(tmp_features_dir, today - timedelta(days=1), rows_yesterday)

        scanner = EnrichmentBackfillScanner(
            feature_extractor=mock_extractor,
            features_output_path=tmp_features_dir,
            lookback_days=3,
            calendar=mock_calendar,
        )

        df = scanner._read_recent_partitions()
        assert df is not None
        assert len(df) == 2

    def test_ignores_old_partitions(
        self, tmp_features_dir: Path, mock_extractor: AsyncMock, mock_calendar: MagicMock
    ) -> None:
        old_date = date.today() - timedelta(days=10)
        _write_partition(tmp_features_dir, old_date, [_make_feature_row("a1")])

        scanner = EnrichmentBackfillScanner(
            feature_extractor=mock_extractor,
            features_output_path=tmp_features_dir,
            lookback_days=3,
            calendar=mock_calendar,
        )

        df = scanner._read_recent_partitions()
        assert df is None

    def test_no_partitions_returns_none(
        self, tmp_features_dir: Path, mock_extractor: AsyncMock, mock_calendar: MagicMock
    ) -> None:
        scanner = EnrichmentBackfillScanner(
            feature_extractor=mock_extractor,
            features_output_path=tmp_features_dir,
            lookback_days=3,
            calendar=mock_calendar,
        )

        df = scanner._read_recent_partitions()
        assert df is None

    def test_skips_partition_when_polars_panics(
        self, tmp_features_dir: Path, mock_extractor: AsyncMock, mock_calendar: MagicMock
    ) -> None:
        today = date.today()
        yesterday = today - timedelta(days=1)
        _write_partition(tmp_features_dir, today, [_make_feature_row("today_row")])
        _write_partition(tmp_features_dir, yesterday, [_make_feature_row("yesterday_row")])

        scanner = EnrichmentBackfillScanner(
            feature_extractor=mock_extractor,
            features_output_path=tmp_features_dir,
            lookback_days=3,
            calendar=mock_calendar,
        )

        class PanicException(BaseException):
            pass

        real_read_parquet = pl.read_parquet
        panic_path_suffix = str((tmp_features_dir / f"dt={today}" / "data.parquet").resolve())

        def _read_parquet_with_panic(path: Path) -> pl.DataFrame:
            if str(Path(path).resolve()) == panic_path_suffix:
                raise PanicException("simulated rust panic")
            return real_read_parquet(path)

        with patch("heber.watch.backfill_scanner.pl.read_parquet", side_effect=_read_parquet_with_panic):
            df = scanner._read_recent_partitions()

        assert df is not None
        assert len(df) == 1
        assert df.get_column("alert_id").to_list() == ["yesterday_row"]


class TestScanAndBackfill:
    """Test the full scan_and_backfill flow."""

    @pytest.mark.asyncio()
    async def test_re_enrich_updates_null_fields(
        self, tmp_features_dir: Path, mock_extractor: AsyncMock, mock_calendar: MagicMock
    ) -> None:
        incomplete_row = _make_feature_row("a1", complete=False)
        today = date.today()
        _write_partition(tmp_features_dir, today, [incomplete_row])

        # Mock extractor returns a complete feature set
        enriched = _make_feature_row("a1", complete=True)

        @dataclass
        class FakeFeatures:
            data: dict = field(default_factory=dict)

            def to_dict(self):
                return self.data

        mock_extractor.extract.return_value = FakeFeatures(data=enriched)

        scanner = EnrichmentBackfillScanner(
            feature_extractor=mock_extractor,
            features_output_path=tmp_features_dir,
            lookback_days=3,
            batch_size=50,
            calendar=mock_calendar,
        )

        with patch("heber.watch.backfill_scanner.EnrichmentBackfillScanner._patch_partition"):
            summary = await scanner.scan_and_backfill()

        assert summary["scanned"] == 1
        assert summary["patched"] == 1
        assert summary["failed"] == 0
        assert mock_extractor.extract.await_count == 1

    @pytest.mark.asyncio()
    async def test_already_complete_rows_skipped(
        self, tmp_features_dir: Path, mock_extractor: AsyncMock, mock_calendar: MagicMock
    ) -> None:
        complete_row = _make_feature_row("a1", complete=True)
        today = date.today()
        _write_partition(tmp_features_dir, today, [complete_row])

        scanner = EnrichmentBackfillScanner(
            feature_extractor=mock_extractor,
            features_output_path=tmp_features_dir,
            lookback_days=3,
            calendar=mock_calendar,
        )

        summary = await scanner.scan_and_backfill()

        assert summary["scanned"] == 1
        assert summary["patched"] == 0
        mock_extractor.extract.assert_not_awaited()

    @pytest.mark.asyncio()
    async def test_failed_re_enrichment_counted_not_written(
        self, tmp_features_dir: Path, mock_extractor: AsyncMock, mock_calendar: MagicMock
    ) -> None:
        incomplete_row = _make_feature_row("a1", complete=False)
        today = date.today()
        _write_partition(tmp_features_dir, today, [incomplete_row])

        mock_extractor.extract.side_effect = RuntimeError("Gateway down")

        scanner = EnrichmentBackfillScanner(
            feature_extractor=mock_extractor,
            features_output_path=tmp_features_dir,
            lookback_days=3,
            calendar=mock_calendar,
        )

        summary = await scanner.scan_and_backfill()

        assert summary["scanned"] == 1
        assert summary["patched"] == 0
        assert summary["failed"] == 1

    @pytest.mark.asyncio()
    async def test_batch_size_limits_processing(
        self, tmp_features_dir: Path, mock_extractor: AsyncMock, mock_calendar: MagicMock
    ) -> None:
        rows = [_make_feature_row(f"a{i}", complete=False) for i in range(10)]
        today = date.today()
        _write_partition(tmp_features_dir, today, rows)

        @dataclass
        class FakeFeatures:
            data: dict = field(default_factory=dict)

            def to_dict(self):
                return self.data

        mock_extractor.extract.return_value = FakeFeatures(data=_make_feature_row("ax", complete=True))

        scanner = EnrichmentBackfillScanner(
            feature_extractor=mock_extractor,
            features_output_path=tmp_features_dir,
            lookback_days=3,
            batch_size=3,
            calendar=mock_calendar,
        )

        with patch("heber.watch.backfill_scanner.EnrichmentBackfillScanner._patch_partition"):
            summary = await scanner.scan_and_backfill()

        assert summary["patched"] == 3
        assert mock_extractor.extract.await_count == 3


class TestScanSkipsOutsideMarketHours:
    """Test that the run loop skips scanning when market is closed."""

    @pytest.mark.asyncio()
    async def test_scan_skipped_outside_market_hours(self, tmp_features_dir: Path, mock_extractor: AsyncMock) -> None:
        cal = MagicMock()
        cal.is_market_open.return_value = False

        scanner = EnrichmentBackfillScanner(
            feature_extractor=mock_extractor,
            features_output_path=tmp_features_dir,
            interval_seconds=0,
            lookback_days=3,
            calendar=cal,
        )

        # Run for a brief moment then stop
        async def stop_after_brief_delay() -> None:
            await asyncio.sleep(0.05)
            scanner.stop()

        with patch.object(scanner, "scan_and_backfill", new_callable=AsyncMock) as mock_scan:
            await asyncio.gather(
                scanner.run(),
                stop_after_brief_delay(),
            )

        mock_scan.assert_not_awaited()
