"""Tests for heber.watch.backfill_scanner — enrichment backfill scanning."""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from heber.watch.backfill_scanner import ENRICHABLE_FIELDS, EnrichmentBackfillScanner

DEFAULT_MAX_AGE = timedelta(minutes=60)

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
        "alert_time": datetime.now(UTC),
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
        "ts_event": datetime.now(UTC),
        "ts_available": datetime.now(UTC),
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
    extractor.live_enrichment_max_age = DEFAULT_MAX_AGE
    return extractor


def _fake_features(row: dict) -> SimpleNamespace:
    """Stand-in for AlertFeatures — ``feature_row_for_gold`` reads ``__dict__``."""
    return SimpleNamespace(**row)


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
    pd.DataFrame(rows).to_parquet(out, index=False)
    return out


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFindIncompleteRows:
    """Test _find_incomplete_rows static method."""

    def test_finds_rows_with_null_enrichment_fields(self) -> None:
        incomplete_row = _make_feature_row("a1", complete=False)
        complete_row = _make_feature_row("a2", complete=True)
        df = pd.DataFrame([incomplete_row, complete_row])

        result = EnrichmentBackfillScanner._find_incomplete_rows(df, DEFAULT_MAX_AGE)

        assert len(result) == 1
        assert result["alert_id"].iloc[0] == "a1"

    def test_all_complete_returns_empty(self) -> None:
        rows = [_make_feature_row(f"a{i}", complete=True) for i in range(5)]
        df = pd.DataFrame(rows)

        result = EnrichmentBackfillScanner._find_incomplete_rows(df, DEFAULT_MAX_AGE)
        assert result.empty

    def test_partial_nulls_detected(self) -> None:
        row = _make_feature_row("a1", complete=True, partial_fields={"gex": None})
        df = pd.DataFrame([row])

        result = EnrichmentBackfillScanner._find_incomplete_rows(df, DEFAULT_MAX_AGE)
        assert len(result) == 1

    def test_empty_df_returns_empty(self) -> None:
        df = pd.DataFrame({"alert_id": pd.Series([], dtype=str), "delta": pd.Series([], dtype="float64")})
        result = EnrichmentBackfillScanner._find_incomplete_rows(df, DEFAULT_MAX_AGE)
        assert result.empty


class TestReadRecentPartitions:
    """Test _read_recent_partitions method."""

    def test_reads_partitions_within_lookback(
        self, tmp_features_dir: Path, mock_extractor: AsyncMock, mock_calendar: MagicMock
    ) -> None:
        today = datetime.now(UTC).date()
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
        old_date = datetime.now(UTC).date() - timedelta(days=10)
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

    def test_skips_partition_when_read_fails(
        self, tmp_features_dir: Path, mock_extractor: AsyncMock, mock_calendar: MagicMock
    ) -> None:
        today = datetime.now(UTC).date()
        yesterday = today - timedelta(days=1)
        _write_partition(tmp_features_dir, today, [_make_feature_row("today_row")])
        _write_partition(tmp_features_dir, yesterday, [_make_feature_row("yesterday_row")])

        scanner = EnrichmentBackfillScanner(
            feature_extractor=mock_extractor,
            features_output_path=tmp_features_dir,
            lookback_days=3,
            calendar=mock_calendar,
        )

        real_read_parquet = pd.read_parquet
        panic_path_suffix = str((tmp_features_dir / f"dt={today}" / "data.parquet").resolve())

        def _read_parquet_with_error(path: Path) -> pd.DataFrame:
            if str(Path(path).resolve()) == panic_path_suffix:
                raise OSError("simulated read failure")
            return real_read_parquet(path)

        with patch("heber.watch.backfill_scanner.pd.read_parquet", side_effect=_read_parquet_with_error):
            df = scanner._read_recent_partitions()

        assert df is not None
        assert len(df) == 1
        assert df["alert_id"].tolist() == ["yesterday_row"]


class TestScanAndBackfill:
    """Test the full scan_and_backfill flow."""

    @pytest.mark.asyncio()
    async def test_re_enrich_updates_null_fields(
        self, tmp_features_dir: Path, mock_extractor: AsyncMock, mock_calendar: MagicMock
    ) -> None:
        incomplete_row = _make_feature_row("a1", complete=False)
        today = datetime.now(UTC).date()
        _write_partition(tmp_features_dir, today, [incomplete_row])

        # Mock extractor returns a complete feature set
        enriched = _make_feature_row("a1", complete=True)

        mock_extractor.extract.return_value = _fake_features(enriched)

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
        today = datetime.now(UTC).date()
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
        today = datetime.now(UTC).date()
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
    async def test_malformed_expiry_is_rejected_not_truncated(
        self, tmp_features_dir: Path, mock_extractor: AsyncMock, mock_calendar: MagicMock
    ) -> None:
        """A malformed stored expiry must not be silently truncated into a fake
        clean date before re-enrichment fetches Greeks for the wrong contract."""
        row = _make_feature_row("a1", complete=False, partial_fields={"expiry": "2026-02-20junk"})
        today = datetime.now(UTC).date()
        _write_partition(tmp_features_dir, today, [row])

        scanner = EnrichmentBackfillScanner(
            feature_extractor=mock_extractor,
            features_output_path=tmp_features_dir,
            lookback_days=3,
            calendar=mock_calendar,
        )

        summary = await scanner.scan_and_backfill()

        mock_extractor.extract.assert_not_awaited()
        assert summary["failed"] == 1
        assert summary["patched"] == 0

    @pytest.mark.asyncio()
    async def test_missing_expiry_is_rejected_not_fabricated_as_today(
        self, tmp_features_dir: Path, mock_extractor: AsyncMock, mock_calendar: MagicMock
    ) -> None:
        """A missing stored expiry must not be fabricated as date.today()."""
        row = _make_feature_row("a1", complete=False, partial_fields={"expiry": None})
        today = datetime.now(UTC).date()
        _write_partition(tmp_features_dir, today, [row])

        scanner = EnrichmentBackfillScanner(
            feature_extractor=mock_extractor,
            features_output_path=tmp_features_dir,
            lookback_days=3,
            calendar=mock_calendar,
        )

        summary = await scanner.scan_and_backfill()

        mock_extractor.extract.assert_not_awaited()
        assert summary["failed"] == 1

    @pytest.mark.asyncio()
    async def test_batch_size_limits_processing(
        self, tmp_features_dir: Path, mock_extractor: AsyncMock, mock_calendar: MagicMock
    ) -> None:
        rows = [_make_feature_row(f"a{i}", complete=False) for i in range(10)]
        today = datetime.now(UTC).date()
        _write_partition(tmp_features_dir, today, rows)

        mock_extractor.extract.return_value = _fake_features(_make_feature_row("ax", complete=True))

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


class TestPointInTimePreservation:
    """The scanner must never replace good point-in-time enrichment with nulls.

    A row captured live at alert time can hold valid Greeks while one unrelated
    enrichable field is null. Re-running ``extract()`` for such a row past the
    live-enrichment age bound returns nulls for every live-only field, so a
    wholesale row replacement destroyed the original values.
    """

    @staticmethod
    def _scanner(
        tmp_features_dir: Path,
        extractor: AsyncMock,
        calendar: MagicMock,
        **kwargs: object,
    ) -> EnrichmentBackfillScanner:
        return EnrichmentBackfillScanner(
            feature_extractor=extractor,
            features_output_path=tmp_features_dir,
            lookback_days=3,
            calendar=calendar,
            **kwargs,  # type: ignore[arg-type]
        )

    def test_stale_row_is_not_selected(self) -> None:
        stale = _make_feature_row(
            "old",
            complete=True,
            partial_fields={
                "alert_time": datetime.now(UTC) - timedelta(hours=3),
                "max_pain_strike": None,
            },
        )
        df = pd.DataFrame([stale])

        result = EnrichmentBackfillScanner._find_incomplete_rows(df, DEFAULT_MAX_AGE)

        assert result.empty

    def test_disabled_age_bound_selects_nothing(self) -> None:
        """``live_enrichment_max_age=None`` disables the extractor's own gate, so
        every re-enrichment would stamp present-day values onto a past alert."""
        df = pd.DataFrame([_make_feature_row("a1", complete=False)])

        assert EnrichmentBackfillScanner._find_incomplete_rows(df, None).empty

    def test_naive_alert_time_treated_as_utc(self) -> None:
        recent = _make_feature_row(
            "naive",
            partial_fields={"alert_time": datetime.now(UTC).replace(tzinfo=None)},
        )
        df = pd.DataFrame([recent])

        assert len(EnrichmentBackfillScanner._find_incomplete_rows(df, DEFAULT_MAX_AGE)) == 1

    def test_unparseable_alert_time_is_excluded(self) -> None:
        bad = _make_feature_row("bad", partial_fields={"alert_time": None})
        df = pd.DataFrame([bad])

        assert EnrichmentBackfillScanner._find_incomplete_rows(df, DEFAULT_MAX_AGE).empty

    @pytest.mark.asyncio()
    async def test_stale_row_greeks_survive_a_scan(
        self, tmp_features_dir: Path, mock_extractor: AsyncMock, mock_calendar: MagicMock
    ) -> None:
        stale = _make_feature_row(
            "old",
            complete=True,
            partial_fields={
                "alert_time": datetime.now(UTC) - timedelta(hours=3),
                "max_pain_strike": None,
            },
        )
        out = _write_partition(tmp_features_dir, datetime.now(UTC).date(), [stale])

        scanner = self._scanner(tmp_features_dir, mock_extractor, mock_calendar)
        summary = await scanner.scan_and_backfill()

        mock_extractor.extract.assert_not_awaited()
        assert summary["patched"] == 0
        on_disk = pd.read_parquet(out)
        assert on_disk["delta"].iloc[0] == 0.55
        assert pd.isna(on_disk["max_pain_strike"].iloc[0])

    @pytest.mark.asyncio()
    async def test_in_window_failure_does_not_wipe_existing_greeks(
        self, tmp_features_dir: Path, mock_extractor: AsyncMock, mock_calendar: MagicMock
    ) -> None:
        """Gateway 401 on the Greeks step must not null out Greeks already held."""
        row = _make_feature_row("a1", complete=True, partial_fields={"iv_rank": None})
        out = _write_partition(tmp_features_dir, datetime.now(UTC).date(), [row])

        fresh = _make_feature_row("a1", complete=False, partial_fields={"iv_rank": 72.0})
        mock_extractor.extract.return_value = _fake_features(fresh)

        scanner = self._scanner(tmp_features_dir, mock_extractor, mock_calendar)
        summary = await scanner.scan_and_backfill()

        assert summary["patched"] == 1
        on_disk = pd.read_parquet(out)
        assert on_disk["delta"].iloc[0] == 0.55
        assert on_disk["gamma"].iloc[0] == 0.03
        assert on_disk["iv_rank"].iloc[0] == 72.0

    @pytest.mark.asyncio()
    async def test_row_going_stale_before_extraction_is_skipped(
        self, tmp_features_dir: Path, mock_extractor: AsyncMock, mock_calendar: MagicMock
    ) -> None:
        """Selection and extraction are minutes apart for a full batch; a row that
        crosses the bound in between must not be extracted."""
        row = _make_feature_row(
            "a1",
            complete=True,
            partial_fields={"alert_time": datetime.now(UTC) - timedelta(minutes=90), "iv_rank": None},
        )
        _write_partition(tmp_features_dir, datetime.now(UTC).date(), [row])

        scanner = self._scanner(tmp_features_dir, mock_extractor, mock_calendar)
        with patch.object(
            EnrichmentBackfillScanner,
            "_find_incomplete_rows",
            staticmethod(lambda df, _max_age: df),
        ):
            summary = await scanner.scan_and_backfill()

        mock_extractor.extract.assert_not_awaited()
        assert summary["skipped"] == 1
        assert summary["patched"] == 0

    @pytest.mark.asyncio()
    async def test_no_write_when_nothing_was_filled(
        self, tmp_features_dir: Path, mock_extractor: AsyncMock, mock_calendar: MagicMock
    ) -> None:
        row = _make_feature_row("a1", complete=True, partial_fields={"iv_rank": None})
        out = _write_partition(tmp_features_dir, datetime.now(UTC).date(), [row])
        before = out.stat().st_mtime_ns

        mock_extractor.extract.return_value = _fake_features(_make_feature_row("a1", complete=False))

        scanner = self._scanner(tmp_features_dir, mock_extractor, mock_calendar)
        summary = await scanner.scan_and_backfill()

        assert summary["patched"] == 0
        assert summary["skipped"] == 1
        assert out.stat().st_mtime_ns == before

    @pytest.mark.asyncio()
    async def test_ts_available_advances_when_a_value_is_filled(
        self, tmp_features_dir: Path, mock_extractor: AsyncMock, mock_calendar: MagicMock
    ) -> None:
        """A value fetched now is only queryable now — keeping the original
        ts_available would backdate it and reintroduce look-ahead."""
        original_ts = datetime.now(UTC) - timedelta(minutes=30)
        row = _make_feature_row(
            "a1",
            complete=True,
            partial_fields={"iv_rank": None, "ts_available": original_ts},
        )
        out = _write_partition(tmp_features_dir, datetime.now(UTC).date(), [row])

        fresh = _make_feature_row("a1", complete=False, partial_fields={"iv_rank": 72.0})
        mock_extractor.extract.return_value = _fake_features(fresh)

        scanner = self._scanner(tmp_features_dir, mock_extractor, mock_calendar)
        await scanner.scan_and_backfill()

        on_disk = pd.read_parquet(out)
        assert on_disk["ts_available"].iloc[0] > pd.Timestamp(original_ts)

    @pytest.mark.asyncio()
    async def test_concurrent_newer_row_survives_the_patch(
        self, tmp_features_dir: Path, mock_extractor: AsyncMock, mock_calendar: MagicMock
    ) -> None:
        """The scan snapshot is stale by patch time; a live re-write landing in
        between must not be reverted."""
        row = _make_feature_row("a1", complete=True, partial_fields={"iv_rank": None})
        out = _write_partition(tmp_features_dir, datetime.now(UTC).date(), [row])

        newer = _make_feature_row("a1", complete=True, partial_fields={"iv_rank": None, "gex": 9_999_999.0})
        fresh = _make_feature_row("a1", complete=False, partial_fields={"iv_rank": 72.0})

        async def _extract_then_live_write(_record: object) -> SimpleNamespace:
            pd.DataFrame([newer]).to_parquet(out, index=False)
            return _fake_features(fresh)

        mock_extractor.extract.side_effect = _extract_then_live_write

        scanner = self._scanner(tmp_features_dir, mock_extractor, mock_calendar)
        await scanner.scan_and_backfill()

        on_disk = pd.read_parquet(out)
        assert on_disk["gex"].iloc[0] == 9_999_999.0
        assert on_disk["iv_rank"].iloc[0] == 72.0

    @pytest.mark.asyncio()
    async def test_other_rows_in_partition_are_untouched(
        self, tmp_features_dir: Path, mock_extractor: AsyncMock, mock_calendar: MagicMock
    ) -> None:
        target = _make_feature_row("a1", complete=True, partial_fields={"iv_rank": None})
        neighbour = _make_feature_row("a2", complete=True)
        out = _write_partition(tmp_features_dir, datetime.now(UTC).date(), [target, neighbour])

        fresh = _make_feature_row("a1", complete=False, partial_fields={"iv_rank": 72.0})
        mock_extractor.extract.return_value = _fake_features(fresh)

        scanner = self._scanner(tmp_features_dir, mock_extractor, mock_calendar)
        await scanner.scan_and_backfill()

        on_disk = pd.read_parquet(out).set_index("alert_id")
        assert len(on_disk) == 2
        assert on_disk.loc["a2", "iv_rank"] == 45.0
        assert on_disk.loc["a1", "iv_rank"] == 72.0

    def test_future_dated_alert_is_not_selected(self) -> None:
        """Flow alerts can carry a timestamp seconds ahead of wall clock; patching
        one would stamp availability earlier than the event it describes."""
        ahead = _make_feature_row(
            "ahead",
            partial_fields={"alert_time": datetime.now(UTC) + timedelta(seconds=10)},
        )
        df = pd.DataFrame([ahead])

        assert EnrichmentBackfillScanner._find_incomplete_rows(df, DEFAULT_MAX_AGE).empty

    @pytest.mark.asyncio()
    async def test_future_dated_alert_is_not_extracted(
        self, tmp_features_dir: Path, mock_extractor: AsyncMock, mock_calendar: MagicMock
    ) -> None:
        row = _make_feature_row("ahead", partial_fields={"alert_time": datetime.now(UTC) + timedelta(seconds=10)})
        _write_partition(tmp_features_dir, datetime.now(UTC).date(), [row])

        scanner = self._scanner(tmp_features_dir, mock_extractor, mock_calendar)
        with patch.object(
            EnrichmentBackfillScanner,
            "_find_incomplete_rows",
            staticmethod(lambda df, _max_age: df),
        ):
            summary = await scanner.scan_and_backfill()

        mock_extractor.extract.assert_not_awaited()
        assert summary["patched"] == 0

    def test_patched_availability_never_precedes_the_event(
        self, tmp_features_dir: Path, mock_extractor: AsyncMock, mock_calendar: MagicMock
    ) -> None:
        future_event = datetime.now(UTC) + timedelta(minutes=5)
        row = _make_feature_row(
            "a1",
            complete=True,
            partial_fields={"iv_rank": None, "ts_event": future_event},
        )
        out = _write_partition(tmp_features_dir, datetime.now(UTC).date(), [row])

        scanner = self._scanner(tmp_features_dir, mock_extractor, mock_calendar)
        assert scanner._patch_partition(row, {"iv_rank": 72.0}) is True

        on_disk = pd.read_parquet(out)
        assert on_disk["ts_available"].iloc[0] >= on_disk["ts_event"].iloc[0]

    def test_partition_without_availability_column_is_left_alone(
        self, tmp_features_dir: Path, mock_extractor: AsyncMock, mock_calendar: MagicMock
    ) -> None:
        """A fill that cannot be held back from a point-in-time read is not made."""
        row = _make_feature_row("a1", complete=True, partial_fields={"iv_rank": None})
        legacy = {k: v for k, v in row.items() if k != "ts_available"}
        out = _write_partition(tmp_features_dir, datetime.now(UTC).date(), [legacy])
        before = out.read_bytes()

        scanner = self._scanner(tmp_features_dir, mock_extractor, mock_calendar)

        assert scanner._patch_partition(legacy, {"iv_rank": 72.0}) is False
        assert out.read_bytes() == before
