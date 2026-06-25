"""Deep behavioral tests for label availability math (heber.gold.labels + duration).

Covers every public function:
- ``parse_duration`` (heber.gold.duration) — all units + invalid formats.
- ``compute_availability_time`` — forward_window/availability_lag combinations,
  daily market-close snapping, and DST-crossing windows (zoneinfo, fixed dates).
- ``LabelMetadata`` / ``LabelDataset`` round-trips and delta accessors.
- ``write_label`` / ``read_label`` point-in-time correctness.

All dates are fixed constants; no wall-clock assertions.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from heber.gold.duration import parse_duration
from heber.gold.labels import (
    LabelDataset,
    LabelMetadata,
    compute_availability_time,
    read_label,
    write_label,
)

pytestmark = pytest.mark.unit

ET = ZoneInfo("America/New_York")


# ---------------------------------------------------------------------------
# parse_duration
# ---------------------------------------------------------------------------
class TestParseDuration:
    def test_seconds(self):
        assert parse_duration("30s") == timedelta(seconds=30)

    def test_minutes_lowercase_m(self):
        assert parse_duration("15m") == timedelta(minutes=15)

    def test_hours(self):
        assert parse_duration("6h") == timedelta(hours=6)

    def test_days(self):
        assert parse_duration("5d") == timedelta(days=5)

    def test_weeks(self):
        assert parse_duration("2w") == timedelta(weeks=2)

    def test_months_uppercase_m_is_thirty_days(self):
        assert parse_duration("3M") == timedelta(days=90)

    def test_zero_value(self):
        assert parse_duration("0s") == timedelta(0)
        assert parse_duration("0d") == timedelta(0)

    def test_leading_trailing_whitespace_stripped(self):
        assert parse_duration("  5d  ") == timedelta(days=5)

    def test_case_sensitivity_m_vs_M(self):
        # Lowercase m = minutes, uppercase M = months. Distinct results.
        assert parse_duration("1m") == timedelta(minutes=1)
        assert parse_duration("1M") == timedelta(days=30)

    @pytest.mark.parametrize(
        "bad",
        ["5", "d", "5x", "-5d", "5.5d", "5 d", "", "5dd", "dd"],
    )
    def test_invalid_formats_raise(self, bad):
        with pytest.raises(ValueError, match="Invalid duration format"):
            parse_duration(bad)


# ---------------------------------------------------------------------------
# compute_availability_time
# ---------------------------------------------------------------------------
class TestComputeAvailabilityTime:
    def test_daily_window_snaps_to_market_close(self):
        label_time = datetime(2026, 1, 5, 10, 30, 0, tzinfo=UTC)
        result = compute_availability_time(label_time, "5d", "0s")
        # +5d -> 2026-01-10, then snapped to 16:05 New York time
        # (21:05 UTC in January). Snapping in UTC would leak the label early.
        assert result == datetime(2026, 1, 10, 21, 5, 0, tzinfo=UTC)

    def test_custom_market_close_time(self):
        label_time = datetime(2026, 1, 5, 10, 30, 0, tzinfo=UTC)
        result = compute_availability_time(label_time, "1d", "0s", market_close_time="13:00:00")
        assert result == datetime(2026, 1, 6, 18, 0, 0, tzinfo=UTC)

    def test_availability_lag_added_after_close_snap(self):
        label_time = datetime(2026, 1, 5, 10, 30, 0, tzinfo=UTC)
        result = compute_availability_time(label_time, "5d", "2h")
        # snapped to 16:05 then +2h lag.
        assert result == datetime(2026, 1, 10, 23, 5, 0, tzinfo=UTC)

    def test_intraday_window_no_close_snap(self):
        # Hours window has no 'd' -> raw forward delta, no market-close re-anchor.
        label_time = datetime(2026, 1, 5, 10, 30, 0, tzinfo=UTC)
        result = compute_availability_time(label_time, "1h", "0s")
        assert result == datetime(2026, 1, 5, 11, 30, 0, tzinfo=UTC)

    def test_minute_window_no_close_snap(self):
        label_time = datetime(2026, 1, 5, 10, 30, 0, tzinfo=UTC)
        result = compute_availability_time(label_time, "30m", "0s")
        assert result == datetime(2026, 1, 5, 11, 0, 0, tzinfo=UTC)

    def test_week_window_no_close_snap(self):
        # 'w' contains no 'd' -> no market-close snapping; time preserved.
        label_time = datetime(2026, 1, 5, 10, 30, 0, tzinfo=UTC)
        result = compute_availability_time(label_time, "1w", "0s")
        assert result == datetime(2026, 1, 12, 10, 30, 0, tzinfo=UTC)

    def test_month_window_no_close_snap(self):
        # 'M' (months) contains no 'd' -> +30 days, no close snap.
        label_time = datetime(2026, 1, 5, 10, 30, 0, tzinfo=UTC)
        result = compute_availability_time(label_time, "1M", "0s")
        assert result == datetime(2026, 2, 4, 10, 30, 0, tzinfo=UTC)

    def test_intraday_window_with_lag(self):
        label_time = datetime(2026, 1, 5, 10, 30, 0, tzinfo=UTC)
        result = compute_availability_time(label_time, "1h", "15m")
        assert result == datetime(2026, 1, 5, 11, 45, 0, tzinfo=UTC)

    def test_dst_spring_forward_crossing_window(self):
        # 2026 US DST starts Sun 2026-03-08. Label on 2026-03-06 (EST, -05:00),
        # +5d lands on 2026-03-11 (EDT, -04:00). Market-close snap re-anchors to
        # 16:05 local wall-clock on the *result* date with that date's offset.
        label_time = datetime(2026, 3, 6, 10, 0, 0, tzinfo=ET)
        result = compute_availability_time(label_time, "5d", "0s")
        assert result.hour == 16 and result.minute == 5
        # Result date is past the spring-forward boundary -> EDT offset.
        assert result.utcoffset() == timedelta(hours=-4)
        assert result.date() == datetime(2026, 3, 11).date()

    def test_dst_fall_back_crossing_window(self):
        # 2026 US DST ends Sun 2026-11-01. Label 2026-10-30 (EDT, -04:00),
        # +5d -> 2026-11-04 (EST, -05:00).
        label_time = datetime(2026, 10, 30, 10, 0, 0, tzinfo=ET)
        result = compute_availability_time(label_time, "5d", "0s")
        assert result.hour == 16 and result.minute == 5
        assert result.utcoffset() == timedelta(hours=-5)
        assert result.date() == datetime(2026, 11, 4).date()

    def test_dst_aware_result_monotonic_after_label(self):
        # Availability must never precede the label time across a DST boundary.
        label_time = datetime(2026, 3, 6, 10, 0, 0, tzinfo=ET)
        result = compute_availability_time(label_time, "5d", "0s")
        assert result > label_time


# ---------------------------------------------------------------------------
# LabelMetadata
# ---------------------------------------------------------------------------
class TestLabelMetadata:
    def test_defaults(self):
        md = LabelMetadata()
        assert md.dataset_type == "label"
        assert md.forward_window == "1d"
        assert md.label_horizon == "close_to_close"
        assert md.availability_lag == "0s"

    def test_to_dict_round_trip(self):
        md = LabelMetadata(forward_window="5d", label_horizon="ctc", availability_lag="2h")
        assert LabelMetadata.from_dict(md.to_dict()) == md

    def test_from_dict_fills_missing_with_defaults(self):
        md = LabelMetadata.from_dict({})
        assert md == LabelMetadata()

    def test_from_dict_partial(self):
        md = LabelMetadata.from_dict({"forward_window": "10d"})
        assert md.forward_window == "10d"
        assert md.availability_lag == "0s"

    def test_forward_window_delta_accessor(self):
        assert LabelMetadata(forward_window="5d").get_forward_window_delta() == timedelta(days=5)

    def test_availability_lag_delta_accessor(self):
        assert LabelMetadata(availability_lag="2h").get_availability_lag_delta() == timedelta(hours=2)


# ---------------------------------------------------------------------------
# LabelDataset
# ---------------------------------------------------------------------------
class TestLabelDataset:
    def test_to_dict_from_dict_round_trip(self):
        created = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
        ds = LabelDataset(
            name="ret5",
            metadata=LabelMetadata(forward_window="5d"),
            version="v2.0.0",
            created_at=created,
            created_by="tester",
        )
        rebuilt = LabelDataset.from_dict(ds.to_dict())
        assert rebuilt.name == "ret5"
        assert rebuilt.version == "v2.0.0"
        assert rebuilt.created_at == created
        assert rebuilt.created_by == "tester"
        assert rebuilt.metadata == ds.metadata

    def test_from_dict_defaults(self):
        data = {
            "name": "ret1",
            "created_at": datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
        }
        ds = LabelDataset.from_dict(data)
        assert ds.version == "v1.0.0"
        assert ds.created_by == "system"
        assert ds.metadata == LabelMetadata()

    def test_save_and_load_round_trip(self, tmp_path):
        created = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
        ds = LabelDataset(
            name="ret5",
            metadata=LabelMetadata(forward_window="5d", availability_lag="1h"),
            version="v1.0.0",
            created_at=created,
            created_by="tester",
        )
        path = ds.save(tmp_path)
        assert path.exists()
        loaded = LabelDataset.load(tmp_path, "ret5")
        assert loaded.to_dict() == ds.to_dict()


# ---------------------------------------------------------------------------
# write_label / read_label
# ---------------------------------------------------------------------------
class TestWriteReadLabel:
    def _frame(self):
        return pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL", "equity:MSFT"],
                "ts_label": [
                    datetime(2026, 1, 5, 10, 0, 0, tzinfo=UTC),
                    datetime(2026, 1, 20, 10, 0, 0, tzinfo=UTC),
                ],
                "label": [0.05, 0.03],
            }
        )

    def test_write_computes_ts_available(self, tmp_path):
        path = write_label(tmp_path, "ret5", self._frame(), "5d")
        assert path.exists()
        written = pd.read_parquet(path)
        assert "ts_available" in written.columns
        assert "ts_event" in written.columns
        # AAPL label 2026-01-05 +5d snapped to 16:05 ET -> 2026-01-10 21:05 UTC.
        aapl = written[written["instrument_key"] == "equity:AAPL"].iloc[0]
        assert pd.Timestamp(aapl["ts_available"]) == pd.Timestamp("2026-01-10 21:05:00", tz="UTC")

    def test_write_missing_required_column_raises(self, tmp_path):
        df = pd.DataFrame({"instrument_key": ["equity:AAPL"], "label": [0.1]})
        with pytest.raises(ValueError, match="Missing required columns"):
            write_label(tmp_path, "ret5", df, "5d")

    def test_read_enforces_point_in_time(self, tmp_path):
        write_label(tmp_path, "ret5", self._frame(), "5d")
        # asof between AAPL availability (01-10 21:05) and MSFT availability (01-25).
        asof = datetime(2026, 1, 12, 0, 0, 0, tzinfo=UTC)
        out = read_label(tmp_path, "ret5", asof_time=asof)
        assert list(out["instrument_key"]) == ["equity:AAPL"]

    def test_read_returns_all_when_asof_after_all(self, tmp_path):
        write_label(tmp_path, "ret5", self._frame(), "5d")
        asof = datetime(2026, 3, 1, 0, 0, 0, tzinfo=UTC)
        out = read_label(tmp_path, "ret5", asof_time=asof)
        assert set(out["instrument_key"]) == {"equity:AAPL", "equity:MSFT"}

    def test_read_returns_empty_when_asof_before_all(self, tmp_path):
        write_label(tmp_path, "ret5", self._frame(), "5d")
        asof = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
        out = read_label(tmp_path, "ret5", asof_time=asof)
        assert out.empty

    def test_read_instrument_key_filter(self, tmp_path):
        write_label(tmp_path, "ret5", self._frame(), "5d")
        asof = datetime(2026, 3, 1, 0, 0, 0, tzinfo=UTC)
        out = read_label(tmp_path, "ret5", asof_time=asof, instrument_keys=["equity:MSFT"])
        assert list(out["instrument_key"]) == ["equity:MSFT"]

    def test_read_missing_dataset_returns_empty(self, tmp_path):
        out = read_label(tmp_path, "nonexistent", asof_time=datetime(2026, 1, 1, tzinfo=UTC))
        assert out.empty

    def test_read_specific_version(self, tmp_path):
        write_label(tmp_path, "ret5", self._frame(), "5d", version="v1.0.0")
        asof = datetime(2026, 3, 1, 0, 0, 0, tzinfo=UTC)
        out = read_label(tmp_path, "ret5", asof_time=asof, version="v1.0.0")
        assert len(out) == 2

    def test_read_latest_version_when_multiple(self, tmp_path):
        write_label(tmp_path, "ret5", self._frame().iloc[[0]], "5d", version="v1.0.0")
        write_label(tmp_path, "ret5", self._frame(), "5d", version="v2.0.0")
        asof = datetime(2026, 3, 1, 0, 0, 0, tzinfo=UTC)
        out = read_label(tmp_path, "ret5", asof_time=asof)
        # Latest version (v2.0.0) has both rows.
        assert len(out) == 2
