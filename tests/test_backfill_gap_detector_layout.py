"""Regression tests for backfill gap detection across Silver layouts."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from heber.backfill import GapDetector


def _mkdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def test_detect_gaps_supports_legacy_provider_feed_layout(tmp_path: Path) -> None:
    _mkdir(tmp_path / "silver" / "alpaca_bars" / "dt=2026-01-01")
    _mkdir(tmp_path / "silver" / "alpaca_bars" / "dt=2026-01-03")

    detector = GapDetector(storage_root=str(tmp_path))
    gaps = detector.detect_gaps("alpaca", "bars", date(2026, 1, 1), date(2026, 1, 3))

    assert gaps == [(date(2026, 1, 2), date(2026, 1, 2))]


def test_detect_gaps_supports_canonical_feed_instrument_layout(tmp_path: Path) -> None:
    _mkdir(tmp_path / "silver" / "feed=bars" / "instrument_type=equity" / "dt=2026-01-01" / "hour=14")
    _mkdir(tmp_path / "silver" / "feed=bars" / "instrument_type=option" / "dt=2026-01-03")

    detector = GapDetector(storage_root=str(tmp_path))
    gaps = detector.detect_gaps("alpaca", "bars", date(2026, 1, 1), date(2026, 1, 3))

    assert gaps == [(date(2026, 1, 2), date(2026, 1, 2))]


def test_detect_gaps_unions_dates_across_layouts(tmp_path: Path) -> None:
    _mkdir(tmp_path / "silver" / "alpaca_bars" / "dt=2026-01-01")
    _mkdir(tmp_path / "silver" / "feed=bars" / "instrument_type=equity" / "dt=2026-01-02")

    detector = GapDetector(storage_root=str(tmp_path))
    gaps = detector.detect_gaps("alpaca", "bars", date(2026, 1, 1), date(2026, 1, 3))

    assert gaps == [(date(2026, 1, 3), date(2026, 1, 3))]
