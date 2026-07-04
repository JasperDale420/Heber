"""Tests for the Massive vendor-raw archive path helper."""

from datetime import date

from heber.backfill.massive.paths import archive_path


def test_archive_path_layout():
    p = archive_path("/root", "day_aggs_v1", date(2024, 1, 2))
    assert str(p).endswith("us_stocks_sip/day_aggs_v1/2024/01/2024-01-02.csv.gz")
