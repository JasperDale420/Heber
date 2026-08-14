"""Tests for retro-flagging historical rows whose enrichment was not point-in-time.

Rows written before the freshness gate existed carry Greeks that were fetched
long after the alert — the ``EnrichmentBackfillScanner`` re-enriched up to three
days back on an hourly cycle. The values are real, so the rows are not corrupt,
but they are not point-in-time either. Flagging them lets a training run exclude
or down-weight them; ``ts_available - alert_time`` is what exposes them.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import numpy as np
import pandas as pd

from heber.ml.datasets import (
    QUALITY_FLAG_ENRICHMENT_CAPTURED_LATE,
    QUALITY_FLAG_ENRICHMENT_PROVENANCE_UNKNOWN,
    add_enrichment_provenance_flags,
)

ALERT = datetime(2026, 3, 11, 15, 0, tzinfo=UTC)
BOUND = timedelta(minutes=60)


def _row(**kw) -> dict:
    base = {
        "alert_id": "a",
        "alert_time": ALERT,
        "delta": 0.5,
        "gamma": 0.1,
        "theta": -0.2,
        "vega": 0.3,
        "iv": 0.4,
    }
    base.update(kw)
    return base


def test_enrichment_captured_after_the_bound_is_flagged() -> None:
    df = pd.DataFrame([_row(alert_id="late", ts_available=ALERT + timedelta(hours=18))])

    out = add_enrichment_provenance_flags(df, max_age=BOUND)

    assert QUALITY_FLAG_ENRICHMENT_CAPTURED_LATE in out.at[0, "quality_flags"]


def test_enrichment_captured_within_the_bound_is_untouched() -> None:
    df = pd.DataFrame([_row(alert_id="live", ts_available=ALERT + timedelta(seconds=20))])

    out = add_enrichment_provenance_flags(df, max_age=BOUND)

    assert out.at[0, "quality_flags"] == []


def test_refused_enrichment_is_not_flagged_late_even_when_written_much_later() -> None:
    """A backfilled row with no Greeks was refused, not contaminated.

    The 2026-08-06/07 partitions were written a week after the alerts, so a
    lag-only rule would mislabel every one of them.
    """
    df = pd.DataFrame(
        [
            _row(
                alert_id="refused",
                ts_available=ALERT + timedelta(days=7),
                delta=None,
                gamma=None,
                theta=None,
                vega=None,
                iv=None,
            )
        ]
    )

    out = add_enrichment_provenance_flags(df, max_age=BOUND)

    assert QUALITY_FLAG_ENRICHMENT_CAPTURED_LATE not in out.at[0, "quality_flags"]


def test_rows_without_ts_available_are_flagged_unknown() -> None:
    """Partitions through 2026-03-10 predate the write-time column entirely."""
    df = pd.DataFrame([{"alert_id": "legacy", "alert_time": ALERT, "delta": 0.5}])

    out = add_enrichment_provenance_flags(df, max_age=BOUND)

    assert QUALITY_FLAG_ENRICHMENT_PROVENANCE_UNKNOWN in out.at[0, "quality_flags"]


def test_existing_flags_are_preserved_including_ndarray_cells() -> None:
    """Parquet round-trips list columns as ndarray; appending must not drop them."""
    df = pd.DataFrame(
        [
            _row(
                alert_id="flagged",
                ts_available=ALERT + timedelta(hours=18),
                quality_flags=np.array(["market_tide_recovered_from_silver"], dtype=object),
            )
        ]
    )

    out = add_enrichment_provenance_flags(df, max_age=BOUND)

    assert sorted(out.at[0, "quality_flags"]) == sorted(
        ["market_tide_recovered_from_silver", QUALITY_FLAG_ENRICHMENT_CAPTURED_LATE]
    )


def test_flagging_is_idempotent() -> None:
    """Re-running the migration must not duplicate a flag."""
    df = pd.DataFrame([_row(alert_id="late", ts_available=ALERT + timedelta(hours=18))])

    once = add_enrichment_provenance_flags(df, max_age=BOUND)
    twice = add_enrichment_provenance_flags(once, max_age=BOUND)

    assert twice.at[0, "quality_flags"].count(QUALITY_FLAG_ENRICHMENT_CAPTURED_LATE) == 1


def test_no_other_column_is_modified() -> None:
    """The migration only appends flags — values must survive untouched."""
    df = pd.DataFrame([_row(alert_id="late", ts_available=ALERT + timedelta(hours=18))])

    out = add_enrichment_provenance_flags(df, max_age=BOUND)

    for col in ("alert_id", "alert_time", "delta", "gamma", "theta", "vega", "iv", "ts_available"):
        assert out[col].tolist() == df[col].tolist(), col


def test_stale_iv_rank_is_flagged_even_when_greeks_are_null() -> None:
    """Enrichment steps fail independently, so Greeks can be null while iv_rank is not.

    The pre-gate scanner re-enriched any null field, so a row can carry a
    present-day iv_rank or max_pain with no Greeks at all. Keying only on Greeks
    would leave those contaminated rows unflagged.
    """
    df = pd.DataFrame(
        [
            _row(
                alert_id="iv_only",
                ts_available=ALERT + timedelta(hours=18),
                delta=None,
                gamma=None,
                theta=None,
                vega=None,
                iv=None,
                iv_rank=42.0,
            )
        ]
    )

    out = add_enrichment_provenance_flags(df, max_age=BOUND)

    assert QUALITY_FLAG_ENRICHMENT_CAPTURED_LATE in out.at[0, "quality_flags"]


def test_silver_recovered_fields_alone_do_not_count_as_live_enrichment() -> None:
    """gex/market_tide recovered from Silver are point-in-time — not a late live fetch."""
    df = pd.DataFrame(
        [
            _row(
                alert_id="recovered",
                ts_available=ALERT + timedelta(days=7),
                delta=None,
                gamma=None,
                theta=None,
                vega=None,
                iv=None,
                gex=1.5,
                market_tide_net_premium=-19_900_000.0,
                quality_flags=["gex_recovered_from_silver", "market_tide_recovered_from_silver"],
            )
        ]
    )

    out = add_enrichment_provenance_flags(df, max_age=BOUND)

    assert QUALITY_FLAG_ENRICHMENT_CAPTURED_LATE not in out.at[0, "quality_flags"]


def test_duplicate_index_labels_do_not_raise() -> None:
    """The helper is exported, so it must not assume a unique index."""
    df = pd.DataFrame(
        [
            _row(alert_id="a", ts_available=ALERT + timedelta(hours=18)),
            _row(alert_id="b", ts_available=ALERT + timedelta(seconds=5)),
        ],
        index=[7, 7],
    )

    out = add_enrichment_provenance_flags(df, max_age=BOUND)

    assert QUALITY_FLAG_ENRICHMENT_CAPTURED_LATE in list(out["quality_flags"])[0]
    assert list(out["quality_flags"])[1] == []


def test_write_path_preserves_every_row_and_is_idempotent(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    """The migration rewrites partitions in place — it must never lose or duplicate a row."""
    import scripts.flag_legacy_enrichment as mig

    part = tmp_path / "dt=2026-03-11"
    part.mkdir()
    out_file = part / "data.parquet"
    original = pd.DataFrame(
        [
            _row(alert_id="late", ts_available=ALERT + timedelta(hours=18)),
            _row(alert_id="live", ts_available=ALERT + timedelta(seconds=5)),
        ]
    )
    original.to_parquet(out_file, index=False)
    monkeypatch.setattr(mig, "GOLD_FEATURES", tmp_path)

    rows, late, unknown, err = mig.flag_partition("2026-03-11", BOUND, write=True)
    assert err is None
    assert (rows, late, unknown) == (2, 1, 0)

    after = pd.read_parquet(out_file)
    assert after["alert_id"].tolist() == ["late", "live"]
    assert set(after.columns) >= set(original.columns)
    assert QUALITY_FLAG_ENRICHMENT_CAPTURED_LATE in list(after["quality_flags"])[0]
    # Cells round-trip as ndarray, so compare length rather than to a list literal.
    assert len(list(after["quality_flags"])[1]) == 0

    # Re-running must be a no-op, not a second flag or a rewrite.
    rows2, late2, unknown2, err2 = mig.flag_partition("2026-03-11", BOUND, write=True)
    assert (err2, late2, unknown2) == (None, 0, 0)
    assert len(pd.read_parquet(out_file)) == 2


def test_partition_dates_never_includes_the_quarantine_tree(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    """Quarantined rows carry a dt= key too; the migration must not walk into them."""
    import scripts.flag_legacy_enrichment as mig

    (tmp_path / "dt=2026-03-11").mkdir()
    (tmp_path / "_quarantine" / "all_greeks_null" / "dt=2026-03-12").mkdir(parents=True)
    monkeypatch.setattr(mig, "GOLD_FEATURES", tmp_path)

    assert mig.partition_dates() == ["2026-03-11"]
