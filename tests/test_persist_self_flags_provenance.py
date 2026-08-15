"""Every Gold feature write stamps its own enrichment provenance.

The freshness gate refuses stale live-only enrichment, but it can be disabled
(``--max-enrichment-age-hours 0``) for a deliberate backlog replay — and that
path wrote present-day Greeks onto past alerts with nothing recording it. The
one-shot migration only covered rows that already existed.

``persist_features_to_gold`` now flags at write time, where ``ts_available`` is
authoritative, so no future write can land unflagged.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from heber.config import settings
from heber.ml.datasets import (
    QUALITY_FLAG_ENRICHMENT_CAPTURED_LATE,
    persist_features_to_gold,
    quality_flag_series,
)
from heber.watch.features import QUALITY_FLAG_GREEKS_NO_POINT_IN_TIME_SOURCE

ALERT = datetime(2026, 3, 11, 15, 0, tzinfo=UTC)


def _row(**kw) -> dict:
    base = {
        "alert_id": "a1",
        "alert_time": ALERT,
        "ts_event": ALERT,
        "ts_available": ALERT + timedelta(seconds=3),
        "delta": 0.5,
        "gamma": 0.1,
        "theta": -0.2,
        "vega": 0.3,
        "iv": 0.4,
    }
    base.update(kw)
    return base


def _written(path, dt_str: str = "2026-03-11") -> pd.DataFrame:
    return pd.read_parquet(path / f"dt={dt_str}" / "data.parquet")


def _flags(df: pd.DataFrame) -> list[list[str]]:
    return [list(f) for f in quality_flag_series(df)]


def test_live_write_is_not_flagged(tmp_path) -> None:  # noqa: ANN001
    """The live consumer persists seconds after the alert — it must stay unflagged."""
    persist_features_to_gold(pd.DataFrame([_row()]), output_path=tmp_path)

    assert _flags(_written(tmp_path)) == [[]]


def test_stale_replay_write_is_flagged(tmp_path) -> None:  # noqa: ANN001
    """Gate disabled for a backlog replay: Greeks fetched now, alert days old."""
    persist_features_to_gold(
        pd.DataFrame([_row(ts_available=ALERT + timedelta(days=3))]),
        output_path=tmp_path,
    )

    assert QUALITY_FLAG_ENRICHMENT_CAPTURED_LATE in _flags(_written(tmp_path))[0]


def test_gate_refused_row_is_not_flagged_late(tmp_path) -> None:  # noqa: ANN001
    """A refused row carries null enrichment, not stale enrichment.

    Backfilled partitions are written days after the alert by design, so a
    lag-only rule would mislabel every one of them.
    """
    refused = _row(
        ts_available=ALERT + timedelta(days=7),
        delta=None,
        gamma=None,
        theta=None,
        vega=None,
        iv=None,
        quality_flags=[QUALITY_FLAG_GREEKS_NO_POINT_IN_TIME_SOURCE],
    )
    persist_features_to_gold(pd.DataFrame([refused]), output_path=tmp_path)

    written = _flags(_written(tmp_path))[0]
    assert QUALITY_FLAG_ENRICHMENT_CAPTURED_LATE not in written
    assert QUALITY_FLAG_GREEKS_NO_POINT_IN_TIME_SOURCE in written


def test_existing_rows_are_not_reflagged_on_a_later_write(tmp_path) -> None:  # noqa: ANN001
    """Appending to a partition must not re-evaluate rows already written."""
    persist_features_to_gold(pd.DataFrame([_row(alert_id="first")]), output_path=tmp_path)
    persist_features_to_gold(
        pd.DataFrame([_row(alert_id="second", ts_available=ALERT + timedelta(days=3))]),
        output_path=tmp_path,
    )

    written = _written(tmp_path).set_index("alert_id")
    flags = {k: list(v) for k, v in quality_flag_series(written).items()}
    assert flags["first"] == []
    assert QUALITY_FLAG_ENRICHMENT_CAPTURED_LATE in flags["second"]


def test_flagging_is_idempotent_across_rewrites(tmp_path) -> None:  # noqa: ANN001
    """Re-persisting the same alert_id must not accumulate duplicate flags."""
    stale = _row(ts_available=ALERT + timedelta(days=3))
    persist_features_to_gold(pd.DataFrame([stale]), output_path=tmp_path)
    persist_features_to_gold(pd.DataFrame([stale]), output_path=tmp_path)

    written = _written(tmp_path)
    assert len(written) == 1
    assert _flags(written)[0].count(QUALITY_FLAG_ENRICHMENT_CAPTURED_LATE) == 1


@pytest.mark.parametrize("column", ["iv_rank", "max_pain_strike"])
def test_stale_non_greek_enrichment_is_flagged(tmp_path, column: str) -> None:  # noqa: ANN001
    """Enrichment steps fail independently, so Greeks can be null while these are not."""
    row = _row(
        ts_available=ALERT + timedelta(days=3),
        delta=None,
        gamma=None,
        theta=None,
        vega=None,
        iv=None,
        **{column: 42.0},
    )
    persist_features_to_gold(pd.DataFrame([row]), output_path=tmp_path)

    # All Greeks null and unflagged -> quarantined; the flag must still be recorded.
    quarantine = tmp_path / "_quarantine" / "all_greeks_null" / "dt=2026-03-11"
    files = sorted(quarantine.glob("*.parquet"))
    assert files, "expected the row to be quarantined"
    assert QUALITY_FLAG_ENRICHMENT_CAPTURED_LATE in _flags(pd.read_parquet(files[0]))[0]


def test_disabling_the_gate_does_not_flag_every_live_write(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    """`...MAX_AGE_MINUTES=0` disables the gate, not the flagging.

    Taking the bound straight from that setting made it `timedelta(0)`, so every
    row with any write lag — a live row persisted three seconds after the alert
    included — was marked late. That is backwards for the one configuration that
    deliberately lets stale enrichment through.
    """
    monkeypatch.setattr(settings, "watch_live_enrichment_max_age_minutes", 0)

    persist_features_to_gold(pd.DataFrame([_row(alert_id="live")]), output_path=tmp_path)
    persist_features_to_gold(
        pd.DataFrame([_row(alert_id="stale", ts_available=ALERT + timedelta(days=3))]),
        output_path=tmp_path,
    )

    written = _written(tmp_path).set_index("alert_id")
    flags = {k: list(v) for k, v in quality_flag_series(written).items()}
    assert flags["live"] == []
    assert QUALITY_FLAG_ENRICHMENT_CAPTURED_LATE in flags["stale"]
