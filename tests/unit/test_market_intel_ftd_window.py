"""FTD features need a read window wide enough for the SEC's publication lag.

Fails-to-deliver data is published roughly 15-30 days after the settlement date
it describes, so on any given day the newest FTD record in existence is weeks
old. The Gold poller runs with a one-day lookback, and ``market_intel`` both read
and filtered FTD to that window — which can never contain an FTD record. The
pipeline therefore produced zero FTD rows every night by construction, not
because anything was broken.

Widening the read alone is not enough: the compute loop re-filters every dataset
to the requested range, so rows fetched from the wider window would be discarded
immediately afterwards. Both halves have to use the same window.

Only FTD gets the wide window. Applying it to the other datasets in this pipeline
would multiply their read cost for no benefit — they are not lagged.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from heber.features.pipelines.market_intel_features import (
    FTD_LOOKBACK_DAYS,
    MarketIntelPipeline,
)

pytestmark = pytest.mark.unit

START = "2026-07-28"
END = "2026-07-29"


class _RecordingReader:
    """Captures every read_silver call and serves canned frames."""

    def __init__(self, frames: dict[str, pd.DataFrame] | None = None):
        self.calls: list[dict] = []
        self._frames = frames or {}

    def read_silver(self, dataset: str, **kwargs) -> pd.DataFrame:
        self.calls.append({"dataset": dataset, **kwargs})
        return self._frames.get(dataset, pd.DataFrame())

    def write_gold(self, dataset: str, df: pd.DataFrame, **_kwargs) -> str:
        return f"/gold/{dataset}"

    def window_for(self, dataset: str) -> tuple[datetime, datetime]:
        for call in self.calls:
            if call["dataset"] == dataset:
                return call["time_range"]
        raise AssertionError(f"{dataset} was never read")


def _run(reader: _RecordingReader, datasets: tuple[str, ...]) -> dict:
    pipeline = MarketIntelPipeline(reader=reader)
    return pipeline.run(start_date=START, end_date=END, datasets=datasets, dry_run=True)


def test_ftd_is_read_over_the_publication_lag_window() -> None:
    reader = _RecordingReader()
    _run(reader, ("ftd",))

    start, _end = reader.window_for("ftd")
    requested_start = datetime.fromisoformat(START).replace(tzinfo=start.tzinfo)
    assert (requested_start - start).days >= 30, (
        "FTD read window is narrower than the SEC publication lag, so it can never contain an FTD record"
    )
    assert (requested_start - start).days == FTD_LOOKBACK_DAYS


def test_other_datasets_keep_the_narrow_window() -> None:
    """Widening every dataset would multiply read cost for no benefit."""
    reader = _RecordingReader()
    _run(reader, ("greek_exposure", "ftd"))

    greek_start, _ = reader.window_for("greek_exposure")
    requested_start = datetime.fromisoformat(START).replace(tzinfo=greek_start.tzinfo)
    assert greek_start == requested_start


def test_lagged_ftd_rows_survive_the_output_filter() -> None:
    """The compute loop must filter on the same widened window it read."""
    lagged_day = (datetime.fromisoformat(START) - timedelta(days=29)).replace(tzinfo=UTC)
    ftd = pd.DataFrame(
        [
            {
                "instrument_key": "equity:AAPL",
                "ts_event": lagged_day,
                "quantity": 1000,
                "price": 10.0,
                "value": 10000.0,
                "ftd_date": lagged_day.date().isoformat(),
            }
        ]
    )
    reader = _RecordingReader({"ftd": ftd})

    stats = _run(reader, ("ftd",))

    assert stats["ftd_features"]["status"] == "success", stats
    assert stats["ftd_features"]["rows"] == 1, "the lagged row was read and then filtered back out by the narrow window"


def test_reads_prune_non_matching_partitions() -> None:
    """Without dt pruning the reader opens every partition to test ts_event.

    Silver ftd holds ~1000 day-partitions of ~34 files each, so an unpruned read
    opens tens of thousands of files off a slow volume just to discard almost all
    of them.
    """
    reader = _RecordingReader()
    _run(reader, ("darkpool", "greek_exposure", "options_sentiment", "ftd"))

    unpruned = [c["dataset"] for c in reader.calls if not c.get("prune_by_dt")]
    assert not unpruned, f"these reads scan every partition: {unpruned}"


def test_ftd_features_omits_the_invalid_days_outstanding_column() -> None:
    """``ftd_days_outstanding`` was structurally always 1 and is now gone.

    Data-Gateway maps the upstream ``date`` field to both the envelope's
    ``ts_event`` and Silver's ``ftd_date``. The feature grouped by the date
    derived from ``ts_event`` and then counted distinct ``ftd_date`` within that
    group — the same value, so the count could never exceed one. It reached ML
    training as a constant.

    It is removed rather than repaired: SEC fails-to-deliver records are
    aggregate outstanding balances, so how long a fail has been open cannot be
    derived from them at all.
    """
    from heber.features.pipelines.market_intel_features import compute_ftd_features

    day = datetime(2026, 6, 30, tzinfo=UTC)
    ftd = pd.DataFrame(
        [
            {
                "instrument_key": "equity:AAPL",
                "ts_event": day,
                "quantity": 1000,
                "price": 10.0,
                "value": 10000.0,
                "ftd_date": "2026-06-30",
            },
            {
                "instrument_key": "equity:AAPL",
                "ts_event": day,
                "quantity": 500,
                "price": 11.0,
                "value": 5500.0,
                "ftd_date": "2026-06-30",
            },
        ]
    )

    out = compute_ftd_features(ftd)

    assert "ftd_days_outstanding" not in out.columns
    # The genuine aggregates are untouched.
    assert out["ftd_quantity"].iloc[0] == 1500
    assert out["ftd_trade_count"].iloc[0] == 2
