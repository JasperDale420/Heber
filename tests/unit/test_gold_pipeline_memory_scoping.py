"""Read-scoping regressions for the two Gold pipelines that OOM-killed the poller.

market_regime and darkpool both failed 3/3 attempts nightly inside the 3 GB
gold-poller container. Neither pipeline's `run()` had any test coverage, so the
scoping that keeps them alive is asserted here rather than assumed.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pandas as pd
import pytest

from heber.features.pipelines.darkpool_features import DarkpoolPipeline
from heber.features.pipelines.market_regime_features import (
    LOOKBACK_DAYS,
    MarketRegimePipeline,
    compute_dispersion,
)

FIXTURE_START = datetime(2026, 4, 4, tzinfo=UTC)
GAP_DAY_OFFSET = 2  # AAPL has intraday-only bars on this day
WINDOW_START = datetime(2026, 8, 2, tzinfo=UTC)  # bar_start lands on FIXTURE_START
WINDOW_END = datetime(2026, 8, 9, tzinfo=UTC)


def _bars(days: int = 130, tickers: tuple[str, ...] = ("AAPL", "MSFT")) -> pd.DataFrame:
    """Daily bars plus intraday, including one ticker-date with intraday ONLY.

    The intraday-only case is the gap-fill path in `_to_daily_close`: intraday
    rows supply a close for (ticker, date) pairs that have no 1Day bar. Any
    rescoping must preserve it.

    The span deliberately covers several calendar months. An earlier version of
    this fixture ran 5 days against a 120-day lookback, so four of the five
    chunks were empty and the equivalence test compared one whole-window
    reduction against itself — it could not have failed on a chunk-splitting bug.
    """
    # Aligned with the lookback window under test: _compute_dispersion(2026-08-02)
    # reads from 2026-04-04, so the fixture starts there and every row counts.
    start = FIXTURE_START
    rows: list[dict] = []
    for d in range(days):
        day = start + timedelta(days=d)
        for i, t in enumerate(tickers):
            # AAPL on this day gets intraday only — no 1Day bar.
            gap = t == "AAPL" and d == GAP_DAY_OFFSET
            if not gap:
                rows.append(
                    {
                        "instrument_key": f"equity:{t}",
                        "ts_event": day.replace(hour=20),
                        "timeframe": "1Day",
                        "close": 100.0 + i * 10 + d,
                    }
                )
            for minute in (30, 45):
                rows.append(
                    {
                        "instrument_key": f"equity:{t}",
                        "ts_event": day.replace(hour=15, minute=minute),
                        "timeframe": "1Min",
                        "close": 90.0 + i * 10 + d + minute / 100,
                    }
                )
    return pd.DataFrame(rows)


def _per_day_reader(frame: pd.DataFrame):
    """Serve read_silver by slicing the frame to the requested time_range."""

    def _read(_dataset: str, **kwargs) -> pd.DataFrame:
        tr = kwargs.get("time_range")
        if tr is None:
            return frame.copy()
        lo = pd.to_datetime(tr[0], utc=True)
        hi = pd.to_datetime(tr[1], utc=True)
        ts = pd.to_datetime(frame["ts_event"], utc=True)
        return frame[(ts >= lo) & (ts <= hi)].copy()

    return _read


class TestMarketRegimeDispersionScoping:
    def test_dispersion_reads_in_bounded_chunks(self) -> None:
        """Guards both ways this read has failed.

        Too big: one call over the ~127-day window loaded ~6.4M rows to keep
        ~160k, and OOM-killed the 3 GB poller nightly.

        Too many: a per-day loop was measured at ~90s per call regardless of
        rows returned — `_open_dataset_safe` re-reads every fragment's schema
        because bars mix string and large_string — so 127 days ran ~3.2h against
        an 1800s timeout. Month-sized chunks sit between the two.
        """
        mock_reader = MagicMock()
        mock_reader.read_silver.side_effect = _per_day_reader(_bars())

        pipeline = MarketRegimePipeline(reader=mock_reader)
        # A realistic window: LOOKBACK_DAYS(120) back from the start date.
        pipeline._compute_dispersion(WINDOW_START, WINDOW_END)

        calls = mock_reader.read_silver.call_args_list
        assert len(calls) > 1, "one bulk read — this is the OOM shape"
        assert len(calls) <= 12, f"{len(calls)} reads; per-open cost makes this the timeout shape"
        for call in calls:
            tr = call.kwargs.get("time_range")
            assert tr is not None, "every read must be time-bounded"
            span = pd.to_datetime(tr[1], utc=True) - pd.to_datetime(tr[0], utc=True)
            assert span <= pd.Timedelta(days=31), f"read spans {span}, expected <= 1 month"
            assert call.kwargs.get("instrument_type") == "equity", (
                "unscoped read opens every instrument_type branch of the feed"
            )
            assert call.kwargs.get("prune_by_dt") is True, "dt pruning keeps the scan cheap"

    def test_dispersion_chunks_cover_the_window_without_gaps(self) -> None:
        """Chunk edges must tile the window exactly.

        `chunk_end = next_month - 1s` against an inclusive `ts_event <=` left a
        one-second hole at every month boundary; ts_event is microsecond
        precision, so rows in that hole belong to no chunk and vanish silently.
        """
        mock_reader = MagicMock()
        mock_reader.read_silver.side_effect = _per_day_reader(_bars())
        MarketRegimePipeline(reader=mock_reader)._compute_dispersion(WINDOW_START, WINDOW_END)

        ranges = [c.kwargs["time_range"] for c in mock_reader.read_silver.call_args_list]
        for (_, prev_end), (next_start, _) in zip(ranges, ranges[1:], strict=False):
            gap = pd.to_datetime(next_start, utc=True) - pd.to_datetime(prev_end, utc=True)
            assert gap == pd.Timedelta(microseconds=1), f"{gap} hole between chunks"

    @pytest.mark.parametrize(
        "start",
        [
            pytest.param(WINDOW_START, id="utc-midnight"),
            pytest.param(WINDOW_START.replace(hour=18), id="mid-day"),
            pytest.param(WINDOW_START.replace(hour=9, minute=30), id="market-open-ish"),
        ],
    )
    def test_chunked_dispersion_matches_whole_window(self, start: datetime) -> None:
        """Chunking must be exactly equivalent for any caller-supplied start time.

        `_to_daily_close` buckets on `ts_event.dt.date` in UTC. If a chunk edge is
        not UTC midnight, one UTC day is split across two chunks and reduced
        twice, emitting two rows for the same (instrument_key, date) — which
        `compute_dispersion` reads as an intra-date return. A mid-day start
        produced a 200x error on the boundary date, which the 90-day rolling
        median then smeared across most of the output.
        """
        frame = _bars()
        end = WINDOW_END

        chunked_reader = MagicMock()
        chunked_reader.read_silver.side_effect = _per_day_reader(frame)
        chunked = MarketRegimePipeline(reader=chunked_reader)._compute_dispersion(start, end)

        # Reference: the same reduction applied once over the same window the
        # chunked path covers — bar_start normalised to UTC midnight, as the
        # implementation does, so all three start times must agree.
        bar_start = (start - timedelta(days=LOOKBACK_DAYS)).replace(hour=0, minute=0, second=0, microsecond=0)
        ts = pd.to_datetime(frame["ts_event"], utc=True)
        window = frame[(ts >= pd.to_datetime(bar_start, utc=True)) & (ts <= pd.to_datetime(end, utc=True))]
        reference = MarketRegimePipeline(reader=MagicMock())._to_daily_close(window.copy())
        expected = compute_dispersion(reference)

        assert not chunked.empty, "fixture must actually span multiple chunks"
        pd.testing.assert_frame_equal(
            chunked.sort_values("ts_event").reset_index(drop=True),
            expected.sort_values("ts_event").reset_index(drop=True),
            check_dtype=False,
        )

    def test_intraday_gap_fill_survives_chunking(self) -> None:
        """AAPL has no 1Day bar on one date; its intraday close must still appear.

        Goes through `_compute_dispersion` so the chunked path is exercised — an
        earlier version called `_to_daily_close` directly and so tested code the
        rescoping never touched.
        """
        frame = _bars()
        mock_reader = MagicMock()
        mock_reader.read_silver.side_effect = _per_day_reader(frame)

        pipeline = MarketRegimePipeline(reader=mock_reader)
        dispersion = pipeline._compute_dispersion(WINDOW_START, WINDOW_END)

        gap_day = pd.Timestamp(FIXTURE_START + timedelta(days=GAP_DAY_OFFSET))
        assert gap_day in set(dispersion["ts_event"]), "the intraday-only ticker-date dropped out of the chunked path"


class TestDarkpoolScoping:
    def test_darkpool_reads_are_projected_and_batched(self) -> None:
        """darkpool was the only Gold pipeline with no columns=, batch_size= or chunking."""
        mock_reader = MagicMock()
        mock_reader.read_silver.return_value = pd.DataFrame()

        DarkpoolPipeline(reader=mock_reader).run(start_date="2026-08-03", end_date="2026-08-04", dry_run=True)

        assert mock_reader.read_silver.call_count >= 1
        for call in mock_reader.read_silver.call_args_list:
            cols = call.kwargs.get("columns")
            assert cols, f"read of {call.args[0]!r} has no column projection"
            assert call.kwargs.get("batch_size"), f"read of {call.args[0]!r} is unbatched"

    def test_missing_required_column_fails_loud(self) -> None:
        """HeberReader silently drops projected columns absent from the schema.

        `dp.get("notional", 0)` would then zero every feature instead of failing,
        so a missing required column must raise.
        """
        without_notional = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "underlying": ["AAPL"],
                "ts_event": [datetime(2026, 8, 3, 15, tzinfo=UTC)],
            }
        )
        mock_reader = MagicMock()
        mock_reader.read_silver.return_value = without_notional

        with pytest.raises(ValueError, match="notional"):
            DarkpoolPipeline(reader=mock_reader).run(start_date="2026-08-03", end_date="2026-08-04", dry_run=True)


class TestTimeframePushdown:
    """Dispersion reads only daily bars, and says so to the scanner.

    Reading every timeframe and reducing afterwards materialised ~62x the rows
    needed — 1,986,898 vs 32,214 for a single month, measured — which is what
    pushed the cold-cache run past the 1800s pipeline timeout.
    """

    def test_dispersion_pushes_timeframe_into_the_scan(self) -> None:
        mock_reader = MagicMock()
        mock_reader.read_silver.side_effect = _per_day_reader(_bars())

        MarketRegimePipeline(reader=mock_reader)._compute_dispersion(WINDOW_START, WINDOW_END)

        for call in mock_reader.read_silver.call_args_list:
            assert call.kwargs.get("timeframe") == "1Day", "reduced client-side instead of filtering at the scan"

    def test_only_daily_rows_reach_the_reduction(self) -> None:
        """With the filter honoured, no intraday row may reach _to_daily_close.

        The deliberate consequence is that a ticker-date with intraday bars but
        no 1Day bar contributes no close. That restores the behaviour every Gold
        row written before 2026-06-09 was computed with — the old
        `len(bars) > 1_000_000` branch always fired in production and filtered to
        1Day — so the series stays comparable with its own history.
        """
        frame = _bars()
        mock_reader = MagicMock()
        mock_reader.read_silver.side_effect = _daily_only_reader(frame)

        seen: list[pd.DataFrame] = []
        original = MarketRegimePipeline._to_daily_close

        def _spy(chunk: pd.DataFrame) -> pd.DataFrame:
            seen.append(chunk)
            return original(chunk)

        pipeline = MarketRegimePipeline(reader=mock_reader)
        pipeline._to_daily_close = _spy  # type: ignore[method-assign]
        pipeline._compute_dispersion(WINDOW_START, WINDOW_END)

        assert seen, "no chunk reached the reduction"
        for chunk in seen:
            assert set(chunk["timeframe"]) == {"1Day"}, "intraday rows leaked past the scan filter"

        gap_day = (FIXTURE_START + timedelta(days=GAP_DAY_OFFSET)).date()
        gap_rows = pd.concat(seen)
        gap_rows = gap_rows[pd.to_datetime(gap_rows["ts_event"], utc=True).dt.date == gap_day]
        assert set(gap_rows["instrument_key"]) == {"equity:MSFT"}, (
            "AAPL has no 1Day bar that day and must not be gap-filled from intraday"
        )


def _daily_only_reader(frame: pd.DataFrame):
    """A reader that honours the timeframe filter, as the real scan does."""
    base = _per_day_reader(frame)

    def _read(dataset: str, **kwargs) -> pd.DataFrame:
        out = base(dataset, **kwargs)
        tf = kwargs.get("timeframe")
        if tf is not None and "timeframe" in out.columns:
            out = out[out["timeframe"] == tf]
        return out

    return _read
