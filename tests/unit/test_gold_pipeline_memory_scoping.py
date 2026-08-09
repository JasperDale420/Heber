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
from heber.features.pipelines.market_regime_features import MarketRegimePipeline


def _bars(days: int = 5, tickers: tuple[str, ...] = ("AAPL", "MSFT")) -> pd.DataFrame:
    """Daily bars plus intraday, including one ticker-date with intraday ONLY.

    The intraday-only case is the gap-fill path in `_to_daily_close`: intraday
    rows supply a close for (ticker, date) pairs that have no 1Day bar. Any
    rescoping must preserve it.
    """
    start = datetime(2026, 3, 2, tzinfo=UTC)
    rows: list[dict] = []
    for d in range(days):
        day = start + timedelta(days=d)
        for i, t in enumerate(tickers):
            # AAPL on the third day gets intraday only — no 1Day bar.
            gap = t == "AAPL" and d == 2
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
        pipeline._compute_dispersion(datetime(2026, 8, 2, tzinfo=UTC), datetime(2026, 8, 9, tzinfo=UTC))

        calls = mock_reader.read_silver.call_args_list
        assert len(calls) > 1, "one bulk read — this is the OOM shape"
        assert len(calls) <= 12, f"{len(calls)} reads; per-open cost makes this the timeout shape"
        for call in calls:
            tr = call.kwargs.get("time_range")
            assert tr is not None, "every read must be time-bounded"
            span = pd.to_datetime(tr[1], utc=True) - pd.to_datetime(tr[0], utc=True)
            assert span <= pd.Timedelta(days=31), f"read spans {span}, expected <= 1 month"

    def test_chunked_dispersion_matches_whole_window(self) -> None:
        """Day-chunking must be exactly equivalent, not merely close.

        `_to_daily_close` groups by (instrument_key, date) with no cross-day
        dependency, so processing per day and concatenating is identical to
        processing the whole frame at once — including the intraday gap-fill.
        """
        frame = _bars()
        start, end = datetime(2026, 3, 2, tzinfo=UTC), datetime(2026, 3, 6, 23, 59, tzinfo=UTC)

        chunked_reader = MagicMock()
        chunked_reader.read_silver.side_effect = _per_day_reader(frame)
        chunked = MarketRegimePipeline(reader=chunked_reader)._compute_dispersion(start, end)

        # Reference: the whole window in one read, through the same code path.
        bulk_reader = MagicMock()
        bulk_reader.read_silver.return_value = frame.copy()
        reference = MarketRegimePipeline(reader=bulk_reader)._to_daily_close(frame.copy())

        from heber.features.pipelines.market_regime_features import compute_dispersion

        expected = compute_dispersion(reference)

        pd.testing.assert_frame_equal(
            chunked.sort_values("ts_event").reset_index(drop=True),
            expected.sort_values("ts_event").reset_index(drop=True),
            check_dtype=False,
        )

    def test_intraday_gap_fill_survives_rescoping(self) -> None:
        """AAPL has no 1Day bar on day 3; its intraday close must still appear."""
        frame = _bars()
        mock_reader = MagicMock()
        mock_reader.read_silver.side_effect = _per_day_reader(frame)

        daily = MarketRegimePipeline(reader=mock_reader)._to_daily_close(frame.copy())
        gap_day = pd.Timestamp("2026-03-04", tz="UTC")
        rows = daily[(daily["instrument_key"] == "equity:AAPL") & (daily["ts_event"] == gap_day)]
        assert len(rows) == 1, "intraday gap-fill lost — that ticker-date would vanish"


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
