"""Unit tests for sector flow feature compute functions.

Tests the standalone compute_sector_flow_features function with synthetic DataFrames.
Does NOT test the SectorFlowPipeline class (that requires a real Heber volume).
"""

from __future__ import annotations

import pandas as pd

from heber.features.pipelines.sector_flow_features import (
    SECTOR_MAP,
    compute_sector_flow_features,
    get_sector,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_alert(
    event_id: str,
    underlying: str,
    ts_event: str | pd.Timestamp,
    put_call: str = "C",
) -> dict:
    """Create a single flow alert row dict."""
    ts = pd.Timestamp(ts_event) if isinstance(ts_event, str) else ts_event
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return {
        "event_id": event_id,
        "underlying": underlying,
        "ts_event": ts,
        "put_call": put_call,
    }


def _make_tide(
    sector: str,
    ts_event: str | pd.Timestamp,
    sentiment: float = 0.5,
    call_put_ratio: float = 1.2,
) -> dict:
    """Create a single sector_tide snapshot row dict."""
    ts = pd.Timestamp(ts_event) if isinstance(ts_event, str) else ts_event
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return {
        "sector": sector,
        "ts_event": ts,
        "sentiment": sentiment,
        "call_put_ratio": call_put_ratio,
        "net_call_premium": 1_000_000.0,
        "net_put_premium": 500_000.0,
        "net_volume": 50_000,
        "instrument_key": f"sector:{sector}",
    }


def _alerts_df(alerts: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(alerts)
    if not df.empty:
        df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    return df


def _tide_df(tides: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(tides)
    if not df.empty:
        df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    return df


# ===========================================================================
# test_empty_input
# ===========================================================================


class TestEmptyInput:
    """Empty input should produce empty output with correct columns."""

    def test_empty_alerts(self) -> None:
        result = compute_sector_flow_features(pd.DataFrame(), pd.DataFrame())
        assert result.empty

    def test_empty_has_expected_columns(self) -> None:
        result = compute_sector_flow_features(pd.DataFrame(), pd.DataFrame())
        expected_cols = {
            "alert_id",
            "instrument_key",
            "ts_event",
            "ts_available",
            "sector_flow_alignment",
            "sector_call_put_ratio",
        }
        assert expected_cols == set(result.columns)


# ===========================================================================
# test_bullish_alert_positive_sector
# ===========================================================================


class TestBullishAlertPositiveSector:
    """A CALL alert in a sector with positive sentiment should align +1.0."""

    def test_alignment_positive(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 10:00:00", "C"),
            ]
        )
        tide = _tide_df(
            [
                _make_tide("Information Technology", "2026-01-15 09:30:00", sentiment=0.8, call_put_ratio=1.5),
            ]
        )
        result = compute_sector_flow_features(alerts, tide)

        assert len(result) == 1
        row = result.iloc[0]
        assert row["sector_flow_alignment"] == 1.0
        assert row["sector_call_put_ratio"] == 1.5


# ===========================================================================
# test_bearish_alert_negative_sector
# ===========================================================================


class TestBearishAlertNegativeSector:
    """A PUT alert in a sector with negative sentiment should also align +1.0 (agreement)."""

    def test_alignment_positive(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 10:00:00", "P"),
            ]
        )
        tide = _tide_df(
            [
                _make_tide("Information Technology", "2026-01-15 09:30:00", sentiment=-0.5, call_put_ratio=0.6),
            ]
        )
        result = compute_sector_flow_features(alerts, tide)

        assert len(result) == 1
        assert result.iloc[0]["sector_flow_alignment"] == 1.0


# ===========================================================================
# test_bullish_alert_negative_sector
# ===========================================================================


class TestBullishAlertNegativeSector:
    """A CALL alert in a sector with negative sentiment should give -1.0 (disagreement)."""

    def test_alignment_negative(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "MSFT", "2026-01-15 10:00:00", "C"),
            ]
        )
        tide = _tide_df(
            [
                _make_tide("Information Technology", "2026-01-15 09:30:00", sentiment=-0.3, call_put_ratio=0.8),
            ]
        )
        result = compute_sector_flow_features(alerts, tide)

        assert len(result) == 1
        assert result.iloc[0]["sector_flow_alignment"] == -1.0


# ===========================================================================
# test_neutral_sector_sentiment
# ===========================================================================


class TestNeutralSectorSentiment:
    """A zero-sentiment sector should give alignment 0.0 regardless of alert direction."""

    def test_neutral_call(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 10:00:00", "C"),
            ]
        )
        tide = _tide_df(
            [
                _make_tide("Information Technology", "2026-01-15 09:30:00", sentiment=0.0, call_put_ratio=1.0),
            ]
        )
        result = compute_sector_flow_features(alerts, tide)

        assert result.iloc[0]["sector_flow_alignment"] == 0.0

    def test_neutral_put(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 10:00:00", "P"),
            ]
        )
        tide = _tide_df(
            [
                _make_tide("Information Technology", "2026-01-15 09:30:00", sentiment=0.0, call_put_ratio=1.0),
            ]
        )
        result = compute_sector_flow_features(alerts, tide)

        assert result.iloc[0]["sector_flow_alignment"] == 0.0


# ===========================================================================
# test_no_sector_data
# ===========================================================================


class TestNoSectorData:
    """When no sector_tide data exists, all alignments should be neutral."""

    def test_empty_tide_gives_neutral(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 10:00:00", "C"),
                _make_alert("a2", "TSLA", "2026-01-15 11:00:00", "P"),
            ]
        )
        result = compute_sector_flow_features(alerts, pd.DataFrame())

        assert len(result) == 2
        assert (result["sector_flow_alignment"] == 0.0).all()
        assert result["sector_call_put_ratio"].isna().all()


# ===========================================================================
# test_unknown_ticker_sector
# ===========================================================================


class TestUnknownTickerSector:
    """Tickers not in SECTOR_MAP should get 'Unknown' sector and neutral alignment."""

    def test_unknown_ticker(self) -> None:
        assert get_sector("XYZNOTREAL") == "Unknown"

        alerts = _alerts_df(
            [
                _make_alert("a1", "XYZNOTREAL", "2026-01-15 10:00:00", "C"),
            ]
        )
        tide = _tide_df(
            [
                _make_tide("Information Technology", "2026-01-15 09:30:00", sentiment=0.9),
            ]
        )
        result = compute_sector_flow_features(alerts, tide)

        assert len(result) == 1
        # Unknown sector maps to neutral alignment
        assert result.iloc[0]["sector_flow_alignment"] == 0.0


# ===========================================================================
# test_most_recent_tide_used
# ===========================================================================


class TestMostRecentTideUsed:
    """Should use the most recent sector_tide snapshot BEFORE the alert, not after."""

    def test_uses_most_recent_before(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 12:00:00", "C"),
            ]
        )
        tide = _tide_df(
            [
                # Earlier snapshot: positive sentiment
                _make_tide("Information Technology", "2026-01-15 09:00:00", sentiment=0.5, call_put_ratio=1.2),
                # Later snapshot but still before alert: negative sentiment
                _make_tide("Information Technology", "2026-01-15 11:00:00", sentiment=-0.3, call_put_ratio=0.7),
                # After alert: should NOT be used
                _make_tide("Information Technology", "2026-01-15 13:00:00", sentiment=0.9, call_put_ratio=2.0),
            ]
        )
        result = compute_sector_flow_features(alerts, tide)

        row = result.iloc[0]
        # Should use the 11:00 snapshot (negative sentiment), CALL alert => -1.0
        assert row["sector_flow_alignment"] == -1.0
        assert row["sector_call_put_ratio"] == 0.7

    def test_no_tide_before_alert(self) -> None:
        """If all sector_tide snapshots are after the alert, no match is found."""
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 08:00:00", "C"),
            ]
        )
        tide = _tide_df(
            [
                _make_tide("Information Technology", "2026-01-15 09:00:00", sentiment=0.5),
            ]
        )
        result = compute_sector_flow_features(alerts, tide)

        # No sector data before alert => neutral
        assert result.iloc[0]["sector_flow_alignment"] == 0.0


# ===========================================================================
# test_multi_sector_independence
# ===========================================================================


class TestMultiSectorIndependence:
    """Alerts in different sectors should use their respective sector data."""

    def test_independent_sectors(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 10:00:00", "C"),  # Tech
                _make_alert("a2", "JPM", "2026-01-15 10:00:00", "C"),  # Financials
            ]
        )
        tide = _tide_df(
            [
                _make_tide("Information Technology", "2026-01-15 09:30:00", sentiment=0.8),
                _make_tide("Financials", "2026-01-15 09:30:00", sentiment=-0.5),
            ]
        )
        result = compute_sector_flow_features(alerts, tide)

        row_aapl = result[result["alert_id"] == "a1"].iloc[0]
        row_jpm = result[result["alert_id"] == "a2"].iloc[0]

        # AAPL (Tech): CALL + positive => +1.0
        assert row_aapl["sector_flow_alignment"] == 1.0
        # JPM (Financials): CALL + negative => -1.0
        assert row_jpm["sector_flow_alignment"] == -1.0


# ===========================================================================
# test_output_columns
# ===========================================================================


class TestOutputColumns:
    """Verify output DataFrame has all required columns with correct types."""

    def test_all_columns_present(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 10:00:00", "C"),
            ]
        )
        tide = _tide_df(
            [
                _make_tide("Information Technology", "2026-01-15 09:30:00"),
            ]
        )
        result = compute_sector_flow_features(alerts, tide)

        expected_cols = {
            "alert_id",
            "instrument_key",
            "ts_event",
            "ts_available",
            "sector_flow_alignment",
            "sector_call_put_ratio",
        }
        assert expected_cols == set(result.columns)

    def test_ts_event_is_datetime(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 10:00:00", "C"),
            ]
        )
        tide = _tide_df(
            [
                _make_tide("Information Technology", "2026-01-15 09:30:00"),
            ]
        )
        result = compute_sector_flow_features(alerts, tide)
        assert pd.api.types.is_datetime64_any_dtype(result["ts_event"])

    def test_alignment_is_float(self) -> None:
        alerts = _alerts_df(
            [
                _make_alert("a1", "AAPL", "2026-01-15 10:00:00", "C"),
            ]
        )
        tide = _tide_df(
            [
                _make_tide("Information Technology", "2026-01-15 09:30:00"),
            ]
        )
        result = compute_sector_flow_features(alerts, tide)
        assert pd.api.types.is_float_dtype(result["sector_flow_alignment"])


# ===========================================================================
# test_sector_map_coverage
# ===========================================================================


class TestSectorMapCoverage:
    """Verify the SECTOR_MAP covers expected tickers."""

    def test_major_tickers_mapped(self) -> None:
        for ticker in ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "JPM", "XOM"]:
            assert ticker in SECTOR_MAP, f"{ticker} missing from SECTOR_MAP"

    def test_get_sector_returns_unknown_for_missing(self) -> None:
        assert get_sector("NOTAREAL_TICKER") == "Unknown"


# ===========================================================================
# test_many_alerts_performance
# ===========================================================================


class TestManyAlertsPerformance:
    """Verify the function handles many alerts without error."""

    def test_500_alerts(self) -> None:
        n_alerts = 500
        alerts = []
        tickers = ["AAPL", "MSFT", "GOOGL", "JPM", "XOM"]
        base = pd.Timestamp("2026-01-15 09:30:00", tz="UTC")
        for i in range(n_alerts):
            ticker = tickers[i % len(tickers)]
            alerts.append(
                _make_alert(
                    f"a{i}",
                    ticker,
                    base + pd.Timedelta(minutes=i),
                    "C" if i % 2 == 0 else "P",
                )
            )

        tide_rows = []
        for sector in [
            "Information Technology",
            "Communication Services",
            "Consumer Discretionary",
            "Financials",
            "Energy",
        ]:
            tide_rows.append(_make_tide(sector, "2026-01-15 09:00:00", sentiment=0.5))

        result = compute_sector_flow_features(_alerts_df(alerts), _tide_df(tide_rows))
        assert len(result) == n_alerts
