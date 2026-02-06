"""Regression tests for alert label pipeline instrument-key handling."""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from heber.features.pipelines.alert_labels import AlertLabelsPipeline


class _StubClient:
    def __init__(self, response_df: pd.DataFrame) -> None:
        self._response_df = response_df
        self.calls: list[dict] = []

    def read_silver(self, **kwargs) -> pd.DataFrame:  # noqa: ANN003
        self.calls.append(kwargs)
        return self._response_df.copy()


def _window() -> tuple[datetime, datetime]:
    return (
        datetime(2026, 1, 1, tzinfo=UTC),
        datetime(2026, 1, 2, tzinfo=UTC),
    )


def test_load_bars_expands_raw_and_canonical_keys() -> None:
    client = _StubClient(
        pd.DataFrame(
            {
                "instrument_key": ["AAPL", "equity:SPY"],
                "timeframe": ["1Day", "1Day"],
            }
        )
    )
    pipeline = AlertLabelsPipeline(client=client)
    start, end = _window()

    bars = pipeline._load_bars(["equity:AAPL", "equity:SPY"], start, end)

    read_call = client.calls[0]
    assert read_call["dataset"] == "bars"
    assert "AAPL" in read_call["instrument_keys"]
    assert "equity:AAPL" in read_call["instrument_keys"]
    assert set(bars["instrument_key"]) == {"equity:AAPL", "equity:SPY"}


def test_load_intraday_bars_reads_bars_dataset_and_filters_5min() -> None:
    client = _StubClient(
        pd.DataFrame(
            {
                "instrument_key": ["AAPL", "AAPL"],
                "timeframe": ["5Min", "1Day"],
            }
        )
    )
    pipeline = AlertLabelsPipeline(client=client)
    start, end = _window()

    bars = pipeline._load_intraday_bars(["equity:AAPL"], start, end)

    read_call = client.calls[0]
    assert read_call["dataset"] == "bars"
    assert len(bars) == 1
    assert bars.iloc[0]["timeframe"] == "5Min"
    assert bars.iloc[0]["instrument_key"] == "equity:AAPL"


def test_load_intraday_bars_falls_back_to_daily_when_5min_missing(monkeypatch) -> None:
    client = _StubClient(
        pd.DataFrame(
            {
                "instrument_key": ["AAPL"],
                "timeframe": ["1Day"],
            }
        )
    )
    pipeline = AlertLabelsPipeline(client=client)
    start, end = _window()
    fallback = pd.DataFrame({"instrument_key": ["equity:AAPL"], "timeframe": ["1Day"]})

    monkeypatch.setattr(pipeline, "_load_bars", lambda symbols, s, e: fallback)  # noqa: ARG005

    bars = pipeline._load_intraday_bars(["equity:AAPL"], start, end)

    assert bars.equals(fallback)


def test_normalize_alert_underlyings_converts_raw_symbols() -> None:
    alerts = pd.DataFrame({"underlying": ["AAPL", "equity:MSFT", None]})

    normalized = AlertLabelsPipeline._normalize_alert_underlyings(alerts)

    assert normalized.iloc[0]["underlying"] == "equity:AAPL"
    assert normalized.iloc[1]["underlying"] == "equity:MSFT"
    assert pd.isna(normalized.iloc[2]["underlying"])
