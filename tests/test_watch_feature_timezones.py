from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from heber.models.silver import FlowAlertRecord
from heber.watch.features import AlertFeatureExtractor


def _build_alert(ts_event: datetime) -> FlowAlertRecord:
    return FlowAlertRecord(
        event_id="evt-1",
        provider="unusual_whales",
        feed="flow_alerts",
        instrument_type="option",
        instrument_key="option:OCC:AAPL260220C00100000",
        symbol="AAPL 2026-02-20 C100",
        ts_event=ts_event,
        ts_ingest=datetime(2026, 1, 5, 15, 0, tzinfo=UTC),
        ts_available=datetime(2026, 1, 5, 15, 0, tzinfo=UTC),
        source="websocket",
        underlying="AAPL",
        occ_symbol="AAPL260220C00100000",
        expiry=date(2026, 2, 20),
        strike=100.0,
        put_call="C",
        premium=12500.0,
        volume=100.0,
        open_interest=200.0,
        spot_px=195.0,
        contract_px=1.25,
        alert_type="SWEEP",
        side="ask",
        aggressor="ask",
        tags=["bullish", "sweep"],
    )


@pytest.mark.asyncio
async def test_extract_uses_market_timezone_for_timing_features() -> None:
    extractor = AlertFeatureExtractor(gateway_url=None)
    alert = _build_alert(datetime(2026, 1, 5, 15, 0, tzinfo=UTC))  # 10:00 ET

    features = await extractor.extract(alert)

    assert features.hour_of_day == 10
    assert features.minute_of_hour == 0
    assert features.day_of_week == 0  # Monday
    assert features.minutes_since_open == 30
    assert features.minutes_to_close == 360


@pytest.mark.asyncio
async def test_extract_treats_naive_alert_time_as_utc() -> None:
    extractor = AlertFeatureExtractor(gateway_url=None)
    aware_alert = _build_alert(datetime(2026, 1, 5, 15, 0, tzinfo=UTC))
    naive_alert = _build_alert(datetime(2026, 1, 5, 15, 0))

    aware_features = await extractor.extract(aware_alert)
    naive_features = await extractor.extract(naive_alert)

    assert naive_features.hour_of_day == aware_features.hour_of_day
    assert naive_features.minute_of_hour == aware_features.minute_of_hour
    assert naive_features.day_of_week == aware_features.day_of_week
    assert naive_features.minutes_since_open == aware_features.minutes_since_open
    assert naive_features.minutes_to_close == aware_features.minutes_to_close
