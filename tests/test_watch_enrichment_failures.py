"""Enrichment-failure observability on AlertFeatures (audit F-14/F-15, W5).

Each per-step enrichment body logs-and-continues on a payload-shape error.
These tests prove the failure is now also recorded on
``AlertFeatures.enrichment_failures`` so callers can tell a fully-enriched
row from a partially-enriched one, and that the field survives the
to_dict/from_dict round-trip while staying out of the Gold schema.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pandas as pd
import pytest

from heber.models.silver import FlowAlertRecord
from heber.watch.features import (
    AlertFeatureExtractor,
    AlertFeatures,
    _backfill_gex_vex,
    _backfill_market_tide,
    persist_features_to_gold,
)


def _base_features(symbol: str = "AAPL") -> AlertFeatures:
    return AlertFeatures(
        alert_id="a1",
        alert_time=datetime(2026, 2, 7, 15, 0, tzinfo=UTC),
        symbol=symbol,
        occ_symbol=f"{symbol}260220C00100000",
        underlying=symbol,
        strike=100.0,
        expiry=date(2026, 2, 20),
        put_call="C",
        days_to_expiry=13,
        premium=12500.0,
        volume=100.0,
        open_interest=200.0,
        volume_oi_ratio=0.5,
        alert_type="SWEEP",
        side="ask",
        aggressor="ask",
        spot_price=195.0,
        contract_price=1.25,
    )


def _make_extractor() -> AlertFeatureExtractor:
    return AlertFeatureExtractor(
        gateway_url="http://gateway:8000",
        request_max_attempts=1,
        retry_base_delay_seconds=0.0,
        retry_jitter_seconds=0.0,
        # These tests use a fixed past alert timestamp to exercise per-step
        # failure recording. The freshness gate would otherwise skip the
        # live-only steps before they can fail; it is covered on its own in
        # test_enrichment_freshness_gate.py.
        live_enrichment_max_age=None,
    )


# --- AlertFeatures.enrichment_failures default + serialization ---


def test_enrichment_failures_defaults_to_empty_list() -> None:
    features = _base_features()
    assert features.enrichment_failures == []


def test_each_alert_features_gets_its_own_failure_list() -> None:
    a = _base_features()
    b = _base_features()
    a.enrichment_failures.append("GEX")
    assert b.enrichment_failures == []


def test_to_dict_includes_enrichment_failures() -> None:
    features = _base_features()
    features.enrichment_failures.append("market tide")
    assert features.to_dict()["enrichment_failures"] == ["market tide"]


def test_from_dict_roundtrips_enrichment_failures() -> None:
    features = _base_features()
    features.enrichment_failures.extend(["GEX", "Greeks"])
    restored = AlertFeatures.from_dict(features.to_dict())
    assert restored.enrichment_failures == ["GEX", "Greeks"]


def test_from_dict_without_enrichment_failures_key_defaults_empty() -> None:
    payload = _base_features().to_dict()
    payload.pop("enrichment_failures")
    restored = AlertFeatures.from_dict(payload)
    assert restored.enrichment_failures == []


# --- Per-step failure recording ---


@pytest.mark.parametrize(
    ("method_name", "step_name"),
    [
        ("_enrich_iv_rank", "IV rank"),
        ("_enrich_gex", "GEX"),
        ("_enrich_max_pain", "max pain"),
        ("_enrich_market_tide", "market tide"),
        ("_enrich_greeks", "Greeks"),
        ("_enrich_market_context", "market context"),
    ],
)
@pytest.mark.asyncio
async def test_payload_error_records_named_step(
    monkeypatch: pytest.MonkeyPatch, method_name: str, step_name: str
) -> None:
    extractor = _make_extractor()

    async def _raise(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        raise ValueError("malformed payload")

    monkeypatch.setattr(extractor, "_request_json_with_retry", _raise)

    features = _base_features()
    method = getattr(extractor, method_name)
    enriched = await method(features)

    assert enriched.enrichment_failures == [step_name]


@pytest.mark.asyncio
async def test_successful_step_records_no_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    extractor = _make_extractor()

    async def _ok(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        return {"data": {"net_premium": 1_000_000.0}}

    monkeypatch.setattr(extractor, "_request_json_with_retry", _ok)

    enriched = await extractor._enrich_market_tide(_base_features())

    assert enriched.market_tide_direction == "bullish"
    assert enriched.enrichment_failures == []


@pytest.mark.asyncio
async def test_skipped_step_no_gateway_records_no_failure() -> None:
    extractor = AlertFeatureExtractor(gateway_url=None)
    enriched = await extractor._enrich_gex(_base_features())
    assert enriched.enrichment_failures == []


# --- extract() aggregates failures across steps ---


def _build_alert() -> FlowAlertRecord:
    return FlowAlertRecord(
        event_id="evt-1",
        provider="unusual_whales",
        feed="flow_alerts",
        instrument_type="option",
        instrument_key="option:OCC:AAPL260220C00100000",
        symbol="AAPL 2026-02-20 C100",
        ts_event=datetime(2026, 2, 9, 15, 0, tzinfo=UTC),
        ts_ingest=datetime(2026, 2, 9, 15, 0, tzinfo=UTC),
        ts_available=datetime(2026, 2, 9, 15, 0, tzinfo=UTC),
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
async def test_extract_records_every_failing_step_in_order(monkeypatch: pytest.MonkeyPatch) -> None:
    extractor = _make_extractor()

    async def _raise(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        raise ValueError("malformed payload")

    monkeypatch.setattr(extractor, "_request_json_with_retry", _raise)

    features = await extractor.extract(_build_alert())

    # Every step that performs an upstream request failed; order matches the
    # enrichment table in extract().
    assert features.enrichment_failures == [
        "market context",
        "Greeks",
        "IV rank",
        "GEX",
        "max pain",
        "market tide",
    ]


@pytest.mark.asyncio
async def test_extract_clean_run_has_no_failures() -> None:
    # No gateway URL -> every step skips cleanly, no failures recorded.
    extractor = AlertFeatureExtractor(gateway_url=None)
    features = await extractor.extract(_build_alert())
    assert features.enrichment_failures == []


# --- enrichment_failures stays out of the Gold schema ---


def test_persist_to_gold_excludes_enrichment_failures(tmp_path) -> None:  # noqa: ANN001
    features = _base_features()
    features.enrichment_failures.append("GEX")
    # Populate Greeks so the row clears the fail-loud all-null-Greek guard.
    features.delta = 0.5
    features.gamma = 0.02
    features.theta = -0.05
    features.vega = 0.1
    features.iv = 0.3

    persist_features_to_gold(features, output_path=tmp_path)

    out_file = tmp_path / "dt=2026-02-07" / "data.parquet"
    df = pd.read_parquet(out_file)
    assert "enrichment_failures" not in df.columns


# --- backfill helpers are split and independently callable ---


def test_backfill_helpers_noop_when_nothing_to_fill() -> None:
    # Both columns already populated -> early return, reader never touched.
    df = pd.DataFrame(
        {
            "symbol": ["AAPL"],
            "alert_time": [pd.Timestamp("2026-02-07T15:00:00Z")],
            "gex": [1.0],
            "vex": [2.0],
            "market_tide_net_premium": [3.0],
            "market_tide_direction": ["bullish"],
        }
    )

    sentinel = object()
    out_gex = _backfill_gex_vex(df, sentinel, "1h")  # type: ignore[arg-type]
    out_tide = _backfill_market_tide(out_gex, sentinel, "1h")  # type: ignore[arg-type]

    assert out_tide["gex"].tolist() == [1.0]
    assert out_tide["market_tide_net_premium"].tolist() == [3.0]
