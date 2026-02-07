from __future__ import annotations

import json
from datetime import UTC, date, datetime

import pyarrow as pa

from heber.models.silver import (
    AnalystRatingRecord,
    CongressTradeRecord,
    ETFMetadataRecord,
    FlowAlertRecord,
    HottestChainRecord,
    IVTermStructureRecord,
    MaxPainRecord,
    NewsArticleRecord,
)
from heber.schemas.silver import SILVER_SCHEMAS


def _base_kwargs() -> dict[str, object]:
    now = datetime(2026, 2, 7, 14, 30, tzinfo=UTC)
    return {
        "event_id": "evt-1",
        "provider": "unusual_whales",
        "feed": "flow_alerts",
        "instrument_type": "option",
        "instrument_key": "option:AAPL250321C00200000",
        "symbol": "AAPL",
        "ts_event": now,
        "ts_ingest": now,
        "ts_available": now,
        "source": "websocket",
    }


def test_lineage_dict_is_normalized_to_json_string() -> None:
    record = FlowAlertRecord(
        **_base_kwargs(),
        underlying="AAPL",
        occ_symbol="AAPL250321C00200000",
        expiry=date(2026, 3, 21),
        strike=200.0,
        put_call="C",
        premium=125000.0,
        volume=42.0,
        alert_type="SWEEP",
        lineage={"trace_id": "abc", "seq": 7},
    )

    assert isinstance(record.lineage, str)
    assert json.loads(record.lineage) == {"seq": 7, "trace_id": "abc"}


def test_schema_version_defaults_follow_release_cohorts() -> None:
    v1_record = FlowAlertRecord(
        **_base_kwargs(),
        underlying="AAPL",
        occ_symbol="AAPL250321C00200000",
        expiry=date(2026, 3, 21),
        strike=200.0,
        put_call="C",
        premium=125000.0,
        volume=42.0,
        alert_type="SWEEP",
    )
    v2_kwargs = {
        **_base_kwargs(),
        "feed": "news_articles",
        "instrument_type": "equity",
        "instrument_key": "equity:AAPL",
    }
    v2_record = NewsArticleRecord(
        **v2_kwargs,
        news_id="news-1",
        ts_published=datetime(2026, 2, 7, 14, 0, tzinfo=UTC),
        headline="Headline",
        url="https://example.com/article",
    )
    v3_kwargs = {
        **_base_kwargs(),
        "feed": "congress_trades",
        "instrument_type": "equity",
        "instrument_key": "equity:AAPL",
    }
    v3_record = CongressTradeRecord(
        **v3_kwargs,
        trade_id="trade-1",
        politician_name="Jane Doe",
        trade_type="buy",
        trade_date=date(2026, 2, 1),
    )
    v4_kwargs = {
        **_base_kwargs(),
        "feed": "analyst_ratings",
        "instrument_type": "equity",
        "instrument_key": "equity:AAPL",
    }
    v4_record = AnalystRatingRecord(
        **v4_kwargs,
        rating_id="rating-1",
        rating="Buy",
        rating_date=date(2026, 2, 7),
    )
    v6_kwargs = {
        **_base_kwargs(),
        "feed": "etf_metadata",
        "instrument_type": "equity",
        "instrument_key": "equity:SPY",
    }
    v6_record = ETFMetadataRecord(
        **v6_kwargs,
        etf_symbol="SPY",
        snapshot_date=date(2026, 2, 7),
    )

    assert v1_record.schema_version == "v1"
    assert v2_record.schema_version == "v2"
    assert v3_record.schema_version == "v3"
    assert v4_record.schema_version == "v4"
    assert v6_record.schema_version == "v6"


def test_expiry_fields_use_date_type_consistently() -> None:
    max_pain = MaxPainRecord(**_base_kwargs(), expiry="2026-03-21", max_pain_strike=200.0)
    hottest_chain = HottestChainRecord(
        **_base_kwargs(),
        contract_symbol="AAPL250321C00200000",
        underlying="AAPL",
        strike=200.0,
        expiry="2026-03-21",
        option_type="call",
        volume=10,
        open_interest=100,
        premium=5000.0,
    )
    iv_term = IVTermStructureRecord(
        **_base_kwargs(),
        expiry="2026-03-21",
        iv=0.35,
        days_to_expiry=42,
    )

    assert isinstance(max_pain.expiry, date)
    assert isinstance(hottest_chain.expiry, date)
    assert isinstance(iv_term.expiry, date)


def test_canonical_schemas_use_date32_for_expiry_fields() -> None:
    assert SILVER_SCHEMAS["max_pain"].field("expiry").type == pa.date32()
    assert SILVER_SCHEMAS["hottest_chain"].field("expiry").type == pa.date32()
    assert SILVER_SCHEMAS["iv_term_structure"].field("expiry").type == pa.date32()
