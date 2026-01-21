"""Silver layer schema definitions per PRD §8.7.

All Silver datasets include the shared base columns plus dataset-specific fields.
These Pydantic models are used for validation and documentation.
"""

from datetime import date, datetime
from typing import Any

import pyarrow as pa
from pydantic import BaseModel, Field

# ==============================================================================
# Shared Base Columns (PRD §8.7.1)
# ==============================================================================


class SilverBase(BaseModel):
    """Base columns present in EVERY Silver dataset (PRD §8.7.1)."""

    event_id: str = Field(..., description="Deterministic idempotency key (SHA256)")
    provider: str = Field(..., description="alpaca, unusual_whales, etc")
    feed: str = Field(..., description="Canonical feed name")
    instrument_type: str = Field(..., description="equity|option|crypto|forex")
    instrument_key: str = Field(..., description="Stable canonical instrument key")
    symbol: str = Field(..., description="Human-friendly symbol")
    ts_event: datetime = Field(..., description="Provider event timestamp")
    ts_ingest: datetime = Field(..., description="Gateway receive timestamp")
    ts_available: datetime = Field(..., description="Earliest safe-use timestamp (anti-leakage)")
    source: str = Field(..., description="websocket|rest")
    schema_version: str = Field(default="v1", description="Dataset schema version")
    quality_flags: list[str] = Field(default_factory=list, description="validated, deduped, late")
    lineage: dict[str, Any] | None = Field(default=None, description="Correlation metadata")


# ==============================================================================
# Market Data Schemas (PRD §8.7.2-8.7.4)
# ==============================================================================


class BarRecord(SilverBase):
    """Silver bars schema (PRD §8.7.2).

    Primary key: (instrument_key, timeframe, bar_start_ts)
    """

    timeframe: str = Field(..., description="1Min, 5Min, 1Hour, etc")
    bar_start_ts: datetime = Field(..., description="Bar start/open time")
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: int | None = None
    vwap: float | None = None


class QuoteRecord(SilverBase):
    """Silver quotes schema (PRD §8.7.3).

    Primary key: (instrument_key, ts_event)
    """

    bid_px: float
    bid_sz: float
    ask_px: float
    ask_sz: float
    bid_exchange: str | None = None
    ask_exchange: str | None = None
    conditions: list[str] | None = None


class TradeRecord(SilverBase):
    """Silver trades schema (PRD §8.7.4).

    Primary key: (instrument_key, ts_event, trade_id)
    """

    price: float
    size: float
    trade_id: str | None = None
    exchange: str | None = None
    conditions: list[str] | None = None
    tape: str | None = None


# ==============================================================================
# Alternative Data Schemas (PRD §8.7.5-8.7.6)
# ==============================================================================


class FlowAlertRecord(SilverBase):
    """Silver flow_alerts schema (PRD §8.7.5, Unusual Whales).

    Primary key: event_id
    """

    underlying: str
    occ_symbol: str | None = None
    expiry: date
    strike: float
    put_call: str = Field(..., description="P or C")
    premium: float
    volume: float
    open_interest: float | None = None
    spot_px: float | None = None
    contract_px: float | None = None
    alert_type: str = Field(..., description="SWEEP, BLOCK, etc")
    side: str | None = None
    aggressor: str | None = None
    tags: list[str] | None = None


class DarkpoolTradeRecord(SilverBase):
    """Silver darkpool_trades schema (PRD §8.7.6, Unusual Whales).

    Primary key: event_id
    """

    underlying: str
    price: float
    size: float
    notional: float | None = None
    venue: str | None = None
    print_id: str | None = None
    conditions: list[str] | None = None


# ==============================================================================
# Reference Data Schemas (PRD §8.7.7)
# ==============================================================================


class OptionContractRecord(SilverBase):
    """Silver option_contracts schema (PRD §8.7.7, reference table).

    Primary key: (occ_symbol) or (underlying, expiry, strike, put_call)
    This is a reference table for options consistency.
    """

    underlying: str
    occ_symbol: str
    expiry: date
    strike: float
    put_call: str = Field(..., description="P or C")
    multiplier: int = Field(default=100)
    style: str | None = Field(default=None, description="american or european")
    exchange: str | None = None
    # SCD fields for validity windows (PRD §10.6)
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    revision_id: str | None = None


# ==============================================================================
# V1.5 Schemas (PRD §8.7.8) - Near-term
# ==============================================================================


class GreeksRecord(SilverBase):
    """Silver greeks schema (PRD §8.7.8, time-series).

    Primary key: (instrument_key, ts_event)
    Time-series Greeks data per option contract.
    """

    # Option identification
    underlying: str
    occ_symbol: str
    expiry: date
    strike: float
    put_call: str = Field(..., description="P or C")

    # Greeks values
    iv: float = Field(..., description="Implied volatility")
    delta: float
    gamma: float
    theta: float
    vega: float
    rho: float | None = None

    # Additional context
    underlying_price: float | None = Field(None, description="Spot price at calculation time")
    bid_iv: float | None = None
    ask_iv: float | None = None
    mid_iv: float | None = None


class ChainSnapshotRecord(SilverBase):
    """Silver option_chain_snapshots schema (PRD §8.7.8, snapshot stream).

    One row per contract per snapshot. Snapshot cadence is typically 5-15 minutes.
    Primary key: (snapshot_id, instrument_key) or (underlying, snapshot_id, occ_symbol)
    """

    # Snapshot identification
    snapshot_id: str = Field(..., description="Unique ID for this snapshot")
    underlying: str

    # Contract identification
    occ_symbol: str
    expiry: date
    strike: float
    put_call: str = Field(..., description="P or C")

    # Snapshot data
    bid_px: float | None = None
    ask_px: float | None = None
    mid_px: float | None = None
    last_px: float | None = None
    bid_sz: float | None = None
    ask_sz: float | None = None
    volume: float | None = None
    open_interest: float | None = None

    # Greeks at snapshot time (optional)
    iv: float | None = None
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None

    # Underlying context
    underlying_price: float | None = None


class MarketTideRecord(SilverBase):
    """Silver market_tide schema (PRD §8.7.8, UW periodic snapshot).

    Primary key: (ts_event) or (snapshot_id)
    Periodic market sentiment/flow snapshot from Unusual Whales.
    """

    snapshot_id: str | None = Field(None, description="Snapshot identifier if provided")

    # Market-wide aggregates
    total_call_premium: float | None = None
    total_put_premium: float | None = None
    call_put_ratio: float | None = None

    # Sentiment indicators
    bullish_flow: float | None = None
    bearish_flow: float | None = None
    neutral_flow: float | None = None
    net_flow: float | None = None

    # Volume metrics
    total_volume: float | None = None
    unusual_volume_count: int | None = None

    # Sector/index data (if provided)
    sector_data: dict[str, Any] | None = None
    index_data: dict[str, Any] | None = None


# ==============================================================================
# V2 Schemas - News and Filing Data (PRD §9, §58, §59)
# ==============================================================================


class NewsArticleRecord(SilverBase):
    """Silver news_articles schema (PRD §9.1).

    Primary key: news_id
    """

    news_id: str = Field(..., description="Hash of URL + title + publish time")
    ts_published: datetime = Field(..., description="Original publish timestamp")
    headline: str
    summary: str | None = None
    body: str | None = Field(None, description="Full text, subject to licensing")
    url: str
    source_name: str | None = None

    # Revision fields (PRD §9.2)
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    revision_id: str | None = None


class NewsEntityRecord(SilverBase):
    """Silver news_entities schema (PRD §9.1).

    Links news articles to instruments. One row per (news_id, instrument_key) pair.
    """

    news_id: str = Field(..., description="References news_articles.news_id")
    entity_type: str = Field(..., description="company, sector, index, etc")
    confidence: float = Field(..., description="0.0-1.0 confidence score")
    match_method: str = Field(..., description="provider_tags | NER | keywords")


class NewsEventRecord(SilverBase):
    """Silver news_events schema (PRD §58).

    Structured news events with sentiment, for Silver-level analytics.
    Cross-references Document Store for full content.
    """

    news_id: str
    doc_store_id: str | None = Field(None, description="Cross-reference to Document Store")

    # Sentiment analysis
    sentiment_score: float | None = Field(None, description="-1.0 (bearish) to 1.0 (bullish)")
    sentiment_label: str | None = Field(None, description="bullish, bearish, neutral")
    relevance_score: float | None = Field(None, description="0.0-1.0 relevance to instrument")

    # Event classification
    event_type: str | None = Field(None, description="earnings, guidance, M&A, etc")
    magnitude: str | None = Field(None, description="low, medium, high impact")


class FilingEventRecord(SilverBase):
    """Silver filing_events schema (PRD §59).

    SEC filings with anti-leakage timestamp semantics.
    ts_available = ts_accepted (when SEC accepted the filing)
    """

    filing_id: str = Field(..., description="Unique filing identifier")
    accession_number: str = Field(..., description="SEC accession number")
    form_type: str = Field(..., description="10-K, 10-Q, 8-K, etc")

    # Timestamps (anti-leakage critical)
    ts_filed: datetime = Field(..., description="When company filed")
    ts_accepted: datetime = Field(..., description="When SEC accepted - use for ts_available")

    # Filing metadata
    company_name: str | None = None
    cik: str | None = Field(None, description="SEC Central Index Key")

    # Cross-reference
    doc_store_id: str | None = Field(None, description="Cross-reference to Document Store")

    # Extracted highlights (optional)
    summary: str | None = None
    key_items: list[str] | None = None


# ==============================================================================
# PyArrow Schema Helpers
# ==============================================================================

SILVER_BASE_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("provider", pa.string(), nullable=False),
        pa.field("feed", pa.string(), nullable=False),
        pa.field("instrument_type", pa.string(), nullable=False),
        pa.field("instrument_key", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("ts_event", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("ts_ingest", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("ts_available", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("schema_version", pa.string(), nullable=False),
        pa.field("quality_flags", pa.list_(pa.string())),
        pa.field("lineage", pa.string()),  # JSON serialized
    ]
)


def get_bars_schema() -> pa.Schema:
    """PyArrow schema for bars Silver dataset."""
    return pa.schema(
        [
            *SILVER_BASE_SCHEMA,
            pa.field("timeframe", pa.string(), nullable=False),
            pa.field("bar_start_ts", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("open", pa.float64(), nullable=False),
            pa.field("high", pa.float64(), nullable=False),
            pa.field("low", pa.float64(), nullable=False),
            pa.field("close", pa.float64(), nullable=False),
            pa.field("volume", pa.float64(), nullable=False),
            pa.field("trade_count", pa.int64()),
            pa.field("vwap", pa.float64()),
        ]
    )


def get_darkpool_trades_schema() -> pa.Schema:
    """PyArrow schema for darkpool_trades Silver dataset."""
    return pa.schema(
        [
            *SILVER_BASE_SCHEMA,
            pa.field("underlying", pa.string(), nullable=False),
            pa.field("price", pa.float64(), nullable=False),
            pa.field("size", pa.float64(), nullable=False),
            pa.field("notional", pa.float64()),
            pa.field("venue", pa.string()),
            pa.field("print_id", pa.string()),
            pa.field("conditions", pa.list_(pa.string())),
        ]
    )


def get_option_contracts_schema() -> pa.Schema:
    """PyArrow schema for option_contracts Silver reference table."""
    return pa.schema(
        [
            *SILVER_BASE_SCHEMA,
            pa.field("underlying", pa.string(), nullable=False),
            pa.field("occ_symbol", pa.string(), nullable=False),
            pa.field("expiry", pa.date32(), nullable=False),
            pa.field("strike", pa.float64(), nullable=False),
            pa.field("put_call", pa.string(), nullable=False),
            pa.field("multiplier", pa.int32(), nullable=False),
            pa.field("style", pa.string()),
            pa.field("exchange", pa.string()),
            pa.field("valid_from", pa.timestamp("us", tz="UTC")),
            pa.field("valid_to", pa.timestamp("us", tz="UTC")),
            pa.field("revision_id", pa.string()),
        ]
    )
