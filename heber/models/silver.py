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

    # UW additional flags (P1)
    is_sweep: bool | None = None
    is_unusual: bool | None = None
    sentiment: str | None = Field(None, description="bullish, bearish, neutral")
    trade_count: int | None = None
    volume_oi_ratio: float | None = None
    total_ask_side_prem: float | None = None
    total_bid_side_prem: float | None = None
    has_floor: bool | None = None
    has_multileg: bool | None = None
    has_singleleg: bool | None = None
    all_opening_trades: bool | None = None


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

    # UW additional fields (P1)
    nbbo_bid: float | None = None
    nbbo_ask: float | None = None
    ext_hours: str | None = None
    trade_settlement: str | None = None
    canceled: bool | None = None


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

    # UW sentiment field (P1)
    sentiment: str | None = Field(None, description="bullish, bearish, neutral")


class SectorTideRecord(SilverBase):
    """Silver sector_tide schema (UW periodic sector sentiment snapshot).

    Primary key: (ts_event, sector)
    Per-sector tide data from Unusual Whales GICS sector endpoint.
    """

    sector: str = Field(..., description="GICS sector name")

    # Premium aggregates
    net_call_premium: float | None = None
    net_put_premium: float | None = None
    call_put_ratio: float | None = None

    # Volume and sentiment
    net_volume: float | None = None
    sentiment: str | None = Field(None, description="bullish, bearish, neutral")


# ==============================================================================
# Phase 1: Core Analytics Schemas (High Value)
# ==============================================================================


class GreekExposureRecord(SilverBase):
    """GEX/DEX/VEX exposure data from UW greek_exposure endpoint."""

    gamma_exposure: float
    delta_exposure: float | None = None
    vanna_exposure: float | None = None
    charm_exposure: float | None = None
    strike: float | None = None
    expiry: date | None = None


class MaxPainRecord(SilverBase):
    """Max pain strike data."""

    expiry: str  # YYYY-MM-DD
    max_pain_strike: float
    call_oi: int | None = None
    put_oi: int | None = None


class NetPremiumTickRecord(SilverBase):
    """Net premium tick data (intraday premium flow)."""

    net_call_premium: float
    net_put_premium: float
    call_volume: int
    put_volume: int


class HottestChainRecord(SilverBase):
    """Hottest options chain/contract (high activity)."""

    contract_symbol: str
    underlying: str
    strike: float
    expiry: str  # YYYY-MM-DD
    option_type: str = Field(..., description="call or put")
    volume: int
    open_interest: int
    premium: float
    iv: float | None = None


# ==============================================================================
# Phase 2: Reference Data Schemas
# ==============================================================================


class EarningsRecord(SilverBase):
    """Earnings calendar entry."""

    earnings_date: str  # YYYY-MM-DD
    time: str = Field(..., description="premarket, afterhours, unknown")
    eps_estimate: float | None = None
    eps_actual: float | None = None
    revenue_estimate: float | None = None
    revenue_actual: float | None = None


class CorporateActionRecord(SilverBase):
    """Corporate action (splits, dividends, mergers)."""

    action_type: str = Field(..., description="split, dividend, merger, spinoff")
    ex_date: str  # YYYY-MM-DD
    record_date: str | None = None
    payable_date: str | None = None
    amount: float | None = None
    ratio: str | None = Field(None, description="For splits: 4:1, 2:1")


# ==============================================================================
# Phase 3: Market Screener Schemas
# ==============================================================================


class MostActiveRecord(SilverBase):
    """Most active stock data."""

    volume: int
    trade_count: int


class MoverRecord(SilverBase):
    """Top gainer/loser data."""

    price: float
    change: float
    percent_change: float
    direction: str = Field(..., description="gainer or loser")


class ScreenerResultRecord(SilverBase):
    """Stock screener result."""

    price: float | None = None
    volume: int | None = None
    market_cap: float | None = None
    sector: str | None = None
    call_volume: int | None = None
    put_volume: int | None = None
    iv_rank: float | None = None


# ==============================================================================
# Phase 4: Advanced Analytics Schemas
# ==============================================================================


class IVRankRecord(SilverBase):
    """IV rank/percentile data."""

    iv_rank: float
    iv_percentile: float | None = None
    current_iv: float | None = None
    one_year_high: float | None = None
    one_year_low: float | None = None


class IVTermStructureRecord(SilverBase):
    """IV term structure data point."""

    expiry: str  # YYYY-MM-DD
    iv: float
    days_to_expiry: int
    call_iv: float | None = None
    put_iv: float | None = None


class VolatilityStatsRecord(SilverBase):
    """Volatility statistics."""

    realized_vol_30d: float | None = None
    realized_vol_60d: float | None = None
    realized_vol_90d: float | None = None
    iv_30d: float | None = None
    iv_percentile: float | None = None
    hv_iv_ratio: float | None = None


class OIChangeRecord(SilverBase):
    """Open interest change data."""

    oi_date: str  # YYYY-MM-DD
    call_oi: int
    put_oi: int
    call_oi_change: int
    put_oi_change: int


class ETFHoldingRecord(SilverBase):
    """ETF holding data."""

    etf_symbol: str
    holding_symbol: str
    weight: float
    shares: int | None = None
    market_value: float | None = None


class ETFFlowRecord(SilverBase):
    """ETF inflow/outflow data."""

    flow_date: str  # YYYY-MM-DD
    inflow: float
    outflow: float
    net_flow: float


class ShortDataRecord(SilverBase):
    """Short interest data."""

    short_date: str  # YYYY-MM-DD
    short_interest: int
    days_to_cover: float | None = None
    short_percent_float: float | None = None
    short_percent_outstanding: float | None = None


class FTDRecord(SilverBase):
    """Failure to deliver data."""

    ftd_date: str  # YYYY-MM-DD
    quantity: int
    price: float | None = None
    value: float | None = None


class SeasonalityRecord(SilverBase):
    """Seasonality data."""

    month: int
    avg_return: float
    median_return: float | None = None
    win_rate: float
    sample_years: int | None = None


class OrderbookRecord(SilverBase):
    """Orderbook snapshot (crypto markets)."""

    bids_json: str = Field(..., description="JSON array of [price, size] tuples")
    asks_json: str = Field(..., description="JSON array of [price, size] tuples")
    depth: int | None = None


# ==============================================================================
# V2 Schemas - News and Filing Data (PRD §9, §58, §59)
# ==============================================================================


# ==============================================================================
# V3 Schemas - Alternative Data (Congress, Insider, Institution, Politician)
# ==============================================================================


class CongressTradeRecord(SilverBase):
    """Congressional trading activity."""

    trade_id: str = Field(..., description="Unique trade identifier")
    politician_name: str
    politician_party: str | None = None
    politician_state: str | None = None
    politician_chamber: str | None = Field(None, description="House or Senate")
    trade_type: str = Field(..., description="buy, sell, exchange")
    trade_date: date
    disclosure_date: date | None = None
    amount_min: float | None = None
    amount_max: float | None = None
    asset_type: str | None = None
    is_late: bool = False


class InsiderTradeRecord(SilverBase):
    """SEC Form 4 insider trading data."""

    filing_id: str = Field(..., description="SEC filing identifier")
    insider_name: str
    insider_title: str | None = None
    insider_relationship: str | None = Field(None, description="Officer, Director, 10% Owner")
    trade_type: str = Field(..., description="P (purchase), S (sale), A (award)")
    trade_date: date
    shares: float
    price: float | None = None
    value: float | None = None
    shares_owned_after: float | None = None
    filing_date: date | None = None


class InsiderFlowRecord(SilverBase):
    """Aggregated insider flow by sector or ticker."""

    period: str = Field(..., description="daily, weekly, monthly")
    period_start: date
    period_end: date
    sector: str | None = None
    total_buys: int
    total_sells: int
    buy_value: float
    sell_value: float
    net_value: float
    top_buyers_json: str | None = Field(None, description="JSON array of top buyers")
    top_sellers_json: str | None = Field(None, description="JSON array of top sellers")


class InstitutionHoldingRecord(SilverBase):
    """13F institutional holdings data."""

    filing_id: str = Field(..., description="SEC 13F filing identifier")
    institution_name: str
    institution_cik: str | None = None
    holding_symbol: str
    shares: float
    value: float
    quarter_end: date
    change_shares: float | None = None
    change_pct: float | None = None
    portfolio_pct: float | None = None
    filing_date: date | None = None


class InstitutionActivityRecord(SilverBase):
    """Institutional activity and 13F filing metadata."""

    filing_id: str
    institution_name: str
    institution_cik: str | None = None
    filing_date: date
    quarter_end: date
    total_value: float | None = None
    holdings_count: int | None = None
    new_positions: int | None = None
    closed_positions: int | None = None
    increased_positions: int | None = None
    decreased_positions: int | None = None


class PoliticianTradeRecord(SilverBase):
    """Politician portfolio trades (superset of congress)."""

    trade_id: str
    politician_id: str
    politician_name: str
    politician_party: str | None = None
    trade_type: str
    trade_date: date
    disclosure_date: date | None = None
    amount_min: float | None = None
    amount_max: float | None = None
    asset_description: str | None = None
    comment: str | None = None


# ==============================================================================
# V4 Schemas - Market Analytics (Analyst, Fundamentals, Events, Indicators)
# ==============================================================================


class AnalystRatingRecord(SilverBase):
    """Analyst ratings and price targets."""

    rating_id: str = Field(..., description="Unique rating identifier")
    analyst_name: str | None = None
    analyst_firm: str | None = None
    rating: str = Field(..., description="Buy, Hold, Sell, etc.")
    rating_prior: str | None = None
    price_target: float | None = None
    price_target_prior: float | None = None
    rating_date: date
    action: str | None = Field(None, description="upgrade, downgrade, initiate, reiterate")


class StockFundamentalsRecord(SilverBase):
    """Stock fundamentals and company info snapshot."""

    snapshot_date: date
    company_name: str | None = None
    sector: str | None = None
    industry: str | None = None
    market_cap: float | None = None
    pe_ratio: float | None = None
    eps: float | None = None
    dividend_yield: float | None = None
    beta: float | None = None
    shares_outstanding: float | None = None
    float_shares: float | None = None
    avg_volume: float | None = None
    price: float | None = None
    high_52w: float | None = None
    low_52w: float | None = None


class EconomicEventRecord(SilverBase):
    """Economic calendar events (FOMC, CPI, GDP, etc.)."""

    event_name: str
    event_type: str = Field(..., description="FOMC, CPI, GDP, NFP, etc.")
    event_date: date
    event_time: str | None = None
    country: str | None = Field(None, description="US, UK, etc.")
    importance: str | None = Field(None, description="high, medium, low")
    actual: str | None = None
    forecast: str | None = None
    previous: str | None = None
    source_url: str | None = None


class MarketIndicatorRecord(SilverBase):
    """Market-wide indicators (SPIKE, correlations, VIX, etc.)."""

    indicator_name: str = Field(..., description="SPIKE, VIX, correlation, etc.")
    indicator_date: date
    indicator_time: str | None = None
    value: float
    value_prior: float | None = None
    change: float | None = None
    change_pct: float | None = None
    percentile: float | None = None
    metadata_json: str | None = Field(None, description="Additional indicator-specific data")


# ==============================================================================
# V5 Schemas - Options Deep Data (History, Chains, Volume Profile, Group Flow)
# ==============================================================================


class OptionHistoryRecord(SilverBase):
    """Historical option contract data."""

    occ_symbol: str
    history_date: date
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    volume: float | None = None
    open_interest: float | None = None
    implied_volatility: float | None = None
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None


class OptionChainSnapshotRecord(SilverBase):
    """Point-in-time snapshot of option chain."""

    snapshot_ts: datetime
    underlying: str
    expiry: date
    chain_json: str = Field(..., description="JSON of full chain data")
    total_call_volume: float | None = None
    total_put_volume: float | None = None
    total_call_oi: float | None = None
    total_put_oi: float | None = None
    atm_iv: float | None = None


class VolumeProfileRecord(SilverBase):
    """Volume profile data for option contracts."""

    occ_symbol: str
    profile_date: date
    price_level: float
    volume: float
    cumulative_volume: float | None = None
    pct_of_total: float | None = None


class GroupFlowRecord(SilverBase):
    """Greek flow aggregated by group (sector, index, etc.)."""

    group_name: str = Field(..., description="SPY, QQQ, sector name, etc.")
    group_type: str = Field(..., description="etf, sector, index")
    flow_date: date
    expiry: date | None = None
    call_gex: float | None = None
    put_gex: float | None = None
    net_gex: float | None = None
    call_dex: float | None = None
    put_dex: float | None = None
    net_premium: float | None = None


# ==============================================================================
# V6 Schemas - ETF Deep Data (Metadata, Sector Weights)
# ==============================================================================


class ETFMetadataRecord(SilverBase):
    """ETF metadata and exposure details."""

    etf_symbol: str
    snapshot_date: date
    fund_name: str | None = None
    issuer: str | None = None
    expense_ratio: float | None = None
    aum: float | None = None
    avg_volume: float | None = None
    inception_date: date | None = None
    asset_class: str | None = None
    category: str | None = None
    index_tracked: str | None = None
    leverage_factor: float | None = None


class ETFSectorWeightsRecord(SilverBase):
    """ETF sector and country weight breakdown."""

    etf_symbol: str
    weight_date: date
    weight_type: str = Field(..., description="sector or country")
    weight_name: str = Field(..., description="Technology, Healthcare, USA, etc.")
    weight_pct: float
    change_pct: float | None = None


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
