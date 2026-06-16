"""Apache Iceberg Catalog Configuration for Heber.

This module provides the Iceberg catalog setup for the Silver/Gold layers,
replacing custom Parquet management with Iceberg's ACID transactions,
schema evolution, and time-travel capabilities.

Phase 1 of OSS Migration Roadmap.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
from enum import StrEnum
from functools import lru_cache
from typing import Any

import structlog
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

logger = structlog.get_logger(__name__)


class IcebergCatalogType(StrEnum):
    """Supported Iceberg catalog backends."""

    SQL = "sql"  # PostgreSQL-based (recommended for production)
    REST = "rest"  # REST catalog (e.g., Tabular, AWS Glue)
    SQLITE = "sqlite"  # Local development
    IN_MEMORY = "in-memory"  # Testing only


@dataclass
class IcebergConfig:
    """Iceberg catalog configuration.

    Environment Variables:
        ICEBERG_CATALOG_TYPE: Catalog backend type (default: sql)
        ICEBERG_CATALOG_URI: Database connection string
        ICEBERG_WAREHOUSE: S3/local path for data files
        ICEBERG_S3_ENDPOINT: S3-compatible endpoint (for MinIO)
        ICEBERG_S3_ACCESS_KEY: S3 access key
        ICEBERG_S3_SECRET_KEY: S3 secret key
    """

    catalog_type: IcebergCatalogType = IcebergCatalogType.SQL
    catalog_uri: str = "postgresql://heber:heber@localhost:5432/heber_iceberg"  # pragma: allowlist secret
    warehouse: str = "s3://heber-lakehouse/warehouse"
    s3_endpoint: str | None = None  # For MinIO: http://localhost:9000
    s3_access_key: str | None = None
    s3_secret_key: str | None = None

    @classmethod
    def from_env(cls) -> IcebergConfig:
        """Load configuration from Heber settings."""
        from heber.config import get_settings

        settings = get_settings()
        return cls(
            catalog_type=IcebergCatalogType(settings.iceberg_catalog_type),
            catalog_uri=settings.iceberg_catalog_uri,
            warehouse=settings.iceberg_warehouse,
            s3_endpoint=settings.iceberg_s3_endpoint,
            s3_access_key=settings.iceberg_s3_access_key,
            s3_secret_key=settings.iceberg_s3_secret_key,
        )


@lru_cache(maxsize=1)
def get_iceberg_catalog(config: IcebergConfig | None = None) -> Catalog:
    """Get or create the Iceberg catalog singleton.

    Args:
        config: Optional configuration. If None, loads from environment.

    Returns:
        Configured Iceberg Catalog instance.
    """
    if config is None:
        config = IcebergConfig.from_env()

    catalog_properties: dict[str, Any] = {
        "type": config.catalog_type.value,
        "warehouse": config.warehouse,
    }

    # Add catalog-specific properties
    if config.catalog_type == IcebergCatalogType.SQL:
        catalog_properties["uri"] = config.catalog_uri

    # Add S3 properties if configured (for MinIO or custom S3)
    if config.s3_endpoint:
        catalog_properties["s3.endpoint"] = config.s3_endpoint
    if config.s3_access_key:
        catalog_properties["s3.access-key-id"] = config.s3_access_key
    if config.s3_secret_key:
        catalog_properties["s3.secret-access-key"] = config.s3_secret_key

    logger.info(
        "initializing_iceberg_catalog",
        catalog_type=config.catalog_type.value,
        warehouse=config.warehouse,
    )

    return load_catalog("heber", **catalog_properties)


# =============================================================================
# Silver Table Schemas (Iceberg format)
# =============================================================================

# Base columns present in all Silver tables
SILVER_BASE_FIELDS = [
    NestedField(1, "event_id", StringType(), required=True),
    NestedField(2, "instrument_key", StringType(), required=True),
    NestedField(3, "instrument_type", StringType(), required=True),
    NestedField(4, "provider", StringType(), required=True),
    NestedField(5, "feed", StringType(), required=True),
    NestedField(6, "ts_event", TimestamptzType(), required=True),
    NestedField(7, "ts_ingest", TimestamptzType(), required=True),
    NestedField(8, "ts_available", TimestamptzType(), required=True),
    NestedField(9, "processing_delay_ms", LongType(), required=False),
    NestedField(10, "source", StringType(), required=False),
    NestedField(11, "quality_flags", StringType(), required=False),
]


def get_silver_bars_schema() -> Schema:
    """Iceberg schema for Silver bars table."""
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "bar_start_ts", TimestamptzType(), required=True),
        NestedField(21, "bar_end_ts", TimestamptzType(), required=True),
        NestedField(22, "bar_duration_seconds", LongType(), required=True),
        NestedField(23, "open", DoubleType(), required=True),
        NestedField(24, "high", DoubleType(), required=True),
        NestedField(25, "low", DoubleType(), required=True),
        NestedField(26, "close", DoubleType(), required=True),
        NestedField(27, "volume", LongType(), required=True),
        NestedField(28, "vwap", DoubleType(), required=False),
        NestedField(29, "trade_count", LongType(), required=False),
    )


def get_silver_quotes_schema() -> Schema:
    """Iceberg schema for Silver quotes table."""
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "bid_price", DoubleType(), required=True),
        NestedField(21, "bid_size", LongType(), required=True),
        NestedField(22, "ask_price", DoubleType(), required=True),
        NestedField(23, "ask_size", LongType(), required=True),
        NestedField(24, "bid_exchange", StringType(), required=False),  # Alpaca WS: bx
        NestedField(25, "ask_exchange", StringType(), required=False),  # Alpaca WS: ax
        NestedField(26, "conditions", StringType(), required=False),  # Alpaca WS: c (JSON)
        NestedField(27, "tape", StringType(), required=False),  # Alpaca WS: z (A/B/C)
    )


def get_silver_trades_schema() -> Schema:
    """Iceberg schema for Silver trades table."""
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "price", DoubleType(), required=True),
        NestedField(21, "size", LongType(), required=True),
        NestedField(22, "exchange", StringType(), required=False),  # Stocks: exchange code
        NestedField(23, "conditions", StringType(), required=False),  # Stocks: trade conditions (JSON)
        NestedField(24, "tape", StringType(), required=False),  # Stocks: A/B/C tape
        NestedField(25, "trade_id", StringType(), required=False),  # Both: unique trade ID
        NestedField(26, "taker_side", StringType(), required=False),  # Crypto: B=buy, S=sell
    )


def get_silver_flow_alerts_schema() -> Schema:
    """Iceberg schema for Silver flow_alerts table.

    Matches UW Flow Alert API response.
    """
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "alert_type", StringType(), required=False),  # alert_rule in UW
        NestedField(21, "sentiment", StringType(), required=False),  # computed
        NestedField(22, "premium", DoubleType(), required=True),  # total_premium
        NestedField(23, "volume", LongType(), required=True),
        NestedField(24, "open_interest", LongType(), required=False),
        NestedField(25, "strike", DoubleType(), required=False),
        NestedField(26, "expiry", StringType(), required=False),  # Changed to string YYYY-MM-DD
        NestedField(27, "option_type", StringType(), required=False),  # call/put
        NestedField(28, "side", StringType(), required=False),  # bid, ask, mid
        NestedField(29, "is_sweep", BooleanType(), required=False),  # has_sweep
        NestedField(30, "is_unusual", BooleanType(), required=False),
        NestedField(31, "symbol", StringType(), required=False),  # ticker
        # Additional UW fields
        NestedField(32, "option_chain", StringType(), required=False),  # OCC symbol
        NestedField(33, "price", DoubleType(), required=False),  # Option price
        NestedField(34, "underlying_price", DoubleType(), required=False),  # Stock price
        NestedField(35, "alert_rule", StringType(), required=False),  # RepeatedHits, etc.
        NestedField(36, "total_size", LongType(), required=False),  # Total contracts
        NestedField(37, "trade_count", LongType(), required=False),  # Number of trades
        NestedField(38, "volume_oi_ratio", DoubleType(), required=False),  # Vol/OI
        NestedField(39, "total_ask_side_prem", DoubleType(), required=False),
        NestedField(40, "total_bid_side_prem", DoubleType(), required=False),
        NestedField(41, "all_opening_trades", BooleanType(), required=False),
        NestedField(42, "has_floor", BooleanType(), required=False),
        NestedField(43, "has_multileg", BooleanType(), required=False),
        NestedField(44, "has_singleleg", BooleanType(), required=False),
        NestedField(45, "expiry_count", LongType(), required=False),  # Number of expiries
    )


def get_silver_darkpool_schema() -> Schema:
    """Iceberg schema for Silver darkpool trades table.

    Matches UW darkpool API response.
    """
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "symbol", StringType(), required=True),
        NestedField(21, "price", DoubleType(), required=True),
        NestedField(22, "size", LongType(), required=True),
        NestedField(23, "notional", DoubleType(), required=True),  # premium in UW
        NestedField(24, "exchange", StringType(), required=False),  # market_center
        NestedField(25, "tracking_id", StringType(), required=False),  # Unique trade ID
        NestedField(26, "nbbo_bid", DoubleType(), required=False),  # NBBO bid at trade
        NestedField(27, "nbbo_ask", DoubleType(), required=False),  # NBBO ask at trade
        NestedField(28, "ext_hours", StringType(), required=False),  # extended_hours_trade
        NestedField(29, "trade_settlement", StringType(), required=False),  # regular_settlement
        NestedField(30, "canceled", BooleanType(), required=False),  # Was trade cancelled
    )


def get_silver_congress_trades_schema() -> Schema:
    """Iceberg schema for Silver congressional trades table.

    Matches UW Senate Stock API response exactly.
    """
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "ticker", StringType(), required=True),  # Stock symbol
        NestedField(21, "name", StringType(), required=True),  # Politician's standard name
        NestedField(22, "txn_type", StringType(), required=True),  # Buy/Sell
        NestedField(23, "amounts", StringType(), required=False),  # Range like "$15,001 - $50,000"
        NestedField(24, "transaction_date", StringType(), required=False),  # YYYY-MM-DD
        NestedField(25, "filed_at_date", StringType(), required=False),  # Disclosure date
        NestedField(26, "member_type", StringType(), required=False),  # house/senate
        NestedField(27, "politician_id", StringType(), required=False),  # UUID for tracking
        NestedField(28, "reporter", StringType(), required=False),  # Who filed
        NestedField(29, "notes", StringType(), required=False),  # Subholding details
        NestedField(30, "issuer", StringType(), required=False),  # not-disclosed, joint, etc.
        NestedField(31, "is_active", BooleanType(), required=False),  # Still in office
    )


def get_silver_insider_trades_schema() -> Schema:
    """Iceberg schema for Silver insider trades table.

    Matches UW Insider Trade Agg API response exactly.
    """
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "ticker", StringType(), required=True),  # Stock symbol
        NestedField(21, "owner_name", StringType(), required=True),  # Insider name
        NestedField(22, "officer_title", StringType(), required=False),  # CEO, CFO, etc.
        NestedField(23, "transaction_code", StringType(), required=True),  # S, P, A, etc.
        NestedField(24, "amount", LongType(), required=False),  # Shares transacted (neg=sell)
        NestedField(25, "price", DoubleType(), required=False),
        NestedField(26, "transaction_date", StringType(), required=False),  # YYYY-MM-DD
        NestedField(27, "filing_date", StringType(), required=False),  # YYYY-MM-DD
        NestedField(28, "formtype", StringType(), required=False),  # 4, 144, etc.
        NestedField(29, "id", StringType(), required=False),  # Trade UUID
        NestedField(30, "is_10b5_1", BooleanType(), required=False),  # Part of 10b5-1 plan
        NestedField(31, "is_director", BooleanType(), required=False),
        NestedField(32, "is_officer", BooleanType(), required=False),
        NestedField(33, "is_ten_percent_owner", BooleanType(), required=False),
        NestedField(34, "sector", StringType(), required=False),
        NestedField(35, "marketcap", StringType(), required=False),
        NestedField(36, "is_s_p_500", BooleanType(), required=False),
        NestedField(37, "next_earnings_date", StringType(), required=False),
        NestedField(38, "shares_owned_before", LongType(), required=False),
        NestedField(39, "shares_owned_after", LongType(), required=False),
        NestedField(40, "security_title", StringType(), required=False),  # Common Stock, etc.
        NestedField(41, "natureofownership", StringType(), required=False),  # Direct, Indirect
        NestedField(42, "transactions", LongType(), required=False),  # Number of transactions
    )


def get_silver_market_tide_schema() -> Schema:
    """Iceberg schema for Silver market tide/sentiment table.

    Matches UW Daily Market Tide API response.
    Supports market tide, sector tide, and ETF tide (all use same UW schema).
    """
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "tide_type", StringType(), required=True),  # market, sector, etf
        NestedField(21, "date", StringType(), required=False),  # Trading date YYYY-MM-DD
        NestedField(22, "net_call_premium", DoubleType(), required=True),
        NestedField(23, "net_put_premium", DoubleType(), required=True),
        NestedField(24, "net_volume", LongType(), required=False),  # Net volume (call - put)
        NestedField(25, "sentiment", StringType(), required=False),  # bullish/bearish/neutral (computed)
        NestedField(26, "sector", StringType(), required=False),  # For sector tide
        NestedField(27, "ticker", StringType(), required=False),  # For ETF tide
    )


def get_silver_greek_exposure_schema() -> Schema:
    """Iceberg schema for Silver greek exposure (GEX) table.

    Matches UW Greek Exposure API response exactly.
    Supports main GEX, by-expiry, by-strike, and by-strike-expiry variants.
    """
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "symbol", StringType(), required=True),  # Ticker
        NestedField(21, "gex_type", StringType(), required=True),  # main, by_expiry, by_strike, by_strike_expiry
        NestedField(22, "date", StringType(), required=False),  # Trading date YYYY-MM-DD
        # Call greeks
        NestedField(23, "call_gamma", DoubleType(), required=False),
        NestedField(24, "call_delta", DoubleType(), required=False),
        NestedField(25, "call_vanna", DoubleType(), required=False),
        NestedField(26, "call_charm", DoubleType(), required=False),
        # Put greeks
        NestedField(27, "put_gamma", DoubleType(), required=False),
        NestedField(28, "put_delta", DoubleType(), required=False),
        NestedField(29, "put_vanna", DoubleType(), required=False),
        NestedField(30, "put_charm", DoubleType(), required=False),
        # Optional grouping fields
        NestedField(31, "strike", DoubleType(), required=False),  # For by-strike variants
        NestedField(32, "expiry", StringType(), required=False),  # For by-expiry variants
        NestedField(33, "dte", LongType(), required=False),  # Days to expiry
    )


def get_silver_user_alerts_schema() -> Schema:
    """Iceberg schema for Silver user alerts table.

    Matches UW Alert API response exactly.
    These are custom user-created alerts (dividends, trading halts, SEC filings, etc.)
    Not to be confused with flow_alerts which are option flow signals.
    """
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "alert_id", StringType(), required=True),  # UW alert UUID
        NestedField(21, "created_at", StringType(), required=False),  # ISO timestamp
        NestedField(22, "name", StringType(), required=False),  # Alert name
        NestedField(23, "noti_type", StringType(), required=True),  # dividends, trading_state, sec_filings, etc.
        NestedField(24, "symbol", StringType(), required=False),  # Ticker or contract
        NestedField(25, "symbol_type", StringType(), required=False),  # stock, option
        NestedField(26, "tape_time", StringType(), required=False),  # Alert tape timestamp
        NestedField(27, "user_noti_config_id", StringType(), required=False),  # User's alert config ID
        NestedField(28, "meta", StringType(), required=False),  # JSON blob of alert-specific data
    )


def get_silver_earnings_schema() -> Schema:
    """Iceberg schema for Silver earnings table.

    Comprehensive schema capturing both UW Earnings (premarket/afterhours calendar)
    and Historical Ticker Earnings schemas. Uses earnings_type discriminator.
    """
    return Schema(
        *SILVER_BASE_FIELDS,
        # Discriminator and common fields
        NestedField(20, "symbol", StringType(), required=True),  # Ticker symbol
        NestedField(21, "earnings_type", StringType(), required=True),  # 'premarket', 'afterhours', 'historical'
        NestedField(22, "report_date", StringType(), required=False),  # Earnings report date
        NestedField(23, "report_time", StringType(), required=False),  # premarket/postmarket
        NestedField(24, "actual_eps", StringType(), required=False),  # Actual EPS
        NestedField(25, "street_mean_est", StringType(), required=False),  # Street consensus estimate
        NestedField(26, "source", StringType(), required=False),  # company, analyst, etc.
        NestedField(27, "ending_fiscal_quarter", StringType(), required=False),  # Fiscal quarter end date
        NestedField(28, "expected_move", StringType(), required=False),  # Expected price move
        NestedField(29, "expected_move_perc", StringType(), required=False),  # Expected move percentage
        # Calendar-specific fields (premarket/afterhours)
        NestedField(30, "full_name", StringType(), required=False),  # Company full name
        NestedField(31, "sector", StringType(), required=False),  # Sector
        NestedField(32, "marketcap", StringType(), required=False),  # Market cap
        NestedField(33, "has_options", BooleanType(), required=False),  # Has options
        NestedField(34, "is_s_p_500", BooleanType(), required=False),  # In S&P 500
        NestedField(35, "continent", StringType(), required=False),  # Continent
        NestedField(36, "country_code", StringType(), required=False),  # Country code
        NestedField(37, "country_name", StringType(), required=False),  # Country name
        NestedField(38, "pre_earnings_close", StringType(), required=False),  # Close before earnings
        NestedField(39, "pre_earnings_date", StringType(), required=False),  # Date before earnings
        NestedField(40, "post_earnings_close", StringType(), required=False),  # Close after earnings
        NestedField(41, "post_earnings_date", StringType(), required=False),  # Date after earnings
        NestedField(42, "reaction", StringType(), required=False),  # Price reaction
        # Historical ticker-specific fields
        NestedField(43, "pre_earnings_move_1d", StringType(), required=False),
        NestedField(44, "pre_earnings_move_3d", StringType(), required=False),
        NestedField(45, "pre_earnings_move_1w", StringType(), required=False),
        NestedField(46, "pre_earnings_move_2w", StringType(), required=False),
        NestedField(47, "post_earnings_move_1d", StringType(), required=False),
        NestedField(48, "post_earnings_move_3d", StringType(), required=False),
        NestedField(49, "post_earnings_move_1w", StringType(), required=False),
        NestedField(50, "post_earnings_move_2w", StringType(), required=False),
        NestedField(51, "long_straddle_1d", StringType(), required=False),
        NestedField(52, "long_straddle_1w", StringType(), required=False),
        NestedField(53, "short_straddle_1d", StringType(), required=False),
        NestedField(54, "short_straddle_1w", StringType(), required=False),
    )


def get_silver_etf_info_schema() -> Schema:
    """Iceberg schema for Silver ETF info table."""
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "ticker", StringType(), required=True),  # ETF ticker
        NestedField(21, "name", StringType(), required=False),  # ETF full name
        NestedField(22, "description", StringType(), required=False),  # ETF description
        NestedField(23, "etf_company", StringType(), required=False),  # Issuer
        NestedField(24, "domicile", StringType(), required=False),  # Country domicile
        NestedField(25, "inception_date", StringType(), required=False),  # Launch date
        NestedField(26, "expense_ratio", StringType(), required=False),  # Expense ratio
        NestedField(27, "aum", StringType(), required=False),  # Assets under management
        NestedField(28, "holdings_count", LongType(), required=False),  # Number of holdings
        NestedField(29, "has_options", BooleanType(), required=False),  # Has options
        NestedField(30, "avg30_volume", StringType(), required=False),  # 30-day avg volume
        NestedField(31, "call_vol", LongType(), required=False),  # Call volume
        NestedField(32, "put_vol", LongType(), required=False),  # Put volume
        NestedField(33, "opt_vol", LongType(), required=False),  # Total options volume
        NestedField(34, "stock_vol", LongType(), required=False),  # Stock volume
        NestedField(35, "website", StringType(), required=False),  # ETF website
    )


def get_silver_etf_holdings_schema() -> Schema:
    """Iceberg schema for Silver ETF holdings table."""
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "etf_ticker", StringType(), required=True),  # Parent ETF
        NestedField(21, "ticker", StringType(), required=True),  # Holding ticker
        NestedField(22, "name", StringType(), required=False),  # Company name
        NestedField(23, "short_name", StringType(), required=False),  # Short name
        NestedField(24, "sector", StringType(), required=False),  # Sector
        NestedField(25, "weight", StringType(), required=False),  # Weight in ETF
        NestedField(26, "shares", LongType(), required=False),  # Shares held
        NestedField(27, "type", StringType(), required=False),  # stock/other
        NestedField(28, "close", StringType(), required=False),  # Close price
        NestedField(29, "high", StringType(), required=False),  # Day high
        NestedField(30, "low", StringType(), required=False),  # Day low
        NestedField(31, "open", StringType(), required=False),  # Day open
        NestedField(32, "prev_price", StringType(), required=False),  # Previous close
        NestedField(33, "volume", LongType(), required=False),  # Day volume
        NestedField(34, "avg30_volume", StringType(), required=False),  # 30-day avg volume
        NestedField(35, "week52_high", StringType(), required=False),  # 52-week high
        NestedField(36, "week52_low", StringType(), required=False),  # 52-week low
        NestedField(37, "has_options", BooleanType(), required=False),  # Has options
        NestedField(38, "call_volume", LongType(), required=False),  # Call volume
        NestedField(39, "put_volume", LongType(), required=False),  # Put volume
        NestedField(40, "call_premium", StringType(), required=False),  # Call premium
        NestedField(41, "put_premium", StringType(), required=False),  # Put premium
        NestedField(42, "bullish_premium", StringType(), required=False),  # Bullish premium
        NestedField(43, "bearish_premium", StringType(), required=False),  # Bearish premium
    )


def get_silver_etf_exposure_schema() -> Schema:
    """Iceberg schema for Silver ETF exposure table (ticker in which ETFs)."""
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "ticker", StringType(), required=True),  # Stock ticker
        NestedField(21, "etf", StringType(), required=True),  # ETF ticker
        NestedField(22, "full_name", StringType(), required=False),  # ETF full name
        NestedField(23, "weight", StringType(), required=False),  # Weight of ticker in ETF
        NestedField(24, "shares", LongType(), required=False),  # Shares held
        NestedField(25, "last_price", StringType(), required=False),  # Last price
        NestedField(26, "prev_price", StringType(), required=False),  # Previous close
    )


def get_silver_etf_inflow_outflow_schema() -> Schema:
    """Iceberg schema for Silver ETF inflow/outflow table."""
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "ticker", StringType(), required=True),  # ETF ticker
        NestedField(21, "date", StringType(), required=True),  # Trading date
        NestedField(22, "change", LongType(), required=False),  # Share change
        NestedField(23, "change_prem", StringType(), required=False),  # Premium change
        NestedField(24, "close", StringType(), required=False),  # Close price
        NestedField(25, "volume", LongType(), required=False),  # Volume
        NestedField(26, "is_fomc", BooleanType(), required=False),  # FOMC date flag
    )


def get_silver_etf_weights_schema() -> Schema:
    """Iceberg schema for Silver ETF sector/country weights table."""
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "ticker", StringType(), required=True),  # ETF ticker
        NestedField(21, "weight_type", StringType(), required=True),  # 'sector' or 'country'
        NestedField(22, "name", StringType(), required=True),  # Sector or country name
        NestedField(23, "weight", StringType(), required=False),  # Allocation weight
    )


def get_silver_group_flow_schema() -> Schema:
    """Iceberg schema for Silver group flow table.

    Captures Greek flow (delta & vega) for flow groups like sectors/industries.
    Works for both per-minute and per-minute-per-expiry data.
    """
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "flow_group", StringType(), required=True),  # airline, tech, etc.
        NestedField(21, "timestamp", StringType(), required=True),  # Minute timestamp
        NestedField(22, "expiry", StringType(), required=False),  # Optional expiry for by-expiry
        # Delta and vega flows
        NestedField(23, "dir_delta_flow", StringType(), required=False),  # Directional delta flow
        NestedField(24, "dir_vega_flow", StringType(), required=False),  # Directional vega flow
        NestedField(25, "total_delta_flow", StringType(), required=False),  # Total delta flow
        NestedField(26, "total_vega_flow", StringType(), required=False),  # Total vega flow
        # OTM flows
        NestedField(27, "otm_dir_delta_flow", StringType(), required=False),  # OTM directional delta
        NestedField(28, "otm_dir_vega_flow", StringType(), required=False),  # OTM directional vega
        NestedField(29, "otm_total_delta_flow", StringType(), required=False),  # OTM total delta
        NestedField(30, "otm_total_vega_flow", StringType(), required=False),  # OTM total vega
        # Call/put premium and volume
        NestedField(31, "net_call_premium", StringType(), required=False),  # Net call premium
        NestedField(32, "net_put_premium", StringType(), required=False),  # Net put premium
        NestedField(33, "net_call_volume", LongType(), required=False),  # Net call volume
        NestedField(34, "net_put_volume", LongType(), required=False),  # Net put volume
        # Aggregates
        NestedField(35, "volume", LongType(), required=False),  # Total volume
        NestedField(36, "transactions", LongType(), required=False),  # Transaction count
    )


def get_silver_insider_flow_schema() -> Schema:
    """Iceberg schema for Silver insider flow table.

    Used for both sector flow and ticker flow endpoints, with flow_type discriminator.
    """
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "flow_type", StringType(), required=True),  # 'sector' or 'ticker'
        NestedField(21, "identifier", StringType(), required=True),  # sector name or ticker
        NestedField(22, "date", StringType(), required=True),  # Trading date
        NestedField(23, "buy_sell", StringType(), required=True),  # 'buy' or 'sell'
        NestedField(24, "avg_price", StringType(), required=False),  # Average price
        NestedField(25, "premium", StringType(), required=False),  # Total premium
        NestedField(26, "volume", LongType(), required=False),  # Share volume
        NestedField(27, "transactions", LongType(), required=False),  # Transaction count
        NestedField(28, "uniq_insiders", LongType(), required=False),  # Unique insiders
    )


def get_silver_ticker_insiders_schema() -> Schema:
    """Iceberg schema for Silver ticker insiders table."""
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "ticker", StringType(), required=True),  # Stock ticker
        NestedField(21, "insider_id", LongType(), required=True),  # Insider ID
        NestedField(22, "name", StringType(), required=False),  # Insider name
        NestedField(23, "name_slug", StringType(), required=False),  # URL slug
        NestedField(24, "is_person", BooleanType(), required=False),  # Is person vs entity
        NestedField(25, "logo_url", StringType(), required=False),  # Logo URL
        NestedField(26, "social_links", StringType(), required=False),  # JSON array of links
    )


def get_silver_institution_activity_schema() -> Schema:
    """Iceberg schema for Silver institutional activity table."""
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "institution_name", StringType(), required=True),  # Institution name
        NestedField(21, "ticker", StringType(), required=True),  # Stock ticker
        NestedField(22, "filing_date", StringType(), required=True),  # Filing date
        NestedField(23, "report_date", StringType(), required=False),  # Report date
        NestedField(24, "security_type", StringType(), required=False),  # Share, Fund, etc.
        NestedField(25, "put_call", StringType(), required=False),  # Put/Call indicator
        NestedField(26, "units", LongType(), required=False),  # Current units
        NestedField(27, "units_change", LongType(), required=False),  # Change in units
        NestedField(28, "avg_price", StringType(), required=False),  # Average price
        NestedField(29, "close", StringType(), required=False),  # Stock close price
        NestedField(30, "buy_price", StringType(), required=False),  # Buy price
        NestedField(31, "sell_price", StringType(), required=False),  # Sell price
        NestedField(32, "price_on_filing", StringType(), required=False),  # Price on filing date
        NestedField(33, "price_on_report", StringType(), required=False),  # Price on report date
        NestedField(34, "shares_outstanding", StringType(), required=False),  # Total shares
    )


def get_silver_institution_holdings_schema() -> Schema:
    """Iceberg schema for Silver institutional holdings table."""
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "institution_name", StringType(), required=True),  # Institution name
        NestedField(21, "ticker", StringType(), required=True),  # Stock ticker
        NestedField(22, "full_name", StringType(), required=False),  # Full stock name
        NestedField(23, "sector", StringType(), required=False),  # Stock sector
        NestedField(24, "date", StringType(), required=False),  # Report date
        NestedField(25, "first_buy", StringType(), required=False),  # First purchase date
        NestedField(26, "price_first_buy", StringType(), required=False),  # Price at first buy
        NestedField(27, "security_type", StringType(), required=False),  # Share, Fund, etc.
        NestedField(28, "put_call", StringType(), required=False),  # Put/Call indicator
        NestedField(29, "units", LongType(), required=False),  # Current units
        NestedField(30, "units_change", LongType(), required=False),  # Change in units
        NestedField(31, "historical_units", StringType(), required=False),  # JSON array
        NestedField(32, "value", LongType(), required=False),  # Position value
        NestedField(33, "avg_price", StringType(), required=False),  # Average price
        NestedField(34, "close", StringType(), required=False),  # Stock close price
        NestedField(35, "perc_of_total", DoubleType(), required=False),  # % of portfolio
        NestedField(36, "perc_of_share_value", DoubleType(), required=False),  # % of shares
        NestedField(37, "shares_outstanding", StringType(), required=False),  # Total shares
    )


def get_silver_institution_sector_exposure_schema() -> Schema:
    """Iceberg schema for Silver institutional sector exposure table."""
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "institution_name", StringType(), required=True),  # Institution name
        NestedField(21, "sector", StringType(), required=True),  # Sector name
        NestedField(22, "report_date", StringType(), required=False),  # Report date
        NestedField(23, "positions", LongType(), required=False),  # Total positions
        NestedField(24, "positions_closed", LongType(), required=False),  # Closed positions
        NestedField(25, "positions_decreased", LongType(), required=False),  # Decreased
        NestedField(26, "positions_increased", LongType(), required=False),  # Increased
        NestedField(27, "value", StringType(), required=False),  # Total value
    )


def get_silver_institutional_ownership_schema() -> Schema:
    """Iceberg schema for Silver institutional ownership table."""
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "ticker", StringType(), required=True),  # Stock ticker
        NestedField(21, "name", StringType(), required=True),  # Institution name
        NestedField(22, "short_name", StringType(), required=False),  # Short name
        NestedField(23, "filing_date", StringType(), required=False),  # Filing date
        NestedField(24, "report_date", StringType(), required=False),  # Report date
        NestedField(25, "first_buy", StringType(), required=False),  # First purchase date
        NestedField(26, "units", LongType(), required=False),  # Current units
        NestedField(27, "units_change", LongType(), required=False),  # Change in units
        NestedField(28, "historical_units", StringType(), required=False),  # JSON array
        NestedField(29, "value", StringType(), required=False),  # Position value
        NestedField(30, "avg_price", StringType(), required=False),  # Average price
        NestedField(31, "inst_value", StringType(), required=False),  # Total institution value
        NestedField(32, "inst_share_value", StringType(), required=False),  # Share value
        NestedField(33, "shares_outstanding", StringType(), required=False),  # Total shares
        NestedField(34, "people", StringType(), required=False),  # JSON array of people
        NestedField(35, "tags", StringType(), required=False),  # JSON array of tags
    )


# =============================================================================
# Alpaca Trading API Schemas
# =============================================================================


def get_silver_alpaca_account_schema() -> Schema:
    """Iceberg schema for Alpaca account snapshots.

    Matches Alpaca Trading API /v2/account response (36 fields).
    """
    return Schema(
        *SILVER_BASE_FIELDS,
        # Identity
        NestedField(20, "account_id", StringType(), required=True),
        NestedField(21, "account_number", StringType(), required=True),
        NestedField(22, "status", StringType(), required=True),
        NestedField(23, "currency", StringType(), required=True),
        # Balances
        NestedField(24, "cash", DoubleType(), required=True),
        NestedField(25, "portfolio_value", DoubleType(), required=False),
        NestedField(26, "equity", DoubleType(), required=False),
        NestedField(27, "last_equity", DoubleType(), required=False),
        NestedField(28, "buying_power", DoubleType(), required=False),
        NestedField(29, "daytrading_buying_power", DoubleType(), required=False),
        NestedField(30, "non_marginable_buying_power", DoubleType(), required=False),
        # Market values
        NestedField(31, "long_market_value", DoubleType(), required=False),
        NestedField(32, "short_market_value", DoubleType(), required=False),
        # Margin
        NestedField(33, "initial_margin", DoubleType(), required=False),
        NestedField(34, "maintenance_margin", DoubleType(), required=False),
        NestedField(35, "last_maintenance_margin", DoubleType(), required=False),
        NestedField(36, "sma", DoubleType(), required=False),
        NestedField(37, "multiplier", DoubleType(), required=False),
        # Restrictions
        NestedField(38, "pattern_day_trader", BooleanType(), required=False),
        NestedField(39, "daytrade_count", LongType(), required=False),
        NestedField(40, "shorting_enabled", BooleanType(), required=False),
        NestedField(41, "trading_blocked", BooleanType(), required=False),
        NestedField(42, "account_blocked", BooleanType(), required=False),
        NestedField(43, "trade_suspended_by_user", BooleanType(), required=False),
        NestedField(44, "transfers_blocked", BooleanType(), required=False),
        # Fees and pending
        NestedField(45, "accrued_fees", DoubleType(), required=False),
        NestedField(46, "pending_transfer_in", DoubleType(), required=False),
        NestedField(47, "pending_transfer_out", DoubleType(), required=False),
        # Timestamps
        NestedField(48, "created_at", StringType(), required=False),
        NestedField(49, "balance_asof", StringType(), required=False),
    )


def get_silver_alpaca_order_schema() -> Schema:
    """Iceberg schema for Alpaca orders.

    Matches Alpaca Trading API /v2/orders response (33 fields).
    """
    return Schema(
        *SILVER_BASE_FIELDS,
        # Identity
        NestedField(20, "order_id", StringType(), required=True),
        NestedField(21, "client_order_id", StringType(), required=False),
        NestedField(22, "symbol", StringType(), required=True),
        NestedField(23, "asset_id", StringType(), required=False),
        NestedField(24, "asset_class", StringType(), required=False),
        # Quantities
        NestedField(25, "qty", DoubleType(), required=False),
        NestedField(26, "filled_qty", DoubleType(), required=False),
        NestedField(27, "notional", DoubleType(), required=False),
        NestedField(28, "filled_avg_price", DoubleType(), required=False),
        # Order type
        NestedField(29, "order_type", StringType(), required=True),
        NestedField(30, "side", StringType(), required=True),
        NestedField(31, "time_in_force", StringType(), required=True),
        NestedField(32, "order_class", StringType(), required=False),
        # Prices
        NestedField(33, "limit_price", DoubleType(), required=False),
        NestedField(34, "stop_price", DoubleType(), required=False),
        NestedField(35, "trail_percent", DoubleType(), required=False),
        NestedField(36, "trail_price", DoubleType(), required=False),
        # Status
        NestedField(37, "status", StringType(), required=True),
        NestedField(38, "extended_hours", BooleanType(), required=False),
        NestedField(39, "legs", StringType(), required=False),  # JSON array
        # Timestamps
        NestedField(40, "created_at", StringType(), required=False),
        NestedField(41, "submitted_at", StringType(), required=False),
        NestedField(42, "filled_at", StringType(), required=False),
        NestedField(43, "expired_at", StringType(), required=False),
        NestedField(44, "canceled_at", StringType(), required=False),
        NestedField(45, "failed_at", StringType(), required=False),
        NestedField(46, "replaced_at", StringType(), required=False),
        # Links
        NestedField(47, "replaced_by", StringType(), required=False),
        NestedField(48, "replaces", StringType(), required=False),
    )


def get_silver_alpaca_position_schema() -> Schema:
    """Iceberg schema for Alpaca positions.

    Matches Alpaca Trading API /v2/positions response (18 fields).
    """
    return Schema(
        *SILVER_BASE_FIELDS,
        # Identity
        NestedField(20, "asset_id", StringType(), required=True),
        NestedField(21, "symbol", StringType(), required=True),
        NestedField(22, "exchange", StringType(), required=False),
        NestedField(23, "asset_class", StringType(), required=False),
        # Holdings
        NestedField(24, "qty", DoubleType(), required=True),
        NestedField(25, "qty_available", DoubleType(), required=False),
        NestedField(26, "side", StringType(), required=False),
        NestedField(27, "avg_entry_price", DoubleType(), required=True),
        NestedField(28, "cost_basis", DoubleType(), required=False),
        # Values
        NestedField(29, "market_value", DoubleType(), required=False),
        NestedField(30, "current_price", DoubleType(), required=False),
        NestedField(31, "lastday_price", DoubleType(), required=False),
        NestedField(32, "change_today", DoubleType(), required=False),
        # P&L
        NestedField(33, "unrealized_pl", DoubleType(), required=False),
        NestedField(34, "unrealized_plpc", DoubleType(), required=False),
        NestedField(35, "unrealized_intraday_pl", DoubleType(), required=False),
        NestedField(36, "unrealized_intraday_plpc", DoubleType(), required=False),
        NestedField(37, "asset_marginable", BooleanType(), required=False),
    )


def get_silver_alpaca_portfolio_history_schema() -> Schema:
    """Iceberg schema for Alpaca portfolio history.

    Matches Alpaca Trading API /v2/account/portfolio/history response (8 fields).
    Note: Arrays stored as JSON strings for Iceberg compatibility.
    """
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "timestamps", StringType(), required=False),  # JSON array
        NestedField(21, "equities", StringType(), required=False),  # JSON array
        NestedField(22, "profit_losses", StringType(), required=False),  # JSON array
        NestedField(23, "profit_loss_pcts", StringType(), required=False),  # JSON array
        NestedField(24, "base_value", DoubleType(), required=False),
        NestedField(25, "base_value_asof", StringType(), required=False),
        NestedField(26, "timeframe", StringType(), required=False),
    )


def get_silver_alpaca_activity_schema() -> Schema:
    """Iceberg schema for Alpaca account activities.

    Matches Alpaca Trading API /v2/account/activities response.
    Combines TradeActivity and NonTradeActivity fields.
    """
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "activity_id", StringType(), required=True),
        NestedField(21, "activity_type", StringType(), required=True),
        NestedField(22, "transaction_time", StringType(), required=False),
        # Trade activity fields
        NestedField(23, "symbol", StringType(), required=False),
        NestedField(24, "side", StringType(), required=False),
        NestedField(25, "qty", DoubleType(), required=False),
        NestedField(26, "price", DoubleType(), required=False),
        NestedField(27, "net_amount", DoubleType(), required=False),
        NestedField(28, "order_id", StringType(), required=False),
        NestedField(29, "cum_qty", DoubleType(), required=False),
        NestedField(30, "leaves_qty", DoubleType(), required=False),
        # Non-trade activity fields
        NestedField(31, "date", StringType(), required=False),
        NestedField(32, "description", StringType(), required=False),
        NestedField(33, "status", StringType(), required=False),
    )


def get_silver_alpaca_asset_schema() -> Schema:
    """Iceberg schema for Alpaca assets.

    Matches Alpaca Trading API /v2/assets response.
    """
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "asset_id", StringType(), required=True),
        NestedField(21, "symbol", StringType(), required=True),
        NestedField(22, "name", StringType(), required=False),
        NestedField(23, "exchange", StringType(), required=False),
        NestedField(24, "asset_class", StringType(), required=False),
        NestedField(25, "status", StringType(), required=False),
        # Trading attributes
        NestedField(26, "tradable", BooleanType(), required=False),
        NestedField(27, "marginable", BooleanType(), required=False),
        NestedField(28, "shortable", BooleanType(), required=False),
        NestedField(29, "easy_to_borrow", BooleanType(), required=False),
        NestedField(30, "fractionable", BooleanType(), required=False),
        # Margin requirements
        NestedField(31, "maintenance_margin_requirement", DoubleType(), required=False),
        NestedField(32, "min_order_size", DoubleType(), required=False),
        NestedField(33, "min_trade_increment", DoubleType(), required=False),
        NestedField(34, "price_increment", DoubleType(), required=False),
    )


def get_silver_alpaca_option_contract_schema() -> Schema:
    """Iceberg schema for Alpaca option contracts.

    Matches Alpaca Trading API /v2/options/contracts response (19 fields).
    """
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "contract_id", StringType(), required=True),
        NestedField(21, "symbol", StringType(), required=True),
        NestedField(22, "name", StringType(), required=False),
        NestedField(23, "status", StringType(), required=False),
        NestedField(24, "tradable", BooleanType(), required=False),
        # Option details
        NestedField(25, "root_symbol", StringType(), required=False),
        NestedField(26, "underlying_symbol", StringType(), required=True),
        NestedField(27, "underlying_asset_id", StringType(), required=False),
        NestedField(28, "option_type", StringType(), required=True),  # call/put
        NestedField(29, "option_style", StringType(), required=False),  # american/european
        NestedField(30, "strike_price", DoubleType(), required=True),
        NestedField(31, "expiration_date", StringType(), required=True),
        NestedField(32, "multiplier", DoubleType(), required=False),
        NestedField(33, "contract_size", DoubleType(), required=False),
        # Market data
        NestedField(34, "open_interest", LongType(), required=False),
        NestedField(35, "open_interest_date", StringType(), required=False),
        NestedField(36, "close_price", DoubleType(), required=False),
        NestedField(37, "close_price_date", StringType(), required=False),
    )


# Iceberg schema registry for table creation
# Named ICEBERG_SILVER_SCHEMAS to avoid collision with schemas/silver.py Arrow schemas
ICEBERG_SILVER_SCHEMAS: dict[str, Schema] = {
    "bars": get_silver_bars_schema(),
    "quotes": get_silver_quotes_schema(),
    "trades": get_silver_trades_schema(),
    "flow_alerts": get_silver_flow_alerts_schema(),
    "darkpool": get_silver_darkpool_schema(),
    "congress_trades": get_silver_congress_trades_schema(),
    "insider_trades": get_silver_insider_trades_schema(),
    "market_tide": get_silver_market_tide_schema(),
    "greek_exposure": get_silver_greek_exposure_schema(),
    "user_alerts": get_silver_user_alerts_schema(),
    "earnings": get_silver_earnings_schema(),
    "etf_info": get_silver_etf_info_schema(),
    "etf_holdings": get_silver_etf_holdings_schema(),
    "etf_exposure": get_silver_etf_exposure_schema(),
    "etf_inflow_outflow": get_silver_etf_inflow_outflow_schema(),
    "etf_weights": get_silver_etf_weights_schema(),
    "group_flow": get_silver_group_flow_schema(),
    "insider_flow": get_silver_insider_flow_schema(),
    "ticker_insiders": get_silver_ticker_insiders_schema(),
    "institution_activity": get_silver_institution_activity_schema(),
    "institution_holdings": get_silver_institution_holdings_schema(),
    "institution_sector_exposure": get_silver_institution_sector_exposure_schema(),
    "institutional_ownership": get_silver_institutional_ownership_schema(),
    # Alpaca Trading API schemas
    "alpaca_account": get_silver_alpaca_account_schema(),
    "alpaca_order": get_silver_alpaca_order_schema(),
    "alpaca_position": get_silver_alpaca_position_schema(),
    "alpaca_portfolio_history": get_silver_alpaca_portfolio_history_schema(),
    "alpaca_activity": get_silver_alpaca_activity_schema(),
    "alpaca_asset": get_silver_alpaca_asset_schema(),
    "alpaca_option_contract": get_silver_alpaca_option_contract_schema(),
}


# =============================================================================
# Table Management
# =============================================================================


def _build_daily_partition_spec(schema: Schema) -> PartitionSpec:
    """Build daily partition spec for ts_event."""
    ts_event_field = schema.find_field("ts_event")
    return PartitionSpec(
        PartitionField(
            source_id=ts_event_field.field_id,
            field_id=1000,
            transform=DayTransform(),
            name="ts_event_day",
        )
    )


def create_silver_table(
    catalog: Catalog,
    table_name: str,
    namespace: str = "silver",
) -> Table:
    """Create a Silver table in Iceberg if it doesn't exist.

    Args:
        catalog: Iceberg catalog instance
        table_name: Name of the table (e.g., "bars", "quotes")
        namespace: Iceberg namespace (default: "silver")

    Returns:
        The created or existing Table
    """
    full_name = f"{namespace}.{table_name}"

    # Check if table exists
    try:
        return catalog.load_table(full_name)
    except Exception:
        pass  # Table doesn't exist, create it

    # Get schema
    if table_name not in ICEBERG_SILVER_SCHEMAS:
        raise ValueError(f"Unknown Silver table: {table_name}")

    schema = ICEBERG_SILVER_SCHEMAS[table_name]

    logger.info("creating_iceberg_table", table=full_name)

    # Create namespace if needed
    with contextlib.suppress(Exception):
        catalog.create_namespace(namespace)

    partition_spec = _build_daily_partition_spec(schema)

    # Create table with partitioning by date
    return catalog.create_table(
        identifier=full_name,
        schema=schema,
        partition_spec=partition_spec,
    )


def initialize_silver_tables(catalog: Catalog | None = None) -> dict[str, Table]:
    """Initialize all Silver tables.

    Args:
        catalog: Optional catalog instance. If None, uses default.

    Returns:
        Dictionary of table_name -> Table
    """
    if catalog is None:
        catalog = get_iceberg_catalog()

    tables = {}
    for table_name in ICEBERG_SILVER_SCHEMAS:
        tables[table_name] = create_silver_table(catalog, table_name)
        logger.info("silver_table_ready", table=table_name)

    return tables
