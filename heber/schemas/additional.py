"""Additional Dataset Schemas (PRD §57).

Dataset schemas beyond core market data.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


# =============================================================================
# Market Data (§57.1)
# =============================================================================


@dataclass
class DailyBar:
    """Daily OHLCV bar schema."""

    event_id: str
    symbol: str
    ts_event: datetime
    ts_available: datetime
    date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    adjusted_close: Decimal | None = None
    dividend: Decimal | None = None
    split_factor: Decimal | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "symbol": self.symbol,
            "ts_event": self.ts_event.isoformat(),
            "ts_available": self.ts_available.isoformat(),
            "date": self.date.isoformat(),
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "close": str(self.close),
            "volume": self.volume,
            "adjusted_close": str(self.adjusted_close) if self.adjusted_close else None,
            "dividend": str(self.dividend) if self.dividend else None,
            "split_factor": str(self.split_factor) if self.split_factor else None,
        }


# =============================================================================
# Options (§57.2)
# =============================================================================


class OptionType(str, Enum):
    """Option type."""

    CALL = "call"
    PUT = "put"


@dataclass
class OptionQuote:
    """Option quote schema."""

    event_id: str
    underlying_symbol: str
    option_symbol: str
    ts_event: datetime
    ts_available: datetime
    option_type: OptionType
    strike: Decimal
    expiration: date
    bid: Decimal
    ask: Decimal
    bid_size: int
    ask_size: int
    last: Decimal | None = None
    volume: int = 0
    open_interest: int = 0
    implied_volatility: Decimal | None = None
    delta: Decimal | None = None
    gamma: Decimal | None = None
    theta: Decimal | None = None
    vega: Decimal | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "underlying_symbol": self.underlying_symbol,
            "option_symbol": self.option_symbol,
            "ts_event": self.ts_event.isoformat(),
            "ts_available": self.ts_available.isoformat(),
            "option_type": self.option_type.value,
            "strike": str(self.strike),
            "expiration": self.expiration.isoformat(),
            "bid": str(self.bid),
            "ask": str(self.ask),
            "bid_size": self.bid_size,
            "ask_size": self.ask_size,
            "last": str(self.last) if self.last else None,
            "volume": self.volume,
            "open_interest": self.open_interest,
            "implied_volatility": str(self.implied_volatility) if self.implied_volatility else None,
            "delta": str(self.delta) if self.delta else None,
            "gamma": str(self.gamma) if self.gamma else None,
            "theta": str(self.theta) if self.theta else None,
            "vega": str(self.vega) if self.vega else None,
        }


@dataclass
class OptionTrade:
    """Option trade schema."""

    event_id: str
    underlying_symbol: str
    option_symbol: str
    ts_event: datetime
    ts_available: datetime
    option_type: OptionType
    strike: Decimal
    expiration: date
    price: Decimal
    size: int
    exchange: str = ""
    conditions: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "underlying_symbol": self.underlying_symbol,
            "option_symbol": self.option_symbol,
            "ts_event": self.ts_event.isoformat(),
            "ts_available": self.ts_available.isoformat(),
            "option_type": self.option_type.value,
            "strike": str(self.strike),
            "expiration": self.expiration.isoformat(),
            "price": str(self.price),
            "size": self.size,
            "exchange": self.exchange,
            "conditions": self.conditions,
        }


# =============================================================================
# Alternative Data (§57.3)
# =============================================================================


class TransactionType(str, Enum):
    """Transaction type for trades."""

    BUY = "buy"
    SELL = "sell"
    EXCHANGE = "exchange"


@dataclass
class CongressTrade:
    """Congress trading disclosure schema."""

    event_id: str
    ts_event: datetime
    ts_available: datetime
    politician: str
    state: str
    party: str
    chamber: str  # House or Senate
    symbol: str
    transaction_type: TransactionType
    amount_min: int
    amount_max: int
    disclosure_date: date
    trade_date: date | None = None
    asset_type: str = "stock"

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "ts_event": self.ts_event.isoformat(),
            "ts_available": self.ts_available.isoformat(),
            "politician": self.politician,
            "state": self.state,
            "party": self.party,
            "chamber": self.chamber,
            "symbol": self.symbol,
            "transaction_type": self.transaction_type.value,
            "amount_min": self.amount_min,
            "amount_max": self.amount_max,
            "disclosure_date": self.disclosure_date.isoformat(),
            "trade_date": self.trade_date.isoformat() if self.trade_date else None,
            "asset_type": self.asset_type,
        }


@dataclass
class LobbyingDisclosure:
    """Lobbying disclosure schema."""

    event_id: str
    ts_event: datetime
    ts_available: datetime
    client_name: str
    registrant_name: str
    filing_year: int
    filing_quarter: int
    amount: Decimal
    issues: list[str] = field(default_factory=list)
    lobbyists: list[str] = field(default_factory=list)
    related_symbols: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "ts_event": self.ts_event.isoformat(),
            "ts_available": self.ts_available.isoformat(),
            "client_name": self.client_name,
            "registrant_name": self.registrant_name,
            "filing_year": self.filing_year,
            "filing_quarter": self.filing_quarter,
            "amount": str(self.amount),
            "issues": self.issues,
            "lobbyists": self.lobbyists,
            "related_symbols": self.related_symbols,
        }


# =============================================================================
# Fundamentals (§57.4)
# =============================================================================


@dataclass
class CompanyInfo:
    """Company information schema."""

    event_id: str
    symbol: str
    ts_event: datetime
    ts_available: datetime
    name: str
    exchange: str
    sector: str = ""
    industry: str = ""
    market_cap: int | None = None
    employees: int | None = None
    description: str = ""
    cik: str = ""
    cusip: str = ""
    isin: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "symbol": self.symbol,
            "ts_event": self.ts_event.isoformat(),
            "ts_available": self.ts_available.isoformat(),
            "name": self.name,
            "exchange": self.exchange,
            "sector": self.sector,
            "industry": self.industry,
            "market_cap": self.market_cap,
            "employees": self.employees,
            "description": self.description,
            "cik": self.cik,
            "cusip": self.cusip,
            "isin": self.isin,
        }


@dataclass
class IncomeStatement:
    """Income statement schema."""

    event_id: str
    symbol: str
    ts_event: datetime
    ts_available: datetime
    fiscal_date: date
    fiscal_quarter: int | None = None
    revenue: Decimal | None = None
    gross_profit: Decimal | None = None
    operating_income: Decimal | None = None
    net_income: Decimal | None = None
    eps: Decimal | None = None
    eps_diluted: Decimal | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "symbol": self.symbol,
            "ts_event": self.ts_event.isoformat(),
            "ts_available": self.ts_available.isoformat(),
            "fiscal_date": self.fiscal_date.isoformat(),
            "fiscal_quarter": self.fiscal_quarter,
            "revenue": str(self.revenue) if self.revenue else None,
            "gross_profit": str(self.gross_profit) if self.gross_profit else None,
            "operating_income": str(self.operating_income) if self.operating_income else None,
            "net_income": str(self.net_income) if self.net_income else None,
            "eps": str(self.eps) if self.eps else None,
            "eps_diluted": str(self.eps_diluted) if self.eps_diluted else None,
        }


@dataclass
class BalanceSheet:
    """Balance sheet schema."""

    event_id: str
    symbol: str
    ts_event: datetime
    ts_available: datetime
    fiscal_date: date
    total_assets: Decimal | None = None
    total_liabilities: Decimal | None = None
    total_equity: Decimal | None = None
    cash: Decimal | None = None
    short_term_debt: Decimal | None = None
    long_term_debt: Decimal | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "symbol": self.symbol,
            "ts_event": self.ts_event.isoformat(),
            "ts_available": self.ts_available.isoformat(),
            "fiscal_date": self.fiscal_date.isoformat(),
            "total_assets": str(self.total_assets) if self.total_assets else None,
            "total_liabilities": str(self.total_liabilities) if self.total_liabilities else None,
            "total_equity": str(self.total_equity) if self.total_equity else None,
            "cash": str(self.cash) if self.cash else None,
            "short_term_debt": str(self.short_term_debt) if self.short_term_debt else None,
            "long_term_debt": str(self.long_term_debt) if self.long_term_debt else None,
        }


@dataclass
class CashFlow:
    """Cash flow statement schema."""

    event_id: str
    symbol: str
    ts_event: datetime
    ts_available: datetime
    fiscal_date: date
    operating_cash_flow: Decimal | None = None
    investing_cash_flow: Decimal | None = None
    financing_cash_flow: Decimal | None = None
    free_cash_flow: Decimal | None = None
    capital_expenditures: Decimal | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "symbol": self.symbol,
            "ts_event": self.ts_event.isoformat(),
            "ts_available": self.ts_available.isoformat(),
            "fiscal_date": self.fiscal_date.isoformat(),
            "operating_cash_flow": str(self.operating_cash_flow) if self.operating_cash_flow else None,
            "investing_cash_flow": str(self.investing_cash_flow) if self.investing_cash_flow else None,
            "financing_cash_flow": str(self.financing_cash_flow) if self.financing_cash_flow else None,
            "free_cash_flow": str(self.free_cash_flow) if self.free_cash_flow else None,
            "capital_expenditures": str(self.capital_expenditures) if self.capital_expenditures else None,
        }


@dataclass
class FinancialRatios:
    """Financial ratios schema."""

    event_id: str
    symbol: str
    ts_event: datetime
    ts_available: datetime
    fiscal_date: date
    pe_ratio: Decimal | None = None
    pb_ratio: Decimal | None = None
    ps_ratio: Decimal | None = None
    debt_to_equity: Decimal | None = None
    current_ratio: Decimal | None = None
    quick_ratio: Decimal | None = None
    roe: Decimal | None = None
    roa: Decimal | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "symbol": self.symbol,
            "ts_event": self.ts_event.isoformat(),
            "ts_available": self.ts_available.isoformat(),
            "fiscal_date": self.fiscal_date.isoformat(),
            "pe_ratio": str(self.pe_ratio) if self.pe_ratio else None,
            "pb_ratio": str(self.pb_ratio) if self.pb_ratio else None,
            "ps_ratio": str(self.ps_ratio) if self.ps_ratio else None,
            "debt_to_equity": str(self.debt_to_equity) if self.debt_to_equity else None,
            "current_ratio": str(self.current_ratio) if self.current_ratio else None,
            "quick_ratio": str(self.quick_ratio) if self.quick_ratio else None,
            "roe": str(self.roe) if self.roe else None,
            "roa": str(self.roa) if self.roa else None,
        }


# =============================================================================
# Economic (§57.5)
# =============================================================================


@dataclass
class EconomicIndicator:
    """Economic indicator schema."""

    event_id: str
    indicator: str  # gdp, cpi, unemployment, etc.
    ts_event: datetime
    ts_available: datetime
    date: date
    value: Decimal
    country: str = "US"
    unit: str = ""
    frequency: str = ""  # monthly, quarterly, annual

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "indicator": self.indicator,
            "ts_event": self.ts_event.isoformat(),
            "ts_available": self.ts_available.isoformat(),
            "date": self.date.isoformat(),
            "value": str(self.value),
            "country": self.country,
            "unit": self.unit,
            "frequency": self.frequency,
        }


@dataclass
class InterestRate:
    """Interest rate schema."""

    event_id: str
    rate_type: str  # fed_funds, prime, libor
    ts_event: datetime
    ts_available: datetime
    date: date
    rate: Decimal
    country: str = "US"

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "rate_type": self.rate_type,
            "ts_event": self.ts_event.isoformat(),
            "ts_available": self.ts_available.isoformat(),
            "date": self.date.isoformat(),
            "rate": str(self.rate),
            "country": self.country,
        }


@dataclass
class TreasuryYield:
    """Treasury yield schema."""

    event_id: str
    maturity: str  # 1m, 3m, 6m, 1y, 2y, 5y, 10y, 30y
    ts_event: datetime
    ts_available: datetime
    date: date
    yield_value: Decimal
    country: str = "US"

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "maturity": self.maturity,
            "ts_event": self.ts_event.isoformat(),
            "ts_available": self.ts_available.isoformat(),
            "date": self.date.isoformat(),
            "yield": str(self.yield_value),
            "country": self.country,
        }


# =============================================================================
# Forex & Crypto (§57.6)
# =============================================================================


@dataclass
class ForexRate:
    """Forex exchange rate schema."""

    event_id: str
    base_currency: str
    quote_currency: str
    ts_event: datetime
    ts_available: datetime
    rate: Decimal
    bid: Decimal | None = None
    ask: Decimal | None = None

    @property
    def pair(self) -> str:
        return f"{self.base_currency}/{self.quote_currency}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "base_currency": self.base_currency,
            "quote_currency": self.quote_currency,
            "pair": self.pair,
            "ts_event": self.ts_event.isoformat(),
            "ts_available": self.ts_available.isoformat(),
            "rate": str(self.rate),
            "bid": str(self.bid) if self.bid else None,
            "ask": str(self.ask) if self.ask else None,
        }


@dataclass
class CryptoBar:
    """Cryptocurrency OHLCV bar schema."""

    event_id: str
    symbol: str  # BTC/USD, ETH/USD
    ts_event: datetime
    ts_available: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    exchange: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "symbol": self.symbol,
            "ts_event": self.ts_event.isoformat(),
            "ts_available": self.ts_available.isoformat(),
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "close": str(self.close),
            "volume": str(self.volume),
            "exchange": self.exchange,
        }


@dataclass
class CryptoQuote:
    """Cryptocurrency quote schema."""

    event_id: str
    symbol: str
    ts_event: datetime
    ts_available: datetime
    bid: Decimal
    ask: Decimal
    bid_size: Decimal
    ask_size: Decimal
    exchange: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "symbol": self.symbol,
            "ts_event": self.ts_event.isoformat(),
            "ts_available": self.ts_available.isoformat(),
            "bid": str(self.bid),
            "ask": str(self.ask),
            "bid_size": str(self.bid_size),
            "ask_size": str(self.ask_size),
            "exchange": self.exchange,
        }


# =============================================================================
# Schema Registry
# =============================================================================

ADDITIONAL_DATASET_SCHEMAS = {
    # Market Data
    "bars_daily": DailyBar,
    # Options
    "option_quotes": OptionQuote,
    "option_trades": OptionTrade,
    # Alternative
    "congress_trades": CongressTrade,
    "lobbying": LobbyingDisclosure,
    # Fundamentals
    "company_info": CompanyInfo,
    "income_statement": IncomeStatement,
    "balance_sheet": BalanceSheet,
    "cash_flow": CashFlow,
    "ratios": FinancialRatios,
    # Economic
    "economic_indicators": EconomicIndicator,
    "interest_rate": InterestRate,
    "treasury_yield": TreasuryYield,
    # Forex & Crypto
    "forex_rates": ForexRate,
    "crypto_bars": CryptoBar,
    "crypto_quotes": CryptoQuote,
}


def get_schema_class(dataset_name: str) -> type | None:
    """Get schema class for a dataset."""
    return ADDITIONAL_DATASET_SCHEMAS.get(dataset_name)


def list_additional_schemas() -> list[str]:
    """List all additional dataset schemas."""
    return list(ADDITIONAL_DATASET_SCHEMAS.keys())
