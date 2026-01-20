"""Tests for Additional Dataset Schemas (PRD §57)."""

from datetime import datetime, date, UTC
from decimal import Decimal

import pytest

from heber.schemas.additional import (
    # Market Data
    DailyBar,
    # Options
    OptionType,
    OptionQuote,
    OptionTrade,
    # Alternative
    TransactionType,
    CongressTrade,
    LobbyingDisclosure,
    # Fundamentals
    CompanyInfo,
    IncomeStatement,
    BalanceSheet,
    CashFlow,
    FinancialRatios,
    # Economic
    EconomicIndicator,
    InterestRate,
    TreasuryYield,
    # Forex & Crypto
    ForexRate,
    CryptoBar,
    CryptoQuote,
    # Registry
    get_schema_class,
    list_additional_schemas,
)


class TestDailyBar:
    """Test DailyBar schema."""
    
    def test_to_dict(self):
        bar = DailyBar(
            event_id="bar-001",
            symbol="AAPL",
            ts_event=datetime(2025, 1, 15, 21, 0, 0, tzinfo=UTC),
            ts_available=datetime(2025, 1, 15, 21, 0, 0, tzinfo=UTC),
            date=date(2025, 1, 15),
            open=Decimal("150.00"),
            high=Decimal("152.00"),
            low=Decimal("149.00"),
            close=Decimal("151.00"),
            volume=50000000,
        )
        
        d = bar.to_dict()
        
        assert d["symbol"] == "AAPL"
        assert d["date"] == "2025-01-15"


class TestOptionQuote:
    """Test OptionQuote schema."""
    
    def test_to_dict(self):
        quote = OptionQuote(
            event_id="oq-001",
            underlying_symbol="AAPL",
            option_symbol="AAPL250117C00150000",
            ts_event=datetime(2025, 1, 15, 10, 0, 0, tzinfo=UTC),
            ts_available=datetime(2025, 1, 15, 10, 0, 0, tzinfo=UTC),
            option_type=OptionType.CALL,
            strike=Decimal("150.00"),
            expiration=date(2025, 1, 17),
            bid=Decimal("2.50"),
            ask=Decimal("2.55"),
            bid_size=100,
            ask_size=50,
            implied_volatility=Decimal("0.25"),
            delta=Decimal("0.55"),
        )
        
        d = quote.to_dict()
        
        assert d["option_type"] == "call"
        assert d["strike"] == "150.00"


class TestCongressTrade:
    """Test CongressTrade schema."""
    
    def test_to_dict(self):
        trade = CongressTrade(
            event_id="ct-001",
            ts_event=datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC),
            ts_available=datetime(2025, 1, 16, 12, 0, 0, tzinfo=UTC),
            politician="Nancy Pelosi",
            state="CA",
            party="D",
            chamber="House",
            symbol="NVDA",
            transaction_type=TransactionType.BUY,
            amount_min=100000,
            amount_max=500000,
            disclosure_date=date(2025, 1, 16),
            trade_date=date(2025, 1, 15),
        )
        
        d = trade.to_dict()
        
        assert d["politician"] == "Nancy Pelosi"
        assert d["transaction_type"] == "buy"


class TestLobbyingDisclosure:
    """Test LobbyingDisclosure schema."""
    
    def test_to_dict(self):
        lobby = LobbyingDisclosure(
            event_id="ld-001",
            ts_event=datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC),
            ts_available=datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC),
            client_name="Big Tech Corp",
            registrant_name="K Street Lobbying",
            filing_year=2025,
            filing_quarter=1,
            amount=Decimal("500000"),
            issues=["Technology", "Privacy"],
            related_symbols=["TECH"],
        )
        
        d = lobby.to_dict()
        
        assert d["amount"] == "500000"
        assert len(d["issues"]) == 2


class TestFundamentals:
    """Test fundamental schemas."""
    
    def test_income_statement(self):
        income = IncomeStatement(
            event_id="is-001",
            symbol="AAPL",
            ts_event=datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC),
            ts_available=datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC),
            fiscal_date=date(2024, 12, 31),
            fiscal_quarter=4,
            revenue=Decimal("100000000000"),
            net_income=Decimal("25000000000"),
            eps=Decimal("1.50"),
        )
        
        d = income.to_dict()
        
        assert d["fiscal_quarter"] == 4
    
    def test_balance_sheet(self):
        balance = BalanceSheet(
            event_id="bs-001",
            symbol="AAPL",
            ts_event=datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC),
            ts_available=datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC),
            fiscal_date=date(2024, 12, 31),
            total_assets=Decimal("500000000000"),
            total_liabilities=Decimal("200000000000"),
            total_equity=Decimal("300000000000"),
        )
        
        d = balance.to_dict()
        
        assert d["symbol"] == "AAPL"
    
    def test_cash_flow(self):
        cf = CashFlow(
            event_id="cf-001",
            symbol="AAPL",
            ts_event=datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC),
            ts_available=datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC),
            fiscal_date=date(2024, 12, 31),
            operating_cash_flow=Decimal("50000000000"),
            free_cash_flow=Decimal("40000000000"),
        )
        
        d = cf.to_dict()
        
        assert "operating_cash_flow" in d


class TestEconomic:
    """Test economic schemas."""
    
    def test_economic_indicator(self):
        gdp = EconomicIndicator(
            event_id="ei-001",
            indicator="gdp",
            ts_event=datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC),
            ts_available=datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC),
            date=date(2024, 12, 31),
            value=Decimal("27000000000000"),
            country="US",
            unit="USD",
            frequency="quarterly",
        )
        
        d = gdp.to_dict()
        
        assert d["indicator"] == "gdp"
    
    def test_treasury_yield(self):
        yield_ = TreasuryYield(
            event_id="ty-001",
            maturity="10y",
            ts_event=datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC),
            ts_available=datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC),
            date=date(2025, 1, 15),
            yield_value=Decimal("4.25"),
        )
        
        d = yield_.to_dict()
        
        assert d["maturity"] == "10y"


class TestForexCrypto:
    """Test forex and crypto schemas."""
    
    def test_forex_rate(self):
        fx = ForexRate(
            event_id="fx-001",
            base_currency="EUR",
            quote_currency="USD",
            ts_event=datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC),
            ts_available=datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC),
            rate=Decimal("1.0850"),
        )
        
        d = fx.to_dict()
        
        assert d["pair"] == "EUR/USD"
        assert fx.pair == "EUR/USD"
    
    def test_crypto_bar(self):
        btc = CryptoBar(
            event_id="cb-001",
            symbol="BTC/USD",
            ts_event=datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC),
            ts_available=datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC),
            open=Decimal("42000.00"),
            high=Decimal("42500.00"),
            low=Decimal("41800.00"),
            close=Decimal("42200.00"),
            volume=Decimal("1500.5"),
        )
        
        d = btc.to_dict()
        
        assert d["symbol"] == "BTC/USD"


class TestSchemaRegistry:
    """Test schema registry."""
    
    def test_list_additional_schemas(self):
        schemas = list_additional_schemas()
        
        assert len(schemas) == 16
        assert "bars_daily" in schemas
        assert "option_quotes" in schemas
    
    def test_get_schema_class(self):
        cls = get_schema_class("congress_trades")
        
        assert cls == CongressTrade
    
    def test_get_unknown_schema(self):
        cls = get_schema_class("unknown")
        
        assert cls is None


def run_all_additional_schema_tests() -> dict[str, bool]:
    """Run all additional schema tests."""
    results = {}
    
    test_classes = [
        TestDailyBar,
        TestOptionQuote,
        TestCongressTrade,
        TestLobbyingDisclosure,
        TestFundamentals,
        TestEconomic,
        TestForexCrypto,
        TestSchemaRegistry,
    ]
    
    for test_class in test_classes:
        instance = test_class()
        for method_name in dir(instance):
            if method_name.startswith("test_"):
                try:
                    getattr(instance, method_name)()
                    results[f"{test_class.__name__}.{method_name}"] = True
                except Exception as e:
                    results[f"{test_class.__name__}.{method_name}"] = False
                    print(f"FAILED: {test_class.__name__}.{method_name}: {e}")
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"\nAdditional Schema Tests: {passed}/{total} passed")
    
    return results


if __name__ == "__main__":
    run_all_additional_schema_tests()
