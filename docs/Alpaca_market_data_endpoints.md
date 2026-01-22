# Alpaca Market Data API Endpoint Coverage

This document tracks the implementation status of Alpaca Market Data API endpoints in the Data Gateway and Heber data pipeline.

**Legend:**

- ✅ = Complete (Gateway method exists, Heber schema available)
- 🔄 = In Progress (Gateway exists, needs Heber schema)
- ❌ = Not Started

---

## Stocks Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `GET /v2/stocks/bars` | Historical bars | ✅ `get_bars` | ✅ `bars` | ✅ |
| `GET /v2/stocks/bars/latest` | Latest bars | ✅ `get_latest_bars` | ✅ `bars` | ✅ |
| `GET /v2/stocks/quotes` | Historical quotes | ✅ `get_historical_quotes` | ✅ `quotes` | ✅ |
| `GET /v2/stocks/quotes/latest` | Latest quotes | ✅ `get_quotes` | ✅ `quotes` | ✅ |
| `GET /v2/stocks/trades` | Historical trades | ✅ `get_trades` | ✅ `trades` | ✅ |
| `GET /v2/stocks/trades/latest` | Latest trades | ✅ `get_latest_trades` | ✅ `trades` | ✅ |
| `GET /v2/stocks/snapshots` | Stock snapshots | ✅ `get_snapshots` | ✅ JSON blob | ✅ |
| `GET /v2/stocks/auctions` | Auction data | ✅ `get_auctions` | ✅ JSON blob | ✅ |
| `GET /v2/stocks/{symbol}/bars` | Symbol bars | ✅ `get_bars` | ✅ `bars` | ✅ |
| `GET /v2/stocks/{symbol}/bars/latest` | Symbol latest bar | ✅ `get_latest_bars` | ✅ `bars` | ✅ |
| `GET /v2/stocks/{symbol}/quotes` | Symbol quotes | ✅ `get_historical_quotes` | ✅ `quotes` | ✅ |
| `GET /v2/stocks/{symbol}/quotes/latest` | Symbol latest quote | ✅ `get_quotes` | ✅ `quotes` | ✅ |
| `GET /v2/stocks/{symbol}/trades` | Symbol trades | ✅ `get_trades` | ✅ `trades` | ✅ |
| `GET /v2/stocks/{symbol}/trades/latest` | Symbol latest trade | ✅ `get_latest_trades` | ✅ `trades` | ✅ |
| `GET /v2/stocks/{symbol}/auctions` | Symbol auctions | ✅ `get_auctions` | ✅ JSON blob | ✅ |
| `GET /v2/stocks/{symbol}/snapshots` | Symbol snapshot | ✅ `get_snapshots` | ✅ JSON blob | ✅ |
| `GET /v2/stocks/meta/conditions/{ticktype}` | Condition codes | ✅ `get_condition_codes` | ✅ JSON blob | ✅ |
| `GET /v2/stocks/meta/exchanges` | Exchange codes | ✅ `get_exchange_codes` | ✅ JSON blob | ✅ |

---

## Options Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `GET /v1beta1/options/bars` | Option bars | ✅ `get_option_bars` | ✅ `bars` | ✅ |
| `GET /v1beta1/options/quotes/latest` | Latest option quotes | ✅ `get_option_quotes` | ✅ `quotes` | ✅ |
| `GET /v1beta1/options/trades` | Option trades | ✅ `get_option_trades` | ✅ `trades` | ✅ |
| `GET /v1beta1/options/trades/latest` | Latest option trades | ✅ `get_option_latest_trades` | ✅ `trades` | ✅ |
| `GET /v1beta1/options/snapshots` | Option snapshots | ✅ `get_option_snapshots` | ✅ `alpaca_option_contract` | ✅ |
| `GET /v1beta1/options/snapshots/{underlying}` | Underlying snapshots | ✅ `get_option_snapshots` | ✅ `alpaca_option_contract` | ✅ |
| `GET /v1beta1/options/meta/conditions/{ticktype}` | Condition codes | ✅ `get_condition_codes` | ✅ JSON blob | ✅ |
| `GET /v1beta1/options/meta/exchanges` | Exchange codes | ✅ `get_exchange_codes` | ✅ JSON blob | ✅ |

---

## Crypto Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `GET /v1beta3/crypto/{loc}/bars` | Crypto bars | ✅ `get_crypto_bars` | ✅ `bars` | ✅ |
| `GET /v1beta3/crypto/{loc}/latest/bars` | Latest crypto bars | ✅ `get_crypto_latest_bars` | ✅ `bars` | ✅ |
| `GET /v1beta3/crypto/{loc}/trades` | Crypto trades | ✅ `get_crypto_trades` | ✅ `trades` | ✅ |
| `GET /v1beta3/crypto/{loc}/latest/trades` | Latest crypto trades | ✅ `get_crypto_latest_trades` | ✅ `trades` | ✅ |
| `GET /v1beta3/crypto/{loc}/quotes` | Crypto quotes | ✅ `get_crypto_quotes` | ✅ `quotes` | ✅ |
| `GET /v1beta3/crypto/{loc}/latest/quotes` | Latest crypto quotes | ✅ `get_crypto_quotes` | ✅ `quotes` | ✅ |
| `GET /v1beta3/crypto/{loc}/snapshots` | Crypto snapshots | ✅ `get_crypto_snapshot` | ✅ JSON blob | ✅ |
| `GET /v1beta3/crypto/{loc}/latest/orderbooks` | Crypto orderbooks | ✅ `get_crypto_orderbook` | ✅ JSON blob | ✅ |

---

## Crypto Perpetuals Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `GET /v1beta1/crypto-perps/{loc}/latest/bars` | Perp bars | ❌ | ❌ | ❌ |
| `GET /v1beta1/crypto-perps/{loc}/latest/pricing` | Perp pricing | ❌ | ❌ | ❌ |
| `GET /v1beta1/crypto-perps/{loc}/latest/orderbooks` | Perp orderbooks | ❌ | ❌ | ❌ |
| `GET /v1beta1/crypto-perps/{loc}/latest/quotes` | Perp quotes | ❌ | ❌ | ❌ |
| `GET /v1beta1/crypto-perps/{loc}/latest/trades` | Perp trades | ❌ | ❌ | ❌ |

---

## Forex Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `GET /v1beta1/forex/latest/rates` | Latest FX rates | ✅ `get_forex_rates` | ✅ JSON blob | ✅ |
| `GET /v1beta1/forex/rates` | Historical FX rates | ✅ `get_forex_rates_historical` | ✅ JSON blob | ✅ |

---

## Screener Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `GET /v1beta1/screener/stocks/most-actives` | Most actives | ✅ `get_most_actives` | ✅ JSON blob | ✅ |
| `GET /v1beta1/screener/{market_type}/movers` | Market movers | ✅ `get_movers` | ✅ JSON blob | ✅ |

---

## News Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `GET /v1beta1/news` | News articles | ✅ `get_news` | ✅ JSON blob | ✅ |

---

## Logos Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `GET /v1beta1/logos/{symbol}` | Company logo | ✅ `get_logo` | N/A (binary) | ✅ |

---

## Fixed Income Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `GET /v1beta1/fixed_income/latest/prices` | Treasury prices | ✅ `get_fixed_income_prices` | ✅ JSON blob | ✅ |

---

## Corporate Actions Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `GET /v1/corporate-actions` | Corporate actions | ✅ `get_corporate_actions` | ✅ `corporate_actions` | ✅ |

---

## Summary

### Complete (42 of 47 endpoints = 89%)

| Category | Complete | Total | Coverage |
|----------|----------|-------|----------|
| Stocks | 18 | 18 | **100%** |
| Options | 8 | 8 | **100%** |
| Crypto | 8 | 8 | **100%** |
| Forex | 2 | 2 | **100%** |
| Screener | 2 | 2 | **100%** |
| News | 1 | 1 | **100%** |
| Logos | 1 | 1 | **100%** |
| Fixed Income | 1 | 1 | **100%** |
| Corporate Actions | 1 | 1 | **100%** |
| **Crypto Perpetuals** | 0 | 5 | 0% |

### Not Implemented (5 endpoints - Crypto Perpetuals)

All crypto perpetuals endpoints - specialized use case excluded from standard equities/options trading. These can be added if needed.

---

## Heber Schema Mapping

The Alpaca Market Data API reuses existing Heber schemas:

| Alpaca Data Type | Heber Schema |
|------------------|--------------|
| Stock Bars | `bars` |
| Stock Quotes | `quotes` |
| Stock Trades | `trades` |
| Option Bars | `bars` |
| Option Quotes | `quotes` |
| Option Trades | `trades` |
| Option Snapshots | `alpaca_option_contract` |
| Crypto Bars | `bars` |
| Crypto Quotes | `quotes` |
| Crypto Trades | `trades` |
| Aggregated/Meta | JSON blob |
