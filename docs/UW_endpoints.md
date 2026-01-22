# UnusualWhales API Endpoints Tracking

This document tracks all UW API endpoints and their integration status with Data-Gateway and Heber.

## Legend

- ✅ **Complete** - Schema matches UW API exactly
- 🔄 **In Progress** - Partially implemented
- ❌ **Not Started** - No schema exists
- ⚪ **Not Needed** - Not relevant for storage

---

## Alerts Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/alerts` | Alerts | ❌ | ❌ | ❌ |
| `/api/alerts/configuration` | Alert configurations | ❌ | ❌ | ⚪ |

---

## Congress Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/congress/trader/{politicianId}` | Recent Reports By Trader | ❌ | ❌ | ❌ |
| `/api/congress/late-reports` | Recent Late Reports | ✅ | ❌ | 🔄 |
| `/api/congress/recent-trades` | Recent Congress Trades | ✅ | ✅ `congress_trades` | ✅ |

---

## Darkpool Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/darkpool/recent` | Recent Darkpool Trades | ✅ | ✅ `darkpool` | ✅ |
| `/api/darkpool/{ticker}` | Ticker Darkpool Trades | ✅ | ✅ `darkpool` | ✅ |

---

## Earnings Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/earnings/afterhours` | Afterhours | ❌ | ❌ | ❌ |
| `/api/earnings/premarket` | Premarket | ❌ | ❌ | ❌ |
| `/api/earnings/{ticker}` | Historical Ticker Earnings | ❌ | ❌ | ❌ |

---

## ETF Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/etf/{ticker}/exposure` | Exposure | ❌ | ❌ | ❌ |
| `/api/etf/{ticker}/holdings` | Holdings | ❌ | ❌ | ❌ |
| `/api/etf/{ticker}/in-outflow` | Inflow & Outflow | ❌ | ❌ | ❌ |
| `/api/etf/{ticker}/info` | Information | ❌ | ❌ | ❌ |
| `/api/etf/{ticker}/weights` | Sector & Country weights | ❌ | ❌ | ❌ |

---

## Group Flow Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/group-flow/{group}/greek-flow` | Greek flow | ❌ | ❌ | ❌ |
| `/api/group-flow/{group}/greek-flow-expiry` | Greek flow by expiry | ❌ | ❌ | ❌ |

---

## Insider Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/insider/transactions` | Transactions | ✅ | ✅ `insider_trades` | ✅ |
| `/api/insider/sector/{sector}/flow` | Sector Flow | ❌ | ❌ | ❌ |
| `/api/insider/insiders` | Insiders | ❌ | ❌ | ❌ |
| `/api/insider/ticker/{ticker}/flow` | Ticker Flow | ❌ | ❌ | ❌ |

---

## Institution Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/institution/{name}/activity` | Institutional Activity | ❌ | ❌ | ❌ |
| `/api/institution/{name}/holdings` | Institutional Holdings | ✅ | ❌ | 🔄 |
| `/api/institution/{name}/sectors` | Sector Exposure | ❌ | ❌ | ❌ |
| `/api/institution/ownership/{ticker}` | Institutional Ownership | ❌ | ❌ | ❌ |
| `/api/institution/list` | List of Institutions | ❌ | ❌ | ❌ |
| `/api/institution/latest-filings` | Latest Filings | ❌ | ❌ | ❌ |

---

## Market Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/market/correlations` | Correlations | ❌ | ❌ | ❌ |
| `/api/market/events` | Economic calendar | ❌ | ❌ | ❌ |
| `/api/market/fda` | FDA Calendar | ✅ | ❌ | 🔄 |
| `/api/market/insider-buy-sells` | Total Insider Buy & Sells | ❌ | ❌ | ❌ |
| `/api/market/tide` | Market Tide | ✅ | ✅ `market_tide` | ✅ |
| `/api/market/oi-change` | OI Change | ❌ | ❌ | ❌ |
| `/api/market/sector-etfs` | Sector Etfs | ❌ | ❌ | ❌ |
| `/api/market/spike` | SPIKE | ❌ | ❌ | ❌ |
| `/api/market/top-net-impact` | Top Net Impact | ❌ | ❌ | ❌ |
| `/api/market/total-options-volume` | Total Options Volume | ❌ | ❌ | ❌ |
| `/api/market/sector-tide` | Sector Tide | ✅ | ✅ `market_tide` | ✅ |
| `/api/market/etf-tide` | ETF Tide | ✅ | ✅ `market_tide` | ✅ |

---

## Net Flow Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/net-flow/expiry` | Net Flow Expiry | ❌ | ❌ | ❌ |

---

## News Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/news/headlines` | News Headlines | ✅ | ❌ | 🔄 |

---

## Option Contract Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/options/{contract}/flow` | Flow Data | ❌ | ❌ | ❌ |
| `/api/options/{contract}/history` | Historic Data | ❌ | ❌ | ❌ |
| `/api/options/{contract}/intraday` | Intraday Data | ❌ | ❌ | ❌ |
| `/api/options/{contract}/volume-profile` | Volume Profile | ❌ | ❌ | ❌ |

---

## Option Trade Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/option-trades/flow-alerts` | Flow Alerts | ✅ | ✅ `flow_alerts` | ✅ |
| `/api/option-trades/full-tape` | Full Tape | ❌ | ❌ | ❌ |

---

## Politician Portfolios Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/politician-portfolios/holds/{ticker}` | Portfolio Holders by Ticker | ❌ | ❌ | ❌ |
| `/api/politician-portfolios/people` | Politicians List | ❌ | ❌ | ❌ |
| `/api/politician-portfolios/{politicianId}/trades` | Politician Trades | ❌ | ❌ | ❌ |

---

## Season Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/season/earnings` | Earnings Calendar | ❌ | ❌ | ❌ |

---

## Spike Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/spike/{ticker}` | Ticker SPIKE | ❌ | ❌ | ❌ |

---

## Stock Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/stock/{ticker}/analyst-ratings` | Analyst Ratings | ✅ | ❌ | 🔄 |
| `/api/stock/{ticker}/company` | Company Information | ❌ | ❌ | ❌ |
| `/api/stock/{ticker}/contract-chain` | Contract Chain | ❌ | ❌ | ❌ |
| `/api/stock/{ticker}/earnings-history` | Earnings History | ❌ | ❌ | ❌ |
| `/api/stock/{ticker}/expirations` | Option Expirations | ❌ | ❌ | ❌ |
| `/api/stock/{ticker}/flow` | Stock Flow | ❌ | ❌ | ❌ |
| `/api/stock/{ticker}/flow-expiry` | Flow per Expiry | ❌ | ❌ | ❌ |
| `/api/stock/{ticker}/flow-strike` | Flow per Strike | ❌ | ❌ | ❌ |
| `/api/stock/{ticker}/greek-exposure` | Greek Exposure | ✅ | ✅ `greek_exposure` | ✅ |
| `/api/stock/{ticker}/greek-exposure-expiry` | Greek Exposure by Expiry | ✅ | ✅ `greek_exposure` | ✅ |
| `/api/stock/{ticker}/greek-exposure-strike` | Greek Exposure by Strike | ✅ | ✅ `greek_exposure` | ✅ |
| `/api/stock/{ticker}/historical-volatility` | Historical Volatility | ❌ | ❌ | ❌ |
| `/api/stock/{ticker}/hottest-chains` | Hottest Chains | ✅ | ❌ | 🔄 |
| `/api/stock/{ticker}/institution-ownership` | Institution Ownership | ❌ | ❌ | ❌ |
| `/api/stock/{ticker}/iv-rank` | IV Rank | ❌ | ❌ | ❌ |
| `/api/stock/{ticker}/max-pain` | Max Pain | ❌ | ❌ | ❌ |
| `/api/stock/{ticker}/news` | Ticker News | ❌ | ❌ | ❌ |
| `/api/stock/{ticker}/oi-change` | OI Change | ❌ | ❌ | ❌ |
| `/api/stock/{ticker}/option-volume` | Option Volume | ❌ | ❌ | ❌ |
| `/api/stock/{ticker}/overview` | Stock Overview | ❌ | ❌ | ❌ |
| `/api/stock/{ticker}/quote` | Stock Quote | ❌ | ❌ | ❌ |
| `/api/stock/{ticker}/short-data` | Short Data | ❌ | ❌ | ❌ |
| `/api/stock/{ticker}/volume-oi-expiry` | Volume & OI by Expiry | ❌ | ❌ | ❌ |

---

## Screener Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/stock-screener` | Stock Screener | ❌ | ❌ | ❌ |
| `/api/screener/option-contracts` | Option Contract Screener | ❌ | ❌ | ❌ |

---

## Summary

### Complete (10)

- ✅ Congress Trades (`/api/congress/recent-trades`)
- ✅ Darkpool (`/api/darkpool/*`)
- ✅ Flow Alerts (`/api/option-trades/flow-alerts`)
- ✅ Insider Trades (`/api/insider/transactions`)
- ✅ Market Tide (`/api/market/tide`)
- ✅ Sector Tide (`/api/market/sector-tide`) - uses `market_tide` table
- ✅ ETF Tide (`/api/market/etf-tide`) - uses `market_tide` table
- ✅ Greek Exposure (`/api/stock/{ticker}/greek-exposure`) - uses `greek_exposure` table
- ✅ Greek Exposure by Expiry - uses `greek_exposure` table
- ✅ Greek Exposure by Strike - uses `greek_exposure` table

### In Progress (8)

- 🔄 Congress Late Reports
- 🔄 FDA Calendar
- 🔄 Greek Exposure (3 endpoints)
- 🔄 Hottest Chains
- 🔄 News Headlines
- 🔄 Analyst Ratings
- 🔄 Institution Holdings

### Not Started (~80+)

See individual sections above

---

## Priority Queue

### High Priority (Market Data Core)

1. Greek Exposure (GEX)
2. Hottest Chains
3. OI Change
4. Earnings Calendar

### Medium Priority (Research Data)

1. Analyst Ratings
2. News Headlines
3. FDA Calendar
4. Institution Holdings

### Lower Priority

1. ETF data
2. Screeners
3. Volume profiles
