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
| `/api/alerts` | Alerts | ✅ | ✅ `user_alerts` | ✅ |
| `/api/alerts/configuration` | Alert configurations | ✅ | ⚪ | ⚪ |

---

## Congress Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/congress/congress-trader` | Recent Reports By Trader | ✅ `get_congress_trader_reports` | ✅ `congress_trades` | ✅ |
| `/api/congress/late-reports` | Recent Late Reports | ✅ `get_congress_late_reports` | ✅ `congress_trades` | ✅ |
| `/api/congress/recent-trades` | Recent Congress Trades | ✅ `get_congress_trades` | ✅ `congress_trades` | ✅ |

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
| `/api/earnings/afterhours` | Afterhours | ✅ `get_raw_earnings_afterhours` | ✅ `earnings` | ✅ |
| `/api/earnings/premarket` | Premarket | ✅ `get_raw_earnings_premarket` | ✅ `earnings` | ✅ |
| `/api/earnings/{ticker}` | Historical Ticker Earnings | ✅ `get_raw_earnings_ticker` | ✅ `earnings` | ✅ |

---

## ETF Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/etf/{ticker}/exposure` | Exposure | ✅ `get_etf_ticker_exposure` | ✅ `etf_exposure` | ✅ |
| `/api/etf/{ticker}/holdings` | Holdings | ✅ `get_etf_holdings` | ✅ `etf_holdings` | ✅ |
| `/api/etf/{ticker}/in-outflow` | Inflow & Outflow | ✅ `get_etf_inflow_outflow` | ✅ `etf_inflow_outflow` | ✅ |
| `/api/etf/{ticker}/info` | Information | ✅ `get_etf_info` | ✅ `etf_info` | ✅ |
| `/api/etf/{ticker}/weights` | Sector & Country weights | ✅ `get_etf_country_weights` | ✅ `etf_weights` | ✅ |

---

## Group Flow Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/group-flow/{group}/greek-flow` | Greek flow | ✅ `get_group_greek_flow` | ✅ `group_flow` | ✅ |
| `/api/group-flow/{group}/greek-flow-expiry` | Greek flow by expiry | ✅ `get_group_greek_flow_by_expiry` | ✅ `group_flow` | ✅ |

---

## Insider Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/insider/transactions` | Transactions | ✅ `get_insider_transactions` | ✅ `insider_trades` | ✅ |
| `/api/insider/sector/{sector}/flow` | Sector Flow | ✅ `get_insider_sector_flow` | ✅ `insider_flow` | ✅ |
| `/api/insider/insiders` | Insiders | ✅ `get_ticker_insiders` | ✅ `ticker_insiders` | ✅ |
| `/api/insider/ticker/{ticker}/flow` | Ticker Flow | ✅ `get_insider_ticker_flow` | ✅ `insider_flow` | ✅ |

---

## Institution Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/institution/{name}/activity` | Institutional Activity | ✅ `get_institution_activity` | ✅ `institution_activity` | ✅ |
| `/api/institution/{name}/holdings` | Institutional Holdings | ✅ `get_institution_holdings` | ✅ `institution_holdings` | ✅ |
| `/api/institution/{name}/sectors` | Sector Exposure | ✅ `get_institution_sector_exposure` | ✅ `institution_sector_exposure` | ✅ |
| `/api/institution/ownership/{ticker}` | Institutional Ownership | ✅ `get_institutional_ownership` | ✅ `institutional_ownership` | ✅ |
| `/api/institution/list` | List of Institutions | ✅ `get_institutions` | ✅ Re-uses `institutional_ownership` | ✅ |
| `/api/institution/latest-filings` | Latest Filings | ✅ `get_latest_institutional_filings` | ✅ `institution_activity` | ✅ |

---

## Market Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/market/correlations` | Correlations | ✅ `get_market_correlations` | ✅ JSON blob | ✅ |
| `/api/market/events` | Economic calendar | ✅ `get_economic_calendar` | ✅ JSON blob | ✅ |
| `/api/market/fda` | FDA Calendar | ✅ `get_fda_calendar` | ✅ JSON blob | ✅ |
| `/api/market/insider-buy-sells` | Total Insider Buy & Sells | ✅ `get_market_insider_trades` | ✅ `insider_trades` | ✅ |
| `/api/market/tide` | Market Tide | ✅ `get_market_tide` | ✅ `market_tide` | ✅ |
| `/api/market/oi-change` | OI Change | ✅ `get_oi_change` | ✅ JSON blob | ✅ |
| `/api/market/sector-etfs` | Sector Etfs | ✅ `get_sector_etfs` | ✅ `etf_info` | ✅ |
| `/api/market/spike` | SPIKE | ✅ `get_market_spike` | ✅ JSON blob | ✅ |
| `/api/market/top-net-impact` | Top Net Impact | ✅ `get_top_net_impact` | ✅ JSON blob | ✅ |
| `/api/market/total-options-volume` | Total Options Volume | ✅ `get_market_options_volume` | ✅ JSON blob | ✅ |
| `/api/market/sector-tide` | Sector Tide | ✅ `get_sector_tide` | ✅ `market_tide` | ✅ |
| `/api/market/etf-tide` | ETF Tide | ✅ `get_market_tide_by_etf` | ✅ `market_tide` | ✅ |

---

## Net Flow Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/net-flow/expiry` | Net Flow Expiry | ✅ `get_net_flow_expiry` | ✅ JSON blob | ✅ |

---

## News Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/news/headlines` | News Headlines | ✅ `get_news_headlines` | ✅ JSON blob | ✅ |

---

## Option Contract Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/options/{contract}/flow` | Flow Data | ✅ `get_option_contract_flow` | ✅ `flow_alerts` | ✅ |
| `/api/options/{contract}/history` | Historic Data | ✅ `get_option_contract_historic` | ✅ JSON blob | ✅ |
| `/api/options/{contract}/intraday` | Intraday Data | ✅ `get_option_contract_intraday` | ✅ JSON blob | ✅ |
| `/api/options/{contract}/volume-profile` | Volume Profile | ✅ `get_option_contract_volume_profile` | ✅ JSON blob | ✅ |

---

## Option Trade Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/option-trades/flow-alerts` | Flow Alerts | ✅ `get_flow_alerts` | ✅ `flow_alerts` | ✅ |
| `/api/option-trades/full-tape` | Full Tape | ✅ `get_full_tape` | ✅ `flow_alerts` | ✅ |

---

## Politician Portfolios Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/politician-portfolios/holds/{ticker}` | Portfolio Holders by Ticker | ✅ `get_politician_holders` | ✅ `congress` | ✅ |
| `/api/politician-portfolios/people` | Politicians List | ✅ `get_politician_people` | ✅ JSON blob | ✅ |
| `/api/politician-portfolios/{politicianId}/trades` | Politician Trades | ✅ `get_politician_portfolios` | ✅ `congress` | ✅ |

---

## Season Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/season/earnings` | Earnings Calendar | ✅ `get_earnings_premarket`/`get_earnings_afterhours` | ✅ `earnings` | ✅ |

---

## Spike Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/spike/{ticker}` | Ticker SPIKE | ✅ `get_market_spike` (via ticker param) | ✅ JSON blob | ✅ |

---

## Stock Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/stock/{ticker}/analyst-ratings` | Analyst Ratings | ✅ `get_analyst_ratings` | ✅ JSON blob | ✅ |
| `/api/stock/{ticker}/company` | Company Information | ✅ `get_stock_info` | ✅ JSON blob | ✅ |
| `/api/stock/{ticker}/contract-chain` | Contract Chain | ✅ `get_stock_option_chains` | ✅ JSON blob | ✅ |
| `/api/stock/{ticker}/earnings-history` | Earnings History | ✅ `get_earnings_ticker` | ✅ `earnings` | ✅ |
| `/api/stock/{ticker}/expirations` | Option Expirations | ✅ `get_stock_option_contracts` | ✅ JSON blob | ✅ |
| `/api/stock/{ticker}/flow` | Stock Flow | ✅ `get_ticker_flow` | ✅ `flow_alerts` | ✅ |
| `/api/stock/{ticker}/flow-expiry` | Flow per Expiry | ✅ `get_ticker_flow_by_expiry` | ✅ `flow_alerts` | ✅ |
| `/api/stock/{ticker}/flow-strike` | Flow per Strike | ✅ `get_ticker_flow_by_strike` | ✅ `flow_alerts` | ✅ |
| `/api/stock/{ticker}/greek-exposure` | Greek Exposure | ✅ `get_stock_greek_exposure` | ✅ `greek_exposure` | ✅ |
| `/api/stock/{ticker}/greek-exposure-expiry` | Greek Exposure by Expiry | ✅ `get_stock_greek_exposure_by_expiry` | ✅ `greek_exposure` | ✅ |
| `/api/stock/{ticker}/greek-exposure-strike` | Greek Exposure by Strike | ✅ `get_stock_greek_exposure_by_strike` | ✅ `greek_exposure` | ✅ |
| `/api/stock/{ticker}/historical-volatility` | Historical Volatility | ✅ `get_historical_volatility` | ✅ JSON blob | ✅ |
| `/api/stock/{ticker}/hottest-chains` | Hottest Chains | ✅ `get_hottest_chains` | ✅ JSON blob | ✅ |
| `/api/stock/{ticker}/institution-ownership` | Institution Ownership | ✅ `get_institutional_ownership` | ✅ `institutional_ownership` | ✅ |
| `/api/stock/{ticker}/iv-rank` | IV Rank | ✅ `get_iv_rank` | ✅ JSON blob | ✅ |
| `/api/stock/{ticker}/max-pain` | Max Pain | ✅ `get_max_pain` | ✅ JSON blob | ✅ |
| `/api/stock/{ticker}/news` | Ticker News | ✅ `get_ticker_news` | ✅ JSON blob | ✅ |
| `/api/stock/{ticker}/oi-change` | OI Change | ✅ `get_oi_change` | ✅ JSON blob | ✅ |
| `/api/stock/{ticker}/option-volume` | Option Volume | ✅ `get_options_volume` | ✅ JSON blob | ✅ |
| `/api/stock/{ticker}/overview` | Stock Overview | ✅ `get_stock_state` | ✅ JSON blob | ✅ |
| `/api/stock/{ticker}/quote` | Stock Quote | ✅ `get_stock_info` | ✅ JSON blob | ✅ |
| `/api/stock/{ticker}/short-data` | Short Data | ✅ `get_short_data` | ✅ JSON blob | ✅ |
| `/api/stock/{ticker}/volume-oi-expiry` | Volume & OI by Expiry | ✅ `get_volume_oi_by_expiry` | ✅ JSON blob | ✅ |

---

## Screener Controller

| Endpoint | Summary | Gateway | Heber Schema | Status |
|----------|---------|---------|--------------|--------|
| `/api/stock-screener` | Stock Screener | ✅ `get_stock_screener` | ✅ JSON blob | ✅ |
| `/api/screener/option-contracts` | Option Contract Screener | ✅ `get_options_screener` | ✅ JSON blob | ✅ |

---

## Summary

### Complete (11)

- ✅ **User Alerts** (`/api/alerts`) - uses `user_alerts` table
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
