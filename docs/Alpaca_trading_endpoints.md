# Alpaca Trading API Endpoint Coverage

This document tracks the implementation status of Alpaca Trading API endpoints in the Data Gateway and Heber data pipeline.

**Legend:**

- ✅ = Complete (Gateway method exists, Heber schema available if applicable)
- 🔄 = In Progress (Gateway method exists, needs Heber schema)
- ❌ = Not Started

---

## Account Controller

| Endpoint | Method | Summary | Gateway | Heber Schema | Status |
|----------|--------|---------|---------|--------------|--------|
| `/v2/account` | GET | Get Account | ✅ `get_account` | ✅ `account_snapshot` | ✅ |
| `/v2/account/configurations` | GET | Get Configurations | ✅ `get_account_configurations` | ✅ JSON blob | ✅ |
| `/v2/account/configurations` | PATCH | Update Configurations | ✅ `set_account_configurations` | N/A (write) | ✅ |
| `/v2/account/activities` | GET | Get Activities | ✅ `get_account_activities` | ✅ JSON blob | ✅ |
| `/v2/account/activities/{activity_type}` | GET | Get Activities by Type | ✅ `get_account_activities` | ✅ JSON blob | ✅ |
| `/v2/account/portfolio/history` | GET | Portfolio History | ✅ `get_portfolio_history` | ✅ `portfolio_history` | ✅ |

---

## Orders Controller

| Endpoint | Method | Summary | Gateway | Heber Schema | Status |
|----------|--------|---------|---------|--------------|--------|
| `/v2/orders` | POST | Create Order | ✅ `create_order` | N/A (write) | ✅ |
| `/v2/orders` | GET | Get All Orders | ✅ `get_orders` | ✅ `orders_history` | ✅ |
| `/v2/orders` | DELETE | Cancel All Orders | ✅ `cancel_all_orders` | N/A (write) | ✅ |
| `/v2/orders:by_client_order_id` | GET | Get Order by Client ID | ✅ `get_order_by_client_id` | ✅ `orders_history` | ✅ |
| `/v2/orders/{order_id}` | GET | Get Order | ✅ `get_order` | ✅ `orders_history` | ✅ |
| `/v2/orders/{order_id}` | PATCH | Replace Order | ✅ `replace_order` | N/A (write) | ✅ |
| `/v2/orders/{order_id}` | DELETE | Cancel Order | ✅ `cancel_order` | N/A (write) | ✅ |

---

## Positions Controller

| Endpoint | Method | Summary | Gateway | Heber Schema | Status |
|----------|--------|---------|---------|--------------|--------|
| `/v2/positions` | GET | Get All Positions | ✅ `get_positions` | ✅ `alpaca_position` | ✅ |
| `/v2/positions` | DELETE | Close All Positions | ✅ `close_all_positions` | N/A (write) | ✅ |
| `/v2/positions/{symbol_or_asset_id}` | GET | Get Position | ✅ `get_position` | ✅ `alpaca_position` | ✅ |
| `/v2/positions/{symbol_or_asset_id}` | DELETE | Close Position | ✅ `close_position` | N/A (write) | ✅ |
| `/v2/positions/{symbol_or_contract_id}/exercise` | POST | Exercise Option | ✅ `exercise_option_position` | N/A (write) | ✅ |
| `/v2/positions/{symbol_or_contract_id}/do-not-exercise` | POST | Do Not Exercise | ✅ `do_not_exercise_option` | N/A (write) | ✅ |

---

## Watchlists Controller

| Endpoint | Method | Summary | Gateway | Heber Schema | Status |
|----------|--------|---------|---------|--------------|--------|
| `/v2/watchlists` | GET | Get All Watchlists | ✅ `get_watchlists` | ✅ JSON blob | ✅ |
| `/v2/watchlists` | POST | Create Watchlist | ✅ `create_watchlist` | N/A (write) | ✅ |
| `/v2/watchlists/{watchlist_id}` | GET | Get Watchlist | ✅ `get_watchlist` | ✅ JSON blob | ✅ |
| `/v2/watchlists/{watchlist_id}` | PUT | Update Watchlist | ✅ `update_watchlist` | N/A (write) | ✅ |
| `/v2/watchlists/{watchlist_id}` | POST | Add Symbol | ✅ `add_asset_to_watchlist` | N/A (write) | ✅ |
| `/v2/watchlists/{watchlist_id}` | DELETE | Delete Watchlist | ✅ `delete_watchlist` | N/A (write) | ✅ |
| `/v2/watchlists:by_name` | GET | Get by Name | ❌ | ✅ JSON blob | ❌ |
| `/v2/watchlists:by_name` | PUT | Update by Name | ❌ | N/A (write) | ❌ |
| `/v2/watchlists:by_name` | POST | Add Symbol by Name | ❌ | N/A (write) | ❌ |
| `/v2/watchlists:by_name` | DELETE | Delete by Name | ❌ | N/A (write) | ❌ |
| `/v2/watchlists/{watchlist_id}/{symbol}` | DELETE | Remove Symbol | ✅ `remove_asset_from_watchlist` | N/A (write) | ✅ |

---

## Assets Controller

| Endpoint | Method | Summary | Gateway | Heber Schema | Status |
|----------|--------|---------|---------|--------------|--------|
| `/v2/assets` | GET | Get All Assets | ✅ `get_assets` | ✅ `assets` | ✅ |
| `/v2/assets/{symbol_or_asset_id}` | GET | Get Asset | ✅ `get_asset` | ✅ `assets` | ✅ |
| `/v2/assets/fixed_income/us_treasuries` | GET | US Treasuries | ✅ `get_fixed_income_prices` | ✅ JSON blob | ✅ |

---

## Market Data (Calendar/Clock) Controller

| Endpoint | Method | Summary | Gateway | Heber Schema | Status |
|----------|--------|---------|---------|--------------|--------|
| `/v2/calendar` | GET | Trading Calendar | ✅ `get_calendar` | ✅ `market_calendar` | ✅ |
| `/v2/clock` | GET | Market Clock | ✅ `get_clock` | ✅ `market_clock` | ✅ |

---

## Options Controller

| Endpoint | Method | Summary | Gateway | Heber Schema | Status |
|----------|--------|---------|---------|--------------|--------|
| `/v2/options/contracts` | GET | Get Option Contracts | ✅ `get_option_chain` | ✅ `option_contracts` | ✅ |
| `/v2/options/contracts/{symbol_or_id}` | GET | Get Option Contract | ✅ `get_option_chain` | ✅ `option_contracts` | ✅ |

---

## Corporate Actions Controller

| Endpoint | Method | Summary | Gateway | Heber Schema | Status |
|----------|--------|---------|---------|--------------|--------|
| `/v2/corporate_actions/announcements/{id}` | GET | Get Announcement | ✅ `get_corporate_actions` | ✅ `corporate_actions` | ✅ |
| `/v2/corporate_actions/announcements` | GET | Get Announcements | ✅ `get_corporate_actions` | ✅ `corporate_actions` | ✅ |

---

## Wallets Controller (Crypto)

| Endpoint | Method | Summary | Gateway | Heber Schema | Status |
|----------|--------|---------|---------|--------------|--------|
| `/v2/wallets` | GET | Get Wallets | ❌ | ❌ | ❌ |
| `/v2/wallets/transfers` | GET | Get Transfers | ❌ | ❌ | ❌ |
| `/v2/wallets/transfers` | POST | Create Transfer | ❌ | N/A (write) | ❌ |
| `/v2/wallets/transfers/{transfer_id}` | GET | Get Transfer | ❌ | ❌ | ❌ |
| `/v2/wallets/whitelists` | GET | Get Whitelists | ❌ | ❌ | ❌ |
| `/v2/wallets/whitelists` | POST | Create Whitelist | ❌ | N/A (write) | ❌ |
| `/v2/wallets/whitelists/{whitelisted_address_id}` | DELETE | Delete Whitelist | ❌ | N/A (write) | ❌ |
| `/v2/wallets/fees/estimate` | GET | Estimate Fees | ❌ | ❌ | ❌ |

---

## Perpetuals Controller

| Endpoint | Method | Summary | Gateway | Heber Schema | Status |
|----------|--------|---------|---------|--------------|--------|
| `/v2/perpetuals/wallets` | GET | Get Perpetual Wallets | ❌ | ❌ | ❌ |
| `/v2/perpetuals/wallets/transfers` | GET | Get Transfers | ❌ | ❌ | ❌ |
| `/v2/perpetuals/wallets/transfers` | POST | Create Transfer | ❌ | N/A (write) | ❌ |
| `/v2/perpetuals/wallets/transfers/{transfer_id}` | GET | Get Transfer | ❌ | ❌ | ❌ |
| `/v2/perpetuals/wallets/whitelists` | GET | Get Whitelists | ❌ | ❌ | ❌ |
| `/v2/perpetuals/wallets/whitelists` | POST | Create Whitelist | ❌ | N/A (write) | ❌ |
| `/v2/perpetuals/wallets/whitelists/{whitelisted_address_id}` | DELETE | Delete Whitelist | ❌ | N/A (write) | ❌ |
| `/v2/perpetuals/wallets/fees/estimate` | GET | Estimate Fees | ❌ | ❌ | ❌ |
| `/v2/perpetuals/leverage` | GET | Get Leverage | ❌ | ❌ | ❌ |
| `/v2/perpetuals/leverage` | POST | Set Leverage | ❌ | N/A (write) | ❌ |
| `/v2/perpetuals/account_vitals` | GET | Account Vitals | ❌ | ❌ | ❌ |

---

## Summary

### Complete (39 of 57 endpoints)

| Category | Complete | Total | Coverage |
|----------|----------|-------|----------|
| Account | 6 | 6 | 100% |
| Orders | 7 | 7 | 100% |
| Positions | 4 | 6 | 67% |
| Watchlists | 7 | 11 | 64% |
| Assets | 3 | 3 | 100% |
| Market Data | 2 | 2 | 100% |
| Options | 2 | 2 | 100% |
| Corporate Actions | 2 | 2 | 100% |
| Wallets | 0 | 8 | 0% |
| Perpetuals | 0 | 11 | 0% |

### Not Implemented (18 endpoints)

**Positions:**

- Exercise option position
- Do-not-exercise option position

**Watchlists (by name variants):**

- Get/Update/Add/Delete by name (4 endpoints)

**Crypto Wallets:**

- All wallet endpoints (8 endpoints)

**Perpetuals:**

- All perpetual endpoints (11 endpoints)

---

## Priority Queue

### High Priority (Core Trading)

All core trading endpoints are **complete** ✅

### Medium Priority

1. Option exercise endpoints (2)
2. Watchlist by-name variants (4)

### Low Priority (Not in Current Scope)

1. Crypto Wallets (8) - specialized use case
2. Perpetuals (11) - specialized use case
