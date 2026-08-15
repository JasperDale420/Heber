# Schema Audit: Data Gateway → Heber

**Audit Date**: 2026-02-04
**Purpose**: Verify schema compatibility between Data Gateway ingestion and Heber storage

---

## Executive Summary

This audit compares:

1. **Data Gateway EventEnvelope** (outbound wrapper)
2. **Heber EventEnvelope** (inbound receiver)
3. **UW Normalized Schemas** (payload data)
4. **Heber Silver Layer Schemas** (storage models)

### Quick Status

| Feed | Envelope Compatible | Payload Fields | Storage Ready | Issues |
|------|---------------------|----------------|---------------|--------|
| `flow_alerts` | ✅ | ⚠️ Partial | ⚠️ | Missing field mappings |
| `darkpool` | ✅ | ⚠️ Partial | ⚠️ | Missing field mappings |
| `market_tide` | ✅ | ✅ | ✅ | None |
| `sector_tide` | ✅ | ⚠️ | ❌ | No Silver schema |

---

## 1. EventEnvelope Compatibility

### Data Gateway EventEnvelope

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `event_id` | `str` | ✅ | BLAKE2b-128 hex hash (32 chars). Digest of `provider\|feed\|instrument_key\|ts_event\|*uniques`; `Decimal` uniques are stringified with `str(d)` (`1.50`), never `str(float(d))` (`1.5`). See [PRD §6.3](../PRD.md) |
| `provider` | `str` | ✅ | e.g., `unusual_whales` |
| `feed` | `str` | ✅ | e.g., `flow_alerts`, `darkpool` |
| `source` | `str` | ✅ | `websocket` or `rest` |
| `instrument_type` | `str` | ✅ | `equity`, `option`, `crypto`, `forex` |
| `instrument_key` | `str` | ✅ | e.g., `equity:AAPL`, `option:OCC:...` |
| `symbol` | `str` | ✅ | Human-readable symbol |
| `ts_event` | `datetime` | ✅ | Provider event time |
| `ts_ingest` | `datetime` | ✅ | Gateway receive time |
| `schema_version` | `str` | ✅ | Default: `v1` |
| `lineage` | `dict` | ✅ | Sequence numbers, stream IDs |
| `quality_flags` | `list[str]` | ✅ | `validated`, `deduped`, `cached` |
| `payload` | `dict` | ✅ | Normalized event data |

### Heber EventEnvelope

| Field | Type | Required | Status | Notes |
|-------|------|----------|--------|-------|
| `event_id` | `str` | ✅ | ✅ Match | |
| `provider` | `str` | ✅ | ✅ Match | |
| `feed` | `str` | ✅ | ✅ Match | |
| `source` | `str` | ✅ | ✅ Match | |
| `instrument_type` | `str` | ✅ | ✅ Match | |
| `instrument_key` | `str` | ✅ | ✅ Match | |
| `symbol` | `str` | ✅ | ✅ Match | |
| `ts_event` | `datetime` | ✅ | ✅ Match | |
| `ts_ingest` | `datetime` | ✅ | ✅ Match | |
| `schema_version` | `str` | ✅ | ✅ Match | |
| `lineage` | `dict` | ✅ | ✅ Match | |
| `quality_flags` | `list[str]` | ✅ | ✅ Match | |
| `payload` | `dict` | ✅ | ✅ Match | |
| **`ts_available`** | `datetime` | ❌ | 🆕 Heber-only | Anti-leakage gate |
| **`raw`** | `dict` | ❌ | 🆕 Heber-only | Bronze fidelity |
| **`processing_delay_ms`** | `int` | ❌ | 🆕 Heber-only | ts_effective calc |

> [!TIP]
> **Envelope Status**: ✅ **Compatible**. Heber's envelope is a superset of Data Gateway's envelope. All required fields match.

---

## 2. Flow Alerts Schema Audit

### 2.1 Data Gateway: `NormalizedFlowAlert`

```python
class NormalizedFlowAlert:
    symbol: str                          # Underlying ticker
    timestamp: datetime                  # Alert time
    strike: Decimal                      # Strike price
    expiry: str                          # Expiration date
    put_call: str                        # "P" or "C"
    premium: Decimal                     # Total premium
    volume: int                          # Contract volume
    open_interest: int                   # OI
    side: str                            # "ask", "bid", "mid"
    is_sweep: bool                       # Sweep indicator
    is_unusual: bool                     # Unusual activity flag
    sentiment: str | None                # bullish/bearish
    option_chain: str | None             # OCC symbol
    price: Decimal | None                # Contract price
    underlying_price: Decimal | None     # Spot price
    alert_rule: str | None               # Trigger rule
    total_size: int | None               # Total contracts
    trade_count: int | None              # Number of trades
    volume_oi_ratio: Decimal | None      # Vol/OI ratio
    total_ask_side_prem: Decimal | None  # Ask-side premium
    total_bid_side_prem: Decimal | None  # Bid-side premium
    all_opening_trades: bool             # Opening trades only
    has_floor: bool                      # Floor trades
    has_multileg: bool                   # Multileg indicator
    has_singleleg: bool                  # Singleleg indicator
    expiry_count: int | None             # Expirations count
    provider: str                        # "unusual_whales"
```

### 2.2 Heber: `FlowAlertRecord` (Silver)

```python
class FlowAlertRecord(SilverBase):
    underlying: str                      # ⚠️ Maps from "symbol"
    occ_symbol: str | None               # ⚠️ Maps from "option_chain"
    expiry: date                         # ⚠️ Type mismatch: str → date
    strike: float                        # ✅ Maps from Decimal
    put_call: str                        # ✅ Match
    premium: float                       # ✅ Maps from Decimal
    volume: float                        # ⚠️ Type: int → float
    open_interest: float | None          # ✅ Match
    spot_px: float | None                # ⚠️ Maps from "underlying_price"
    contract_px: float | None            # ⚠️ Maps from "price"
    alert_type: str                      # ⚠️ Maps from "alert_rule"
    side: str | None                     # ✅ Match
    aggressor: str | None                # ❌ Not in Gateway schema
    tags: list[str] | None               # ❌ Not in Gateway schema
```

### 2.3 Flow Alerts: Field Mapping Issues

| Gateway Field | Heber Field | Status | Action Required |
|---------------|-------------|--------|-----------------|
| `symbol` | `underlying` | ⚠️ Name | Already handled in watch consumer |
| `option_chain` | `occ_symbol` | ⚠️ Name | Already handled in watch consumer |
| `expiry` (str) | `expiry` (date) | ⚠️ Type | Parse str to date |
| `underlying_price` | `spot_px` | ⚠️ Name | Add mapping |
| `price` | `contract_px` | ⚠️ Name | Add mapping |
| `alert_rule` | `alert_type` | ⚠️ Name | Add mapping |
| — | `aggressor` | ❌ Missing | Not in UW API |
| — | `tags` | ❌ Missing | Not in UW API |
| `is_sweep` | — | ❌ Missing | Add to Silver or derive |
| `is_unusual` | — | ❌ Missing | Add to Silver or derive |
| `sentiment` | — | ❌ Missing | Add to Silver |
| `trade_count` | — | ❌ Missing | Add to Silver |
| `volume_oi_ratio` | — | ❌ Missing | Add to Silver |
| `total_ask_side_prem` | — | ❌ Missing | Add to Silver |
| `total_bid_side_prem` | — | ❌ Missing | Add to Silver |
| `has_floor` | — | ❌ Missing | Add to Silver |
| `has_multileg` | — | ❌ Missing | Add to Silver |
| `has_singleleg` | — | ❌ Missing | Add to Silver |

---

## 3. Darkpool Schema Audit

### 3.1 Data Gateway: `NormalizedDarkpoolTrade`

```python
class NormalizedDarkpoolTrade:
    symbol: str                          # Ticker
    timestamp: datetime                  # Trade time
    price: Decimal                       # Trade price
    size: int                            # Share count
    notional: Decimal                    # Dollar value
    exchange: str | None                 # Exchange code
    tracking_id: str | None              # Trade tracking ID
    nbbo_bid: Decimal | None             # NBBO bid at time
    nbbo_ask: Decimal | None             # NBBO ask at time
    ext_hours: str | None                # Extended hours flag
    trade_settlement: str | None         # Settlement type
    canceled: bool                       # Canceled flag
    provider: str                        # "unusual_whales"
```

### 3.2 Heber: `DarkpoolTradeRecord` (Silver)

```python
class DarkpoolTradeRecord(SilverBase):
    underlying: str                      # ⚠️ Maps from "symbol"
    price: float                         # ✅ Maps from Decimal
    size: float                          # ⚠️ Type: int → float
    notional: float | None               # ✅ Maps from Decimal
    venue: str | None                    # ⚠️ Maps from "exchange"
    print_id: str | None                 # ⚠️ Maps from "tracking_id"
    conditions: list[str] | None         # ❌ Not in Gateway schema
```

### 3.3 Darkpool: Field Mapping Issues

| Gateway Field | Heber Field | Status | Action Required |
|---------------|-------------|--------|-----------------|
| `symbol` | `underlying` | ⚠️ Name | Add mapping |
| `exchange` | `venue` | ⚠️ Name | Add mapping |
| `tracking_id` | `print_id` | ⚠️ Name | Add mapping |
| — | `conditions` | ❌ Missing | Not in UW API |
| `nbbo_bid` | — | ❌ Missing | Add to Silver |
| `nbbo_ask` | — | ❌ Missing | Add to Silver |
| `ext_hours` | — | ❌ Missing | Add to Silver |
| `trade_settlement` | — | ❌ Missing | Add to Silver |
| `canceled` | — | ❌ Missing | Add to Silver |

---

## 4. Market Tide Schema Audit

### 4.1 Data Gateway: `NormalizedMarketTide`

```python
class NormalizedMarketTide:
    timestamp: datetime                  # Snapshot time
    date: str | None                     # Trading date
    net_call_premium: Decimal            # Net call premium
    net_put_premium: Decimal             # Net put premium
    net_volume: int | None               # Net volume
    sentiment: str                       # Market sentiment
    provider: str                        # "unusual_whales"
```

### 4.2 Heber: `MarketTideRecord` (Silver)

```python
class MarketTideRecord(SilverBase):
    snapshot_id: str | None              # ❌ Not in Gateway
    total_call_premium: float | None     # ⚠️ Maps from "net_call_premium"
    total_put_premium: float | None      # ⚠️ Maps from "net_put_premium"
    call_put_ratio: float | None         # ❌ Not in Gateway (derived)
    bullish_flow: float | None           # ❌ Not in Gateway
    bearish_flow: float | None           # ❌ Not in Gateway
    neutral_flow: float | None           # ❌ Not in Gateway
    net_flow: float | None               # ❌ Not in Gateway
    total_volume: float | None           # ⚠️ Maps from "net_volume"
    unusual_volume_count: int | None     # ❌ Not in Gateway
    sector_data: dict | None             # ❌ Not in Gateway
    index_data: dict | None              # ❌ Not in Gateway
```

### 4.3 Market Tide: Field Mapping Issues

| Gateway Field | Heber Field | Status | Action Required |
|---------------|-------------|--------|-----------------|
| `net_call_premium` | `total_call_premium` | ⚠️ Name | Add mapping |
| `net_put_premium` | `total_put_premium` | ⚠️ Name | Add mapping |
| `net_volume` | `total_volume` | ⚠️ Name | Add mapping |
| `sentiment` | — | ❌ Missing | Add to Silver |
| — | `call_put_ratio` | 🔧 Derived | Compute from premiums |
| — | `snapshot_id` | ❌ Missing | Generate on ingest |

---

## 5. Sector Tide Schema Audit

### 5.1 Data Gateway: sector_tide (dict payload)

Currently published as raw dict from UW API, no normalized schema.

### 5.2 Heber: No Silver Schema

**Status**: ❌ **No Silver schema defined for sector_tide**

### 5.3 Recommended Action

Create `SectorTideRecord` Silver schema:

```python
class SectorTideRecord(SilverBase):
    sector: str                          # GICS sector name
    net_call_premium: float | None
    net_put_premium: float | None
    net_volume: float | None
    sentiment: str | None
    call_put_ratio: float | None         # Derived
```

---

## 6. Watch Consumer Field Mapping

The watch consumer (`heber/watch/consumer.py`) has its own field mapping in `_map_alert_fields`:

### 6.1 Current Watch Consumer Mappings

```python
# _map_alert_fields (lines 275-289)
{
    "id": parsed.get("id") or parsed.get("event_id") or parsed.get("alert_id"),
    "occ_symbol": parsed.get("occ_symbol") or parsed.get("option_chain"),  # ✅ Fixed
    "underlying": parsed.get("underlying") or parsed.get("ticker") or parsed.get("symbol"),  # ✅ Fixed
    "put_call": put_call,                          # ✅
    "expiry": parsed.get("expiry"),                # ✅
    "strike": float(parsed.get("strike", 0)),      # ✅
    "spot_px": float(parsed.get("spot_px") or parsed.get("underlying_price", 0)),  # ✅
    "contract_px": float(parsed.get("contract_px") or parsed.get("price", 0)),     # ✅
}
```

### 6.2 Watch Consumer Missing Mappings

| Gateway Payload Field | Watch Needs | Status |
|-----------------------|-------------|--------|
| `premium` | Premium tracking | ❌ Not mapped |
| `volume` | Volume analysis | ❌ Not mapped |
| `open_interest` | OI tracking | ❌ Not mapped |
| `is_sweep` | Sweep alerts | ❌ Not mapped |
| `alert_rule` | Alert classification | ❌ Not mapped |
| `sentiment` | Sentiment analysis | ❌ Not mapped |

---

## 7. Recommended Actions

### Immediate (P0)

- [ ] **7.1** Add missing field mappings to Silver writer for `flow_alerts`
- [ ] **7.2** Add missing field mappings to Silver writer for `darkpool`
- [ ] **7.3** Create `SectorTideRecord` Silver schema

### Short-term (P1)

- [ ] **7.4** Add missing UW fields to `FlowAlertRecord`:
  - `is_sweep`, `is_unusual`, `sentiment`
  - `trade_count`, `volume_oi_ratio`
  - `total_ask_side_prem`, `total_bid_side_prem`
  - `has_floor`, `has_multileg`, `has_singleleg`

- [ ] **7.5** Add missing UW fields to `DarkpoolTradeRecord`:
  - `nbbo_bid`, `nbbo_ask`
  - `ext_hours`, `trade_settlement`, `canceled`

- [ ] **7.6** Add `sentiment` to `MarketTideRecord`

### Long-term (P2)

- [ ] **7.7** Create normalized schema for sector_tide in Data Gateway
- [ ] **7.8** Add computed fields (call_put_ratio) to tide records
- [ ] **7.9** Document field mappings in PRD

---

## 8. File References

| Component | File |
|-----------|------|
| Data Gateway EventEnvelope | [envelope.py](file:///Users/jacobmcmillan/Empire/Data-Gateway/gateway/core/envelope.py) |
| Data Gateway Normalized Schemas | [schemas/**init**.py](file:///Users/jacobmcmillan/Empire/Data-Gateway/gateway/schemas/__init__.py) |
| Data Gateway UW Poller | [uw_poller.py](file:///Users/jacobmcmillan/Empire/Data-Gateway/gateway/core/uw_poller.py) |
| Heber EventEnvelope | [envelope.py](file:///Users/jacobmcmillan/Empire/Heber/heber/models/envelope.py) |
| Heber Silver Schemas | [silver.py](file:///Users/jacobmcmillan/Empire/Heber/heber/models/silver.py) |
| Watch Consumer | [consumer.py](file:///Users/jacobmcmillan/Empire/Heber/heber/watch/consumer.py) |
