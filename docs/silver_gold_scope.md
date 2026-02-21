# Silver and Gold Scope Matrix

## Why this document exists

You asked a simple but important question:

1. What data should we really normalize into Silver?
2. What should Gold consume after that?

This document is the decision map for that.

## Ground rules

- Bronze is the source of truth (raw, complete, append-only).
- Silver is only for typed, high-signal, analytics-ready data.
- Gold is only for model/trading datasets that are directly used.
- If a feed is not actively used yet, keep it in Bronze and do not force it into Silver.

## Current decision rubric

A feed should stay in Silver only if all of these are true:

1. It has a stable schema mapping.
2. It passes required-field checks.
3. It is consumed by a real pipeline or near-term roadmap.

If any of those are false, keep it in Bronze for now.

## Silver Keep/Drop Matrix (Gateway -> Bronze -> Silver)

Status definitions:

- `KEEP_CORE`: critical now; keep strongly typed and validated.
- `KEEP_CONTEXT`: useful context feed; keep typed but lower priority/SLA.
- `BRONZE_ONLY`: keep raw in Bronze; do not prioritize Silver normalization yet.

| Raw Feed(s) | Canonical Silver Dataset | Status | Required Fields Gate | Why |
|---|---|---|---|---|
| `bars`, `crypto_bars` | `bars` | `KEEP_CORE` | `open, high, low, close, volume` | Foundation for labels + momentum/volatility/cross-asset features |
| `quotes` | `quotes` | `KEEP_CORE` | `bid_px, ask_px` | Needed for microstructure features |
| `trades`, `option_trades`, `crypto_trades` | `trades` | `KEEP_CORE` | `price, size` | Needed for microstructure and execution analytics |
| `flow_alerts`, `ticker_flow` | `flow_alerts` | `KEEP_CORE` | `occ_symbol, strike, put_call, premium, volume` | Primary input for watch labeling + flow features |
| `darkpool`, `darkpool_ticker` | `darkpool` | `KEEP_CONTEXT` | `underlying, price, size` | Useful for flow context and confirmation features |
| `market_tide` | `market_tide` | `KEEP_CONTEXT` | `total_call_premium, total_put_premium` | Regime/context signal for options flow |
| `sector_tide` | `sector_tide` | `KEEP_CONTEXT` | `sector, net_call_premium, net_put_premium` | Sector regime context |
| `greek_exposure` | `greek_exposure` | `KEEP_CONTEXT` | `gamma_exposure` | Optional context features |
| `iv_rank` | `iv_rank` | `KEEP_CONTEXT` | `iv_rank` | Optional options regime context |
| `oi_change` | `oi_change` | `KEEP_CONTEXT` | `oi_date, call_oi, put_oi` | Optional options positioning context |
| `historic_option_volume` | `historic_option_volume` | `KEEP_CONTEXT` | `hov_date, expiry, volume` | Optional options context |
| `earnings` | `earnings` | `KEEP_CONTEXT` | `earnings_date` | Event context for labeling/filters |
| `short_interest`, `short_volume` | `short_data` | `KEEP_CONTEXT` | `short_date, short_interest` | Secondary context factor |
| `news` | `news` | `BRONZE_ONLY` | `news_id, headline, ts_published` | Not in active Gold pipelines yet |
| `ftds` | `ftd` | `BRONZE_ONLY` | `ftd_date, quantity` | Not in active Gold pipelines yet |
| `congress_trades` | `congress_trades` | `BRONZE_ONLY` | `politician_name, trade_type, trade_date` | Not in active Gold pipelines yet |
| `insider_trades` | `insider_trades` | `BRONZE_ONLY` | `insider_name, trade_type, trade_date` | Not in active Gold pipelines yet |
| `institutions` | `institution_holdings` | `BRONZE_ONLY` | `institution_name, value, quarter_end` | Not in active Gold pipelines yet |

## What this means operationally

- `KEEP_CORE` feeds should block bad rows from entering Silver.
- `KEEP_CONTEXT` feeds should stay healthy but can tolerate lower throughput priority.
- `BRONZE_ONLY` feeds should still be ingested and retained in Bronze, but Silver quality and compaction work should not be blocked by them.

## Gold Input Plan (after Silver scope lock)

## Gold Dataset Priority 1 (ship/maintain now)

### `labels_alert_barriers`

Source pipeline:

- `heber/features/pipelines/alert_labels.py`

Silver inputs:

- `flow_alerts`: `event_id, underlying, ts_event, put_call, expiry, occ_symbol`
- `bars` (daily and optional intraday): `instrument_key, bar_start_ts, open, high, low, close, volume, timeframe`
- `bars` for market context: SPY and UVXY keys

External dependency (already in code):

- Option bars are fetched from Data Gateway for contract-level labels.

## Gold Dataset Priority 2 (enable next)

### `features_momentum`

- From Silver `bars`.
- Required: `instrument_key, bar_start_ts, close` (plus OHLCV for full feature set).

### `features_volatility`

- From Silver `bars`.
- Required: `instrument_key, bar_start_ts, high, low, close`.

### `features_microstructure`

- From Silver `quotes` (and later `trades`).
- Required: `instrument_key, ts_event, bid_px, ask_px, bid_sz, ask_sz`.

### `features_flow`

- From Silver `flow_alerts` (optionally joined with `darkpool`).
- Required: `underlying, ts_event, premium, put_call, alert_type`.

### `features_cross_asset`

- From Silver `bars` for target symbols plus benchmark (SPY).

## Gold Dataset Priority 3 (later)

- Event/news/fundamental blends (requires stable consumer demand first).

## Implementation order (practical)

1. Keep Silver strict for `KEEP_CORE` first (already in progress with required-field gating).
2. Stabilize replay/backfill for `KEEP_CORE` partitions.
3. Promote Gold Priority 1 checks to daily operational run.
4. Add Gold Priority 2 feature datasets with clear owners and quality checks.
5. Leave `BRONZE_ONLY` feeds raw until a real Gold use-case appears.

## How to verify the plan is working

- Data flow proof command (JSON):
  - `heber health-dataflow --mode manual --window-seconds 900`
- Silver quality smoke checks:
  - Core required fields should be non-null for recent partitions.
- Gold readiness checks:
  - `labels_alert_barriers` should have fresh rows and non-empty label columns.

## Default policy statement

Until a feed has an approved Gold use-case, it stays in Bronze and does not get Silver hardening priority.
