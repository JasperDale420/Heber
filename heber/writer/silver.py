"""Silver layer writer - normalized Parquet datasets.

Silver is the canonical, normalized event layer optimized for querying.
Format: Parquet
Path: silver/feed={}/instrument_type={}/dt={}/[hour={}]/
"""

from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from heber.config import settings
from heber.models.envelope import EventEnvelope

logger = structlog.get_logger(__name__)

# Dataset-specific schemas (per PRD Section 8.7)
SILVER_SCHEMAS = {
    "bars": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            # Bars-specific
            ("timeframe", pa.string()),
            ("bar_start_ts", pa.timestamp("us", tz="UTC")),
            ("open", pa.float64()),
            ("high", pa.float64()),
            ("low", pa.float64()),
            ("close", pa.float64()),
            ("volume", pa.float64()),
            ("trade_count", pa.int64()),
            ("vwap", pa.float64()),
        ]
    ),
    "quotes": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            # Quotes-specific
            ("bid_px", pa.float64()),
            ("bid_sz", pa.float64()),
            ("ask_px", pa.float64()),
            ("ask_sz", pa.float64()),
            ("bid_exchange", pa.string()),
            ("ask_exchange", pa.string()),
        ]
    ),
    "trades": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            # Trades-specific
            ("trade_id", pa.string()),
            ("price", pa.float64()),
            ("size", pa.float64()),
            ("exchange", pa.string()),
            ("tape", pa.string()),
        ]
    ),
    "flow_alerts": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            # Flow-specific
            ("underlying", pa.string()),
            ("occ_symbol", pa.string()),
            ("expiry", pa.date32()),
            ("strike", pa.float64()),
            ("put_call", pa.string()),
            ("premium", pa.float64()),
            ("volume", pa.float64()),
            ("open_interest", pa.float64()),
            ("spot_px", pa.float64()),
            ("contract_px", pa.float64()),
            ("alert_type", pa.string()),
            ("side", pa.string()),
            ("aggressor", pa.string()),
            # UW additional fields (P1)
            ("is_sweep", pa.bool_()),
            ("is_unusual", pa.bool_()),
            ("sentiment", pa.string()),
            ("trade_count", pa.int64()),
            ("volume_oi_ratio", pa.float64()),
            ("total_ask_side_prem", pa.float64()),
            ("total_bid_side_prem", pa.float64()),
            ("has_floor", pa.bool_()),
            ("has_multileg", pa.bool_()),
            ("has_singleleg", pa.bool_()),
            ("all_opening_trades", pa.bool_()),
        ]
    ),
    "darkpool": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            # Darkpool-specific
            ("underlying", pa.string()),
            ("price", pa.float64()),
            ("size", pa.float64()),
            ("notional", pa.float64()),
            ("venue", pa.string()),
            ("print_id", pa.string()),
            ("nbbo_bid", pa.float64()),
            ("nbbo_ask", pa.float64()),
            ("ext_hours", pa.string()),
            ("trade_settlement", pa.string()),
            ("canceled", pa.bool_()),
        ]
    ),
    "sector_tide": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            # Sector tide-specific
            ("sector", pa.string()),
            ("net_call_premium", pa.float64()),
            ("net_put_premium", pa.float64()),
            ("net_volume", pa.float64()),
            ("sentiment", pa.string()),
            ("call_put_ratio", pa.float64()),
        ]
    ),
    "market_tide": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            # Market tide-specific
            ("total_call_premium", pa.float64()),
            ("total_put_premium", pa.float64()),
            ("net_volume", pa.float64()),
            ("sentiment", pa.string()),
            ("call_put_ratio", pa.float64()),
        ]
    ),
    # Phase 1: Core Analytics
    "greek_exposure": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            ("gamma_exposure", pa.float64()),
            ("delta_exposure", pa.float64()),
            ("vanna_exposure", pa.float64()),
            ("charm_exposure", pa.float64()),
            ("strike", pa.float64()),
            ("expiry", pa.date32()),
        ]
    ),
    "max_pain": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            ("expiry", pa.string()),
            ("max_pain_strike", pa.float64()),
            ("call_oi", pa.int64()),
            ("put_oi", pa.int64()),
        ]
    ),
    "net_premium_tick": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            ("net_call_premium", pa.float64()),
            ("net_put_premium", pa.float64()),
            ("call_volume", pa.int64()),
            ("put_volume", pa.int64()),
        ]
    ),
    "hottest_chain": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            ("contract_symbol", pa.string()),
            ("underlying", pa.string()),
            ("strike", pa.float64()),
            ("expiry", pa.string()),
            ("option_type", pa.string()),
            ("volume", pa.int64()),
            ("open_interest", pa.int64()),
            ("premium", pa.float64()),
            ("iv", pa.float64()),
        ]
    ),
    # Phase 2: Reference Data
    "earnings": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            ("earnings_date", pa.string()),
            ("time", pa.string()),
            ("eps_estimate", pa.float64()),
            ("eps_actual", pa.float64()),
            ("revenue_estimate", pa.float64()),
            ("revenue_actual", pa.float64()),
        ]
    ),
    "corporate_action": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            ("action_type", pa.string()),
            ("ex_date", pa.string()),
            ("record_date", pa.string()),
            ("payable_date", pa.string()),
            ("amount", pa.float64()),
            ("ratio", pa.string()),
        ]
    ),
    # Phase 3: Screeners
    "most_active": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            ("volume", pa.int64()),
            ("trade_count", pa.int64()),
        ]
    ),
    "mover": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            ("price", pa.float64()),
            ("change", pa.float64()),
            ("percent_change", pa.float64()),
            ("direction", pa.string()),
        ]
    ),
    "screener_result": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            ("price", pa.float64()),
            ("volume", pa.int64()),
            ("market_cap", pa.float64()),
            ("sector", pa.string()),
            ("call_volume", pa.int64()),
            ("put_volume", pa.int64()),
            ("iv_rank", pa.float64()),
        ]
    ),
    # Phase 4: Advanced Analytics
    "iv_rank": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            ("iv_rank", pa.float64()),
            ("iv_percentile", pa.float64()),
            ("current_iv", pa.float64()),
            ("one_year_high", pa.float64()),
            ("one_year_low", pa.float64()),
        ]
    ),
    "iv_term_structure": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            ("expiry", pa.string()),
            ("iv", pa.float64()),
            ("days_to_expiry", pa.int64()),
            ("call_iv", pa.float64()),
            ("put_iv", pa.float64()),
        ]
    ),
    "volatility_stats": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            ("realized_vol_30d", pa.float64()),
            ("realized_vol_60d", pa.float64()),
            ("realized_vol_90d", pa.float64()),
            ("iv_30d", pa.float64()),
            ("iv_percentile", pa.float64()),
            ("hv_iv_ratio", pa.float64()),
        ]
    ),
    "oi_change": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            ("oi_date", pa.string()),
            ("call_oi", pa.int64()),
            ("put_oi", pa.int64()),
            ("call_oi_change", pa.int64()),
            ("put_oi_change", pa.int64()),
        ]
    ),
    "etf_holding": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            ("etf_symbol", pa.string()),
            ("holding_symbol", pa.string()),
            ("weight", pa.float64()),
            ("shares", pa.int64()),
            ("market_value", pa.float64()),
        ]
    ),
    "etf_flow": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            ("flow_date", pa.string()),
            ("inflow", pa.float64()),
            ("outflow", pa.float64()),
            ("net_flow", pa.float64()),
        ]
    ),
    "short_data": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            ("short_date", pa.string()),
            ("short_interest", pa.int64()),
            ("days_to_cover", pa.float64()),
            ("short_percent_float", pa.float64()),
            ("short_percent_outstanding", pa.float64()),
        ]
    ),
    "ftd": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            ("ftd_date", pa.string()),
            ("quantity", pa.int64()),
            ("price", pa.float64()),
            ("value", pa.float64()),
        ]
    ),
    "seasonality": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            ("month", pa.int64()),
            ("avg_return", pa.float64()),
            ("median_return", pa.float64()),
            ("win_rate", pa.float64()),
            ("sample_years", pa.int64()),
        ]
    ),
    # Reference Data
    "option_contract": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            ("underlying", pa.string()),
            ("occ_symbol", pa.string()),
            ("expiry", pa.date32()),
            ("strike", pa.float64()),
            ("put_call", pa.string()),
            ("multiplier", pa.int64()),
            ("style", pa.string()),
            ("exchange", pa.string()),
            ("valid_from", pa.timestamp("us", tz="UTC")),
            ("valid_to", pa.timestamp("us", tz="UTC")),
            ("revision_id", pa.string()),
        ]
    ),
    "news": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            ("news_id", pa.string()),
            ("ts_published", pa.timestamp("us", tz="UTC")),
            ("headline", pa.string()),
            ("summary", pa.string()),
            ("body", pa.string()),
            ("url", pa.string()),
            ("source_name", pa.string()),
            ("valid_from", pa.timestamp("us", tz="UTC")),
            ("valid_to", pa.timestamp("us", tz="UTC")),
            ("revision_id", pa.string()),
        ]
    ),
    "orderbook": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            ("bids_json", pa.string()),  # JSON array of [price, size]
            ("asks_json", pa.string()),  # JSON array of [price, size]
            ("depth", pa.int64()),
        ]
    ),
}


# Default schema for unknown feeds
DEFAULT_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("provider", pa.string()),
        ("feed", pa.string()),
        ("instrument_type", pa.string()),
        ("instrument_key", pa.string()),
        ("symbol", pa.string()),
        ("ts_event", pa.timestamp("us", tz="UTC")),
        ("ts_ingest", pa.timestamp("us", tz="UTC")),
        ("ts_available", pa.timestamp("us", tz="UTC")),
        ("source", pa.string()),
        ("schema_version", pa.string()),
        ("quality_flags", pa.list_(pa.string())),
        ("payload_json", pa.string()),  # Store payload as JSON string
    ]
)


class SilverWriter:
    """Writes normalized events to Silver layer as Parquet."""

    def __init__(self):
        self.buffers: dict[str, list[dict]] = defaultdict(list)
        self.last_flush: datetime = datetime.utcnow()

    def _get_partition_key(self, envelope: EventEnvelope) -> str:
        """Generate partition key for an event."""
        dt = envelope.ts_event.strftime("%Y-%m-%d")

        # High-volume feeds use hour partitioning
        if envelope.feed in ("quotes", "trades"):
            hour = envelope.ts_event.strftime("%H")
            return f"feed={envelope.feed}/instrument_type={envelope.instrument_type}/dt={dt}/hour={hour}"

        return f"feed={envelope.feed}/instrument_type={envelope.instrument_type}/dt={dt}"

    def _get_schema(self, feed: str) -> pa.Schema:
        """Get schema for a feed."""
        return SILVER_SCHEMAS.get(feed, DEFAULT_SCHEMA)

    def _envelope_to_row(self, envelope: EventEnvelope) -> dict[str, Any]:
        """Convert envelope to Silver row format."""
        # Base columns from envelope
        row = {
            "event_id": envelope.event_id,
            "provider": envelope.provider,
            "feed": envelope.feed,
            "instrument_type": envelope.instrument_type,
            "instrument_key": envelope.instrument_key,
            "symbol": envelope.symbol,
            "ts_event": envelope.ts_event,
            "ts_ingest": envelope.ts_ingest,
            "ts_available": envelope.ts_available,
            "source": envelope.source,
            "schema_version": envelope.schema_version,
            "quality_flags": envelope.quality_flags,
        }

        # Add payload fields
        payload = envelope.payload
        if envelope.feed in SILVER_SCHEMAS:
            # Field name mappings for UW feeds (payload field -> Silver schema field)
            field_mappings = {
                "flow_alerts": {
                    "price": "contract_px",
                    "underlying_price": "spot_px",
                    "option_chain": "occ_symbol",
                    "symbol": "underlying",  # symbol in payload is the underlying
                    "alert_rule": "alert_type",
                },
                "darkpool": {
                    "symbol": "underlying",  # symbol in payload is the underlying
                    "exchange": "venue",
                    "tracking_id": "print_id",
                },
                "market_tide": {
                    "net_call_premium": "total_call_premium",
                    "net_put_premium": "total_put_premium",
                },
                "sector_tide": {
                    # sector_tide uses same field names, but we include for consistency
                },
            }
            mappings = field_mappings.get(envelope.feed, {})

            # Map payload fields to schema columns with type coercion
            schema = SILVER_SCHEMAS[envelope.feed]
            for field in schema:
                if field.name in row:
                    continue  # Already set from envelope

                # Try mapped name first, then direct name
                source_name = next((k for k, v in mappings.items() if v == field.name), field.name)
                value = payload.get(source_name)

                # Type coercion based on Arrow type
                if value is not None:
                    value = self._coerce_value(value, field.type)

                row[field.name] = value
        else:
            # Store payload as JSON for unknown feeds
            import json

            row["payload_json"] = json.dumps(payload, default=str)

        return row

    def _coerce_value(self, value: Any, arrow_type: pa.DataType) -> Any:
        """Coerce a value to match the expected Arrow type."""
        if value is None:
            return None

        try:
            if pa.types.is_floating(arrow_type):
                return float(value) if value != "" else None
            elif pa.types.is_integer(arrow_type):
                return int(float(value)) if value != "" else None
            elif pa.types.is_date(arrow_type):
                from datetime import date, datetime

                if isinstance(value, date):
                    return value
                if isinstance(value, datetime):
                    return value.date()
                if isinstance(value, str):
                    return datetime.strptime(value[:10], "%Y-%m-%d").date()
            elif pa.types.is_timestamp(arrow_type):
                from datetime import datetime

                if isinstance(value, datetime):
                    return value
                if isinstance(value, str):
                    # Handle ISO format with timezone
                    return datetime.fromisoformat(value.replace("Z", "+00:00"))
            # For strings and other types, return as-is
            return value
        except (ValueError, TypeError) as e:
            logger.warning(
                "Type coercion failed",
                value=str(value)[:50],
                target_type=str(arrow_type),
                error=str(e),
            )
            return None

    def _get_file_path(self, partition_key: str) -> Path:
        """Get file path for a partition."""
        base = settings.silver_path / partition_key
        base.mkdir(parents=True, exist_ok=True)

        ts = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
        return base / f"part-{ts}.parquet"

    async def write(self, envelope: EventEnvelope) -> None:
        """Buffer an event for writing."""
        partition_key = self._get_partition_key(envelope)
        row = self._envelope_to_row(envelope)
        self.buffers[partition_key].append(row)

    async def flush_if_needed(self) -> None:
        """Flush buffers if conditions are met."""
        now = datetime.utcnow()
        elapsed = (now - self.last_flush).total_seconds()

        for partition_key, rows in list(self.buffers.items()):
            should_flush = (
                len(rows) >= settings.silver_max_rows_per_file or elapsed >= settings.bronze_flush_interval_seconds
            )
            if should_flush and rows:
                await self._flush_partition(partition_key, rows)
                self.buffers[partition_key] = []

        self.last_flush = now

    async def flush(self) -> None:
        """Flush all buffers immediately."""
        for partition_key, rows in list(self.buffers.items()):
            if rows:
                await self._flush_partition(partition_key, rows)
                self.buffers[partition_key] = []

    async def _flush_partition(self, partition_key: str, rows: list[dict]) -> None:
        """Write rows to a Parquet file."""
        if not rows:
            return

        # Determine feed from partition key
        feed = partition_key.split("/")[0].split("=")[1]
        schema = self._get_schema(feed)
        file_path = self._get_file_path(partition_key)

        try:
            # Create Arrow table
            table = pa.Table.from_pylist(rows, schema=schema)

            # Write Parquet with compression
            pq.write_table(
                table,
                file_path,
                compression="snappy",
                row_group_size=100_000,
            )

            logger.info(
                "Flushed Silver partition",
                partition=partition_key,
                rows=len(rows),
                file=str(file_path),
            )
        except Exception as e:
            logger.error(
                "Failed to flush Silver partition",
                partition=partition_key,
                error=str(e),
                exc_info=True,
            )
            raise
