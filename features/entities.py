"""Entity definitions for Feast feature store."""

from feast import Entity

# Primary entity: what we compute features for
equity = Entity(
    name="instrument_key",
    description="Canonical instrument identifier (e.g., equity:AAPL, option:AAPL250117C00150000)",
    join_keys=["instrument_key"],
)

# Entity for flow alert labels
alert = Entity(
    name="alert",
    description="Individual flow alert from Unusual Whales",
    join_keys=["alert_id"],
)

# Entity for market-wide (non-ticker) features
market = Entity(
    name="market",
    description="Market-level aggregate features (no per-ticker key)",
    join_keys=["market_id"],
)
