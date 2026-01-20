"""Entity definitions for Feast feature store."""

from feast import Entity

# Primary entity: what we compute features for
equity = Entity(
    name="instrument_key",
    description="Canonical instrument identifier (e.g., equity:AAPL, option:AAPL250117C00150000)",
    join_keys=["instrument_key"],
)
