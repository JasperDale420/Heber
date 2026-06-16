from __future__ import annotations

from heber.catalog.seeds import FEED_MAPPING_SEEDS
from heber.schemas.silver import SILVER_SCHEMAS
from heber.writer.ingest_contracts import (
    BRONZE_ONLY_SILVER_DATASETS,
    DATA_GATEWAY_FEEDS,
    FEED_ALIAS_MAP,
    resolve_feed_alias,
)


def test_alias_map_matches_contract() -> None:
    assert FEED_ALIAS_MAP["ftds"] == "ftd"
    assert FEED_ALIAS_MAP["short_interest"] == "short_data"
    assert FEED_ALIAS_MAP["short_volume"] == "short_data"
    assert FEED_ALIAS_MAP["historic_option_volume"] == "historic_option_volume"
    assert FEED_ALIAS_MAP["flow"] == "flow_alerts"
    assert FEED_ALIAS_MAP["ticker_flow"] == "flow_alerts"
    assert FEED_ALIAS_MAP["greeks"] == "greek_exposure"
    assert FEED_ALIAS_MAP["gex"] == "greek_exposure"
    assert FEED_ALIAS_MAP["darkpool_ticker"] == "darkpool"
    assert FEED_ALIAS_MAP["option_trades"] == "trades"
    assert FEED_ALIAS_MAP["crypto_bars"] == "bars"
    assert FEED_ALIAS_MAP["crypto_trades"] == "trades"
    assert FEED_ALIAS_MAP["institutions"] == "institution_holdings"


def test_all_data_gateway_feeds_route_to_mapped_silver_schema() -> None:
    """Every Silver-bound contracted feed resolves to a typed Silver schema.

    Bronze-only feeds (reference / metadata endpoints) are explicitly excluded —
    they're contracted so the writer doesn't DLQ them, but they intentionally
    have no Silver schema until a downstream use-case promotes them.
    """
    for feed in DATA_GATEWAY_FEEDS:
        canonical_feed = resolve_feed_alias(feed)
        if canonical_feed in BRONZE_ONLY_SILVER_DATASETS:
            continue
        assert canonical_feed in SILVER_SCHEMAS, f"{feed} -> {canonical_feed} missing Silver schema"


def test_seed_catalog_includes_data_gateway_alias_mappings() -> None:
    seed_lookup = {(item["provider"], item["gateway_feed"]): item["silver_dataset_name"] for item in FEED_MAPPING_SEEDS}

    assert seed_lookup[("unusual_whales", "ftds")] == "ftd"
    assert seed_lookup[("unusual_whales", "short_interest")] == "short_data"
    assert seed_lookup[("unusual_whales", "short_volume")] == "short_data"
    assert seed_lookup[("unusual_whales", "historic_option_volume")] == "historic_option_volume"
    assert seed_lookup[("unusual_whales", "flow")] == "flow_alerts"
    assert seed_lookup[("unusual_whales", "ticker_flow")] == "flow_alerts"
    assert seed_lookup[("unusual_whales", "greeks")] == "greek_exposure"
    assert seed_lookup[("unusual_whales", "gex")] == "greek_exposure"
    assert seed_lookup[("unusual_whales", "darkpool_ticker")] == "darkpool"
    assert seed_lookup[("alpaca", "option_trades")] == "trades"
    assert seed_lookup[("alpaca", "option_chain_snapshot")] == "option_chain_snapshot"
    assert seed_lookup[("alpaca", "crypto_bars")] == "bars"
    assert seed_lookup[("alpaca", "crypto_trades")] == "trades"
    assert seed_lookup[("unusual_whales", "institutions")] == "institution_holdings"
