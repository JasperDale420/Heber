from __future__ import annotations

import ast
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from heber.writer.consumer import EventConsumer
from heber.writer.ingest_contracts import CONTRACTED_RAW_FEEDS, DLQ_REASON_UNCONTRACTED

NOW = datetime(2026, 2, 11, 15, 30, tzinfo=UTC)

ROUTE_PATTERN = re.compile(r'@router\.(?:get|post|put|delete)\(\s*"(?P<path>/[^"]*)"')


def _data_gateway_root() -> Path:
    return Path(__file__).resolve().parents[2] / "Data-Gateway"


def _load_feed_mapping(middleware_path: Path) -> dict[str, str]:
    source = middleware_path.read_text(encoding="utf-8")
    module = ast.parse(source)

    for node in module.body:
        if not isinstance(node, ast.Assign):
            continue
        if len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name) or target.id != "FEED_MAPPING":
            continue
        if not isinstance(node.value, ast.Dict):
            continue

        mapping: dict[str, str] = {}
        for key_node, value_node in zip(node.value.keys, node.value.values, strict=False):
            if isinstance(key_node, ast.Constant) and isinstance(key_node.value, str):
                if isinstance(value_node, ast.Constant) and isinstance(value_node.value, str):
                    mapping[key_node.value] = value_node.value
        return mapping

    raise AssertionError("Could not extract FEED_MAPPING from middleware.py")


def _extract_provider_route_first_segments(api_root: Path) -> set[str]:
    provider_paths: list[Path] = [
        api_root / "alpaca",
        api_root / "finnhub",
        api_root / "alphavantage",
        api_root / "uw",
        api_root / "yf.py",
        api_root / "sec.py",
        api_root / "news.py",
    ]

    segments: set[str] = set()
    for path in provider_paths:
        files = [path] if path.is_file() else list(path.rglob("*.py"))
        for file_path in files:
            source = file_path.read_text(encoding="utf-8")
            for match in ROUTE_PATTERN.finditer(source):
                route_path = match.group("path")
                parts = [part for part in route_path.split("/") if part]
                if parts:
                    segments.add(parts[0])

    return segments


def _event_payload(feed: str) -> dict[str, object]:
    return {
        "event_id": f"evt-{feed}",
        "provider": "unusual_whales",
        "feed": feed,
        "source": "rest",
        "instrument_type": "equity",
        "instrument_key": "equity:AAPL",
        "symbol": "AAPL",
        "ts_event": NOW.isoformat(),
        "ts_ingest": NOW.isoformat(),
        "payload": {"sample": "value"},
    }


def test_non_contracted_route_derived_feeds_are_blocked_from_silver() -> None:
    dg_root = _data_gateway_root()
    if not dg_root.exists():
        pytest.skip(f"Data-Gateway repo not found at {dg_root}")

    api_root = dg_root / "gateway" / "api"
    middleware_path = api_root / "middleware" / "envelope.py"
    feed_mapping = _load_feed_mapping(middleware_path)
    route_segments = _extract_provider_route_first_segments(api_root)
    derived_feeds = {feed_mapping.get(segment, segment) for segment in route_segments}

    # After the 2026-06 contract sweep every current route-derived feed is
    # contracted, so this set is usually empty — the synthetic feed below
    # keeps the blocking mechanism itself under test either way, and any
    # future uncontracted route still gets exercised here.
    non_contracted = sorted(feed for feed in derived_feeds if feed not in CONTRACTED_RAW_FEEDS)
    synthetic_feed = "synthetic_route_feed_not_contracted"
    assert synthetic_feed not in CONTRACTED_RAW_FEEDS

    consumer = EventConsumer()
    for feed in [*non_contracted, synthetic_feed]:
        consumer.bronze_writer.write = MagicMock()
        consumer.silver_writer.write = MagicMock()

        success, error, retryable = consumer._process_event_once({"data": json.dumps(_event_payload(feed))})

        assert success is False, feed
        assert error == DLQ_REASON_UNCONTRACTED, feed
        assert retryable is False, feed
        consumer.bronze_writer.write.assert_called_once()
        consumer.silver_writer.write.assert_not_called()
