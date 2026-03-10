from __future__ import annotations

import ast
from pathlib import Path

import pytest

from heber.schemas.silver import SILVER_SCHEMAS
from heber.writer.ingest_contracts import CONTRACTED_RAW_FEEDS, resolve_feed_alias


def _data_gateway_root() -> Path:
    # Monorepo layout: Empire/Heber + sibling Empire/Data-Gateway
    return Path(__file__).resolve().parents[2] / "Data-Gateway"


def _extract_stream_feeds(stream_path: Path) -> set[str]:
    source = stream_path.read_text(encoding="utf-8")
    module = ast.parse(source)

    for node in module.body:
        if not isinstance(node, ast.Assign):
            continue
        if len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name):
            continue
        if target.id != "MESSAGE_TYPE_TO_DATA_TYPE":
            continue
        if not isinstance(node.value, ast.Dict):
            continue

        feeds: set[str] = set()
        for value in node.value.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                feeds.add(value.value)
        return feeds

    raise AssertionError("Could not extract MESSAGE_TYPE_TO_DATA_TYPE from stream.py")


def _extract_wrap_event_feeds(source_path: Path) -> set[str]:
    source = source_path.read_text(encoding="utf-8")
    module = ast.parse(source)

    feeds: set[str] = set()
    for node in ast.walk(module):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Name) or node.func.id != "wrap_event":
            continue

        for keyword in node.keywords:
            if keyword.arg != "feed":
                continue
            value = keyword.value
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                feeds.add(value.value)

    return feeds


def _extract_backfill_feeds(backfill_path: Path) -> set[str]:
    source = backfill_path.read_text(encoding="utf-8")
    module = ast.parse(source)

    dispatch_value: ast.Dict | None = None
    for node in module.body:
        if isinstance(node, ast.AnnAssign):
            target = node.target
            if isinstance(target, ast.Name) and target.id == "BACKFILL_DISPATCH":
                if isinstance(node.value, ast.Dict):
                    dispatch_value = node.value
                    break
        if isinstance(node, ast.Assign):
            if len(node.targets) != 1:
                continue
            target = node.targets[0]
            if isinstance(target, ast.Name) and target.id == "BACKFILL_DISPATCH":
                if isinstance(node.value, ast.Dict):
                    dispatch_value = node.value
                    break

    if dispatch_value is None:
        raise AssertionError("Could not extract BACKFILL_DISPATCH from backfill.py")

    feeds: set[str] = set()
    for key in dispatch_value.keys:
        if not isinstance(key, ast.Tuple) or len(key.elts) != 2:
            continue
        provider, feed = key.elts
        if isinstance(provider, ast.Constant) and isinstance(provider.value, str):
            if provider.value not in {"alpaca", "unusual_whales"}:
                continue
        if isinstance(feed, ast.Constant) and isinstance(feed.value, str):
            feeds.add(feed.value)
    return feeds


def test_all_emitted_data_gateway_feeds_map_to_silver_contract() -> None:
    dg_root = _data_gateway_root()
    if not dg_root.exists():
        pytest.skip(f"Data-Gateway repo not found at {dg_root}")

    stream_file = dg_root / "gateway" / "core" / "stream.py"
    uw_poller_file = dg_root / "gateway" / "core" / "uw_poller.py"
    option_capture_file = dg_root / "gateway" / "core" / "option_capture.py"
    backfill_file = dg_root / "gateway" / "core" / "backfill.py"

    emitted_feeds = (
        _extract_stream_feeds(stream_file)
        | _extract_wrap_event_feeds(uw_poller_file)
        | _extract_wrap_event_feeds(option_capture_file)
        | _extract_backfill_feeds(backfill_file)
    )

    missing_from_contract = sorted(feed for feed in emitted_feeds if feed not in CONTRACTED_RAW_FEEDS)
    assert not missing_from_contract, f"Missing Data Gateway feeds in CONTRACTED_RAW_FEEDS: {missing_from_contract}"

    unmapped = []
    for feed in sorted(emitted_feeds):
        canonical = resolve_feed_alias(feed)
        if canonical not in SILVER_SCHEMAS:
            unmapped.append((feed, canonical))

    assert not unmapped, f"Data Gateway feeds without Silver mapping: {unmapped}"
