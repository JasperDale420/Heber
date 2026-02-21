from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime
from pathlib import Path

from heber.writer.transformer import BronzeToSilverTransformer

NOW = datetime(2026, 2, 12, 15, 30, tzinfo=UTC)


def _write_bronze_event(path: Path, event: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps(event) + "\n")


def test_transformer_skips_news_feed_by_bronze_only_policy(tmp_path: Path) -> None:
    bronze_root = tmp_path / "bronze"
    silver_root = tmp_path / "silver"

    event = {
        "event_id": "evt-news-1",
        "provider": "alpaca",
        "feed": "news",
        "source": "rest",
        "instrument_type": "equity",
        "instrument_key": "equity:AAPL",
        "symbol": "AAPL",
        "ts_event": NOW.isoformat(),
        "ts_ingest": NOW.isoformat(),
        "payload": {
            "article_id": "news-1",
            "headline": "AAPL posts strong guidance",
            "published_at": NOW.isoformat(),
            "symbols": ["AAPL"],
            "source": "Reuters",
        },
    }

    _write_bronze_event(
        bronze_root / "provider=alpaca" / "feed=news" / "dt=2026-02-12" / "hour=15" / "events.jsonl.gz",
        event,
    )

    transformer = BronzeToSilverTransformer(bronze_path=bronze_root, silver_path=silver_root)
    count = transformer.transform(feed="news", provider="alpaca")

    assert count == 0
    assert not any(silver_root.rglob("*.parquet"))
