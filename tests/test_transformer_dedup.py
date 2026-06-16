"""Tests for Bronze-to-Silver transformer deduplication.

Verifies that the transformer skips events that already exist in Silver
partitions and deduplicates within a single batch run.
"""

import gzip
import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from heber.writer.transformer import BronzeToSilverTransformer


def _make_envelope(event_id: str, symbol: str = "AAPL", ts_event: str = "2026-03-11T14:30:00Z") -> dict:
    """Create a minimal valid EventEnvelope dict for flow_alerts."""
    return {
        "event_id": event_id,
        "provider": "unusual_whales",
        "feed": "flow_alerts",
        "instrument_type": "option",
        "instrument_key": f"option:OCC:{symbol}260116C00200000",
        "symbol": symbol,
        "source": "rest",
        "ts_event": ts_event,
        "ts_ingest": ts_event,
        "ts_available": ts_event,
        "schema_version": "v1",
        "quality_flags": ["validated"],
        "payload": {
            "underlying": symbol,
            "expiry": "2026-01-16",
            "strike": 200.0,
            "put_call": "C",
            "premium": 50000.0,
            "volume": 100,
            "open_interest": 500,
            "alert_type": "SWEEP",
        },
    }


def _write_bronze_file(bronze_dir: Path, feed: str, dt: str, envelopes: list[dict]) -> None:
    """Write envelopes to a Bronze JSONL.gz file."""
    provider_dir = bronze_dir / "provider=unusual_whales" / f"feed={feed}" / f"dt={dt}"
    provider_dir.mkdir(parents=True, exist_ok=True)
    file_path = provider_dir / "part-001.jsonl.gz"
    with gzip.open(file_path, "wt", encoding="utf-8") as f:
        for env in envelopes:
            f.write(json.dumps(env) + "\n")


def _write_silver_parquet(silver_dir: Path, partition_key: str, event_ids: list[str]) -> None:
    """Write a minimal Silver Parquet file with given event_ids."""
    partition_path = silver_dir / partition_key
    partition_path.mkdir(parents=True, exist_ok=True)

    # Minimal Silver schema with event_id column
    table = pa.table(
        {
            "event_id": pa.array(event_ids, type=pa.string()),
            "provider": pa.array(["unusual_whales"] * len(event_ids), type=pa.string()),
            "feed": pa.array(["flow_alerts"] * len(event_ids), type=pa.string()),
            "instrument_type": pa.array(["option"] * len(event_ids), type=pa.string()),
            "instrument_key": pa.array(["option:OCC:AAPL260116C00200000"] * len(event_ids), type=pa.string()),
            "symbol": pa.array(["AAPL"] * len(event_ids), type=pa.string()),
            "source": pa.array(["rest"] * len(event_ids), type=pa.string()),
            "ts_event": pa.array(
                [datetime(2026, 3, 11, 14, 30, tzinfo=UTC)] * len(event_ids), type=pa.timestamp("us", tz="UTC")
            ),
            "ts_ingest": pa.array(
                [datetime(2026, 3, 11, 14, 30, tzinfo=UTC)] * len(event_ids), type=pa.timestamp("us", tz="UTC")
            ),
            "ts_available": pa.array(
                [datetime(2026, 3, 11, 14, 30, tzinfo=UTC)] * len(event_ids), type=pa.timestamp("us", tz="UTC")
            ),
            "schema_version": pa.array(["v1"] * len(event_ids), type=pa.string()),
            "quality_flags": pa.array([["validated"]] * len(event_ids), type=pa.list_(pa.string())),
        }
    )
    pq.write_table(table, partition_path / "existing.parquet")


class TestCollectExistingEventIds:
    """Tests for _collect_existing_event_ids."""

    def test_empty_partition(self, tmp_path: Path) -> None:
        """Returns empty set when partition does not exist."""
        transformer = BronzeToSilverTransformer(
            bronze_path=tmp_path / "bronze",
            silver_path=tmp_path / "silver",
        )
        result = transformer._collect_existing_event_ids("feed=flow_alerts/instrument_type=option/dt=2026-03-11")
        assert result == set()

    def test_reads_existing_ids(self, tmp_path: Path) -> None:
        """Returns event_ids from existing Parquet files."""
        silver_dir = tmp_path / "silver"
        partition_key = "feed=flow_alerts/instrument_type=option/dt=2026-03-11"
        _write_silver_parquet(silver_dir, partition_key, ["evt-001", "evt-002", "evt-003"])

        transformer = BronzeToSilverTransformer(
            bronze_path=tmp_path / "bronze",
            silver_path=silver_dir,
        )
        result = transformer._collect_existing_event_ids(partition_key)
        assert result == {"evt-001", "evt-002", "evt-003"}

    def test_skips_hidden_files(self, tmp_path: Path) -> None:
        """Ignores dotfile Parquet files (sidecar metadata)."""
        silver_dir = tmp_path / "silver"
        partition_key = "feed=flow_alerts/instrument_type=option/dt=2026-03-11"
        partition_path = silver_dir / partition_key
        partition_path.mkdir(parents=True)

        # Write a hidden sidecar file
        table = pa.table({"event_id": pa.array(["hidden-001"], type=pa.string())})
        pq.write_table(table, partition_path / ".metadata.parquet")

        # Write a real file
        _write_silver_parquet(silver_dir, partition_key, ["real-001"])

        transformer = BronzeToSilverTransformer(
            bronze_path=tmp_path / "bronze",
            silver_path=silver_dir,
        )
        result = transformer._collect_existing_event_ids(partition_key)
        assert "hidden-001" not in result
        assert "real-001" in result


class TestTransformerDedup:
    """Integration tests for transformer deduplication during backfill."""

    def test_skips_existing_events(self, tmp_path: Path) -> None:
        """Transformer skips events that already exist in Silver."""
        bronze_dir = tmp_path / "bronze"
        silver_dir = tmp_path / "silver"

        # Write 3 events to Bronze
        envelopes = [
            _make_envelope("evt-001", "AAPL"),
            _make_envelope("evt-002", "TSLA"),
            _make_envelope("evt-003", "NVDA"),
        ]
        _write_bronze_file(bronze_dir, "flow_alerts", "2026-03-11", envelopes)

        # Pre-populate Silver with evt-001 and evt-002
        partition_key = "feed=flow_alerts/instrument_type=option/dt=2026-03-11"
        _write_silver_parquet(silver_dir, partition_key, ["evt-001", "evt-002"])

        transformer = BronzeToSilverTransformer(
            bronze_path=bronze_dir,
            silver_path=silver_dir,
        )
        records = transformer.transform(
            feed="flow_alerts",
            dt="2026-03-11",
            provider="unusual_whales",
        )

        # Only evt-003 should have been written
        assert records == 1

    def test_deduplicates_within_batch(self, tmp_path: Path) -> None:
        """Transformer deduplicates events within the same Bronze file."""
        bronze_dir = tmp_path / "bronze"
        silver_dir = tmp_path / "silver"

        # Write the same event twice in Bronze
        envelopes = [
            _make_envelope("evt-dup", "AAPL"),
            _make_envelope("evt-dup", "AAPL"),  # duplicate
            _make_envelope("evt-unique", "TSLA"),
        ]
        _write_bronze_file(bronze_dir, "flow_alerts", "2026-03-11", envelopes)

        transformer = BronzeToSilverTransformer(
            bronze_path=bronze_dir,
            silver_path=silver_dir,
        )
        records = transformer.transform(
            feed="flow_alerts",
            dt="2026-03-11",
            provider="unusual_whales",
        )

        # Only 2 unique events should be written
        assert records == 2

    def test_no_existing_silver_writes_all(self, tmp_path: Path) -> None:
        """When Silver is empty, all Bronze events are written."""
        bronze_dir = tmp_path / "bronze"
        silver_dir = tmp_path / "silver"

        envelopes = [
            _make_envelope("evt-a", "AAPL"),
            _make_envelope("evt-b", "TSLA"),
        ]
        _write_bronze_file(bronze_dir, "flow_alerts", "2026-03-11", envelopes)

        transformer = BronzeToSilverTransformer(
            bronze_path=bronze_dir,
            silver_path=silver_dir,
        )
        records = transformer.transform(
            feed="flow_alerts",
            dt="2026-03-11",
            provider="unusual_whales",
        )

        assert records == 2


class TestIdempotentRerun:
    """Verify that re-running the transformer produces zero new writes."""

    def test_rerun_produces_zero_records(self, tmp_path: Path) -> None:
        """A second run of the same transformer writes nothing new."""
        bronze_dir = tmp_path / "bronze"
        silver_dir = tmp_path / "silver"

        envelopes = [
            _make_envelope("evt-x", "SPY"),
            _make_envelope("evt-y", "QQQ"),
        ]
        _write_bronze_file(bronze_dir, "flow_alerts", "2026-03-11", envelopes)

        transformer = BronzeToSilverTransformer(
            bronze_path=bronze_dir,
            silver_path=silver_dir,
        )

        # First run — writes all events
        first_run = transformer.transform(
            feed="flow_alerts",
            dt="2026-03-11",
            provider="unusual_whales",
        )
        assert first_run == 2

        # Second run — should skip everything
        second_run = transformer.transform(
            feed="flow_alerts",
            dt="2026-03-11",
            provider="unusual_whales",
        )
        assert second_run == 0
