import shutil
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa

from heber.models.envelope import EventEnvelope
from heber.writer.silver import SilverWriter
from heber.writer.transformer import SILVER_SCHEMAS, BronzeToSilverTransformer


class TestWriterRefactor(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.silver_path = Path(self.test_dir) / "silver"
        self.silver_path.mkdir(parents=True)

        # Mock settings object
        self.mock_settings = MagicMock()
        self.mock_settings.data_root = Path(self.test_dir)
        self.mock_settings.silver_path = self.silver_path

        # Sample data
        self.sample_envelope = EventEnvelope(
            event_id="evt_123",
            provider="alpaca",
            feed="bars",
            instrument_type="equity",
            instrument_key="SPY",
            symbol="SPY",
            ts_event=datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC),
            ts_ingest=datetime(2025, 1, 1, 12, 0, 1, tzinfo=UTC),
            source="test",
            payload={"t": "2025-01-01T12:00:00Z", "o": 100.0, "h": 101.0, "l": 99.0, "c": 100.5, "v": 1000},
        )

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_silver_writer_flush_partition(self):
        """Verify SilverWriter writes parquet correctly."""
        with patch("heber.writer.silver.settings", self.mock_settings):
            writer = SilverWriter()

            # Mocking schema to allow writing dummy data
            schema = pa.schema(
                [
                    ("instrument_key", pa.string()),
                    ("ts_event", pa.timestamp("ns", tz="UTC")),
                    ("open", pa.float64()),
                    ("close", pa.float64()),
                ]
            )
            writer._get_schema = MagicMock(return_value=schema)

            # Write dummy data
            rows = [
                {
                    "instrument_key": "SPY",
                    "ts_event": pd.Timestamp("2025-01-01 12:00:00+00:00"),
                    "open": 100.0,
                    "close": 100.5,
                }
            ]

            partition_key = "feed=bars/instrument_type=equity/dt=2025-01-01"
            writer._flush_partition(partition_key, rows)

            # Verify file exists
            partition_path = self.silver_path / partition_key
            self.assertTrue(partition_path.exists())
            files = list(partition_path.glob("*.parquet"))
            self.assertEqual(len(files), 1)

            # Verify content
            df = pd.read_parquet(files[0])
            self.assertEqual(len(df), 1)
            self.assertEqual(df.iloc[0]["instrument_key"], "SPY")

    def test_writer_utils_build_silver_candidates(self):
        """Verify shared build_silver_candidates utility."""
        from heber.writer.utils import build_silver_candidates

        # Test default (consumer style)
        candidates = build_silver_candidates(self.sample_envelope)
        self.assertIsInstance(candidates, list)
        self.assertEqual(len(candidates), 1)  # simple payload = 1 candidate

        # Test override (transformer style)
        candidates_override = build_silver_candidates(self.sample_envelope, feed_override="bars")
        self.assertIsInstance(candidates_override, list)

    def test_transformer_flush_buffer(self):
        """Verify BronzeToSilverTransformer._flush_buffer (replaced _write_silver_batch)."""
        # Define a temporary schema for 'test_feed'
        test_schema = pa.schema(
            [
                ("instrument_key", pa.string()),
                ("instrument_type", pa.string()),
                ("ts_event", pa.timestamp("ns", tz="UTC")),
                ("price", pa.float64()),
            ]
        )

        with (
            patch.dict(SILVER_SCHEMAS, {"test_feed": test_schema}),
            patch("heber.writer.transformer.settings", self.mock_settings),
        ):
            transformer = BronzeToSilverTransformer()

            records = [
                {
                    "instrument_key": "AAPL",
                    "instrument_type": "equity",
                    "ts_event": pd.Timestamp("2025-01-01 12:00:00+00:00"),
                    "price": 150.0,
                }
            ]

            # New method signature
            partition_key = "feed=test_feed/instrument_type=equity/dt=2025-01-01"
            transformer._flush_buffer(records, partition_key, "test_feed")

            # Verify path construction
            partition_path = self.silver_path / partition_key
            self.assertTrue(partition_path.exists())
            files = list(partition_path.glob("*.parquet"))
            self.assertEqual(len(files), 1)

            # Verify content
            df = pd.read_parquet(files[0])
            self.assertEqual(len(df), 1)
            self.assertEqual(df.iloc[0]["instrument_key"], "AAPL")


if __name__ == "__main__":
    unittest.main()
