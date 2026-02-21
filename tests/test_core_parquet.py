import shutil
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

UTC = UTC

from heber.core.parquet import read_parquet_dataset


class TestCoreParquet(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.root = Path(self.test_dir)

        # Create sample data
        self.df = pd.DataFrame(
            {
                "ts_event": [
                    datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC),
                    datetime(2025, 1, 1, 11, 0, 0, tzinfo=UTC),
                    datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC),
                ],
                "ts_available": [
                    datetime(2025, 1, 1, 10, 5, 0, tzinfo=UTC),
                    datetime(2025, 1, 1, 11, 5, 0, tzinfo=UTC),
                    datetime(2025, 1, 1, 12, 5, 0, tzinfo=UTC),
                ],
                "value": [1, 2, 3],
                "instrument": ["A", "A", "B"],
            }
        )

        self.parquet_path = self.root / "data.parquet"
        self.df.to_parquet(self.parquet_path)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_read_simple(self):
        """Test basic read."""
        df = read_parquet_dataset(self.parquet_path)
        self.assertEqual(len(df), 3)

    def test_time_range_filtering(self):
        """Test time range filtering."""
        start = datetime(2025, 1, 1, 10, 30, 0, tzinfo=UTC)
        end = datetime(2025, 1, 1, 11, 30, 0, tzinfo=UTC)

        df = read_parquet_dataset(self.parquet_path, time_range=(start, end))
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["value"], 2)

    def test_asof_filtering(self):
        """Test asof time filtering."""
        # Available at 10:05, 11:05, 12:05

        # Query at 11:00 -> should only see 10:00 event (available 10:05? No wait, 11:00 > 10:05)
        asof = datetime(2025, 1, 1, 11, 0, 0, tzinfo=UTC)

        df = read_parquet_dataset(self.parquet_path, asof_time=asof)

        # 10:00 event available at 10:05 <= 11:00 -> YES
        # 11:00 event available at 11:05 <= 11:00 -> NO

        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["value"], 1)

    def test_column_pruning(self):
        """Test column selection."""
        df = read_parquet_dataset(self.parquet_path, columns=["ts_event", "value"])
        self.assertListEqual(sorted(df.columns), ["ts_event", "value"])

    def test_filters_pushdown(self):
        """Test pyarrow filter pushdown."""
        # Filter: instrument == 'B'
        df = read_parquet_dataset(self.parquet_path, filters=[("instrument", "==", "B")])
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["instrument"], "B")


if __name__ == "__main__":
    unittest.main()
