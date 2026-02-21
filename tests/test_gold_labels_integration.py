import shutil
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd

UTC = UTC


# Import the module to test
from heber.gold.labels import read_label, write_label


class TestGoldLabelsRefactor(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for the Gold layer
        self.test_dir = tempfile.mkdtemp()
        self.gold_root = Path(self.test_dir)

        # Sample data
        self.dataset_name = "test_labels"
        self.asof_time = datetime(2025, 1, 3, 16, 0, 0, tzinfo=UTC)

        # Create a sample dataframe
        # ts_event: when the feature/label reference time is
        # ts_available: when it becomes known (simulated)
        self.df = pd.DataFrame(
            {
                "instrument_key": ["AAPL", "GOOG", "MSFT"],
                "ts_label": [
                    datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC),
                    datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC),
                    datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC),
                ],
                "label": [1.0, 0.5, -0.5],
            }
        )

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_write_and_read_label(self):
        """Verify write_label and read_label integration."""

        # Write labels
        # forward_window="1d" -> available = ts_label + 1d
        write_label(
            gold_root=self.gold_root,
            dataset=self.dataset_name,
            df=self.df,
            forward_window="1d",
            label_horizon="close_to_close",
            availability_lag="0s",
        )

        # Determine expected availability
        # 2025-01-01 10:00 + 1d = 2025-01-02 10:00 (ignoring market close logic for simplicity,
        # but write_label uses compute_availability_time which might align to close)

        # Let's inspect what was written to know exact ts_available
        dataset_path = self.gold_root / f"dataset={self.dataset_name}" / "type=label"
        versions = list(dataset_path.glob("version=*"))
        self.assertTrue(len(versions) > 0)
        parquet_file = versions[0] / "data.parquet"
        written_df = pd.read_parquet(parquet_file)
        self.assertIn("ts_available", written_df.columns)

        expected_available = written_df["ts_available"].iloc[0]

        # 1. Read BEFORE availability
        # asof_time = expected_available - 1 minute
        before_time = expected_available - timedelta(minutes=1)
        df_before = read_label(gold_root=self.gold_root, dataset=self.dataset_name, asof_time=before_time)
        self.assertTrue(df_before.empty, "Should not return labels before they are available")

        # 2. Read AFTER availability
        # asof_time = expected_available + 1 minute
        after_time = expected_available + timedelta(minutes=1)
        df_after = read_label(gold_root=self.gold_root, dataset=self.dataset_name, asof_time=after_time)
        self.assertFalse(df_after.empty, "Should return labels after they are available")
        self.assertEqual(len(df_after), 3)
        self.assertEqual(df_after.iloc[0]["instrument_key"], "AAPL")

    def test_read_label_instrument_filtering(self):
        """Verify instrument filtering works."""
        write_label(gold_root=self.gold_root, dataset=self.dataset_name, df=self.df, forward_window="1d")

        # Read with filter
        # ensure allow enough time
        asof_time = datetime(2025, 1, 5, 0, 0, 0, tzinfo=UTC)

        df = read_label(
            gold_root=self.gold_root, dataset=self.dataset_name, asof_time=asof_time, instrument_keys=["AAPL"]
        )

        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["instrument_key"], "AAPL")


if __name__ == "__main__":
    unittest.main()
