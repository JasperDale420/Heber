import os

import pyarrow.dataset as ds

from heber.config import settings


def find_problematic_trades_files():
    feed_path = settings.silver_path / "feed=trades"

    # Get all parquet files
    parquet_files = []
    for root, _, files in os.walk(feed_path):
        for file in files:
            if file.endswith(".parquet"):
                parquet_files.append(os.path.join(root, file))

    print(f"Total parquet files to scan: {len(parquet_files)}")

    # Batch process them to find bad ones
    batch_size = 100
    for i in range(0, len(parquet_files), batch_size):
        batch = parquet_files[i : i + batch_size]
        print(f"Scanning batch {i} to {i + len(batch)}...")
        try:
            ds.dataset(batch, format="parquet")
        except Exception as e:
            print(f"❌ Error in batch {i} to {i + len(batch)}: {e}")
            # If batch fails, scan individually
            for file in batch:
                try:
                    ds.dataset(file, format="parquet")
                except Exception as file_e:
                    print(f"  ❌ Bad file: {file}\n     Error: {file_e}")


if __name__ == "__main__":
    find_problematic_trades_files()
