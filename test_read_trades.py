import pyarrow.dataset as ds

from heber.config import settings


def test_read_all_feeds():
    silver_path = settings.silver_path

    feeds_to_test = ["bars", "bars_1m", "darkpool", "flow_alerts", "greek_exposure", "market_tide", "quotes", "trades"]

    for feed in feeds_to_test:
        feed_path = silver_path / f"feed={feed}"
        if not feed_path.exists():
            print(f"[{feed}] Skipping, path does not exist: {feed_path}")
            continue

        print(f"[{feed}] Attempting to load dataset from {feed_path}...")
        try:
            dataset = ds.dataset(feed_path, format="parquet")
            print(f"[{feed}] ✅ Success! Found {len(dataset.files)} files")
            schema = dataset.schema
            print(f"[{feed}] Successfully loaded master schema with {len(schema.names)} fields.")

        except Exception as e:
            print(f"[{feed}] ❌ Failed to load dataset! {e.__class__.__name__}: {e}")


if __name__ == "__main__":
    test_read_all_feeds()
