import time

from heber.config import settings
from heber.ops.logging import configure_logging
from heber.writer.compactor import Compactor


def compact_trades():
    configure_logging(service_name="heber-compactor-test", log_level="INFO", json_output=False)
    compactor = Compactor()
    trades_path = settings.silver_path / "feed=trades"

    if not trades_path.exists():
        print(f"Path does not exist: {trades_path}")
        return

    start_time = time.time()
    for partition_path in trades_path.rglob("dt=*"):
        if partition_path.is_dir():
            merged = compactor.compact_partition(partition_path)
            if merged > 0:
                print(f"Merged {merged} files in {partition_path}")

    elapsed = time.time() - start_time
    print(f"Finished check in {elapsed:.2f} seconds")


if __name__ == "__main__":
    compact_trades()
