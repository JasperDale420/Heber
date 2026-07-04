"""Schema-enforcing, atomic parquet writer injected into Heber's BackfillWriter."""

import os
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from heber.schemas.silver import get_silver_schema


def make_bars_parquet_writer() -> Callable[[list[dict[str, Any]], Path], None]:
    """Return parquet_writer(records, path): write canonical `bars` schema atomically (temp + os.replace)."""
    schema = get_silver_schema("bars")

    def _write(records: list[dict[str, Any]], path: Path) -> None:
        table = pa.Table.from_pylist(records, schema=schema)
        tmp = path.with_name(f"_{uuid.uuid4().hex}.parquet.tmp")
        pq.write_table(table, str(tmp))
        os.replace(tmp, path)

    return _write
