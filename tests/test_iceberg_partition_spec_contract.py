"""Regression test for Iceberg partition spec wiring."""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ICEBERG_CATALOG = ROOT / "heber" / "storage" / "iceberg_catalog.py"


def test_create_silver_table_uses_partition_spec_object() -> None:
    source = ICEBERG_CATALOG.read_text(encoding="utf-8")
    tree = ast.parse(source)

    assert "PartitionSpec" in source
    assert "DayTransform()" in source
    assert "partition_spec=[" not in source

    create_table_calls = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr == "create_table":
                create_table_calls.append(node)

    assert create_table_calls, "Expected create_table() call in iceberg_catalog.py"
    call = create_table_calls[0]

    partition_kw = next((kw for kw in call.keywords if kw.arg == "partition_spec"), None)
    assert partition_kw is not None, "partition_spec keyword must be provided to create_table()"
    assert not isinstance(partition_kw.value, ast.List), "partition_spec should be a PartitionSpec object, not a list"
