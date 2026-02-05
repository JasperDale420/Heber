"""Regression tests for Silver schema single source of truth."""

from __future__ import annotations

from pathlib import Path

from heber.schemas.silver import DEFAULT_SCHEMA, SILVER_SCHEMAS
from heber.writer.silver import SilverWriter


def test_silver_writer_uses_canonical_schema_module() -> None:
    writer = SilverWriter()

    assert writer._get_schema("bars") is SILVER_SCHEMAS["bars"]
    assert writer._get_schema("unknown-feed").equals(DEFAULT_SCHEMA)


def test_writer_silver_does_not_define_inline_schema_constants() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    silver_writer_source = (repo_root / "heber" / "writer" / "silver.py").read_text(encoding="utf-8")

    assert "SILVER_SCHEMAS = {" not in silver_writer_source
    assert "DEFAULT_SCHEMA = pa.schema(" not in silver_writer_source
