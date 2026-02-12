"""Shared Bronze->Silver row normalizer used by live and backfill pipelines."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import Any

import pyarrow as pa

from heber.models.envelope import EventEnvelope
from heber.schemas.silver import SILVER_SCHEMAS
from heber.writer.ingest_contracts import (
    FIELD_MAPPINGS,
    REQUIRED_NON_NULL_FIELDS,
    UnmappedFeedError,
    resolve_silver_feed,
)


class MissingRequiredFieldsError(ValueError):
    """Raised when a normalized Silver row is missing required non-null fields."""

    def __init__(self, feed: str, missing_fields: list[str], event_id: str | None = None):
        self.feed = feed
        self.missing_fields = missing_fields
        self.event_id = event_id
        message = f"missing_required_fields:{feed}:{','.join(missing_fields)}"
        if event_id:
            message = f"{message}:event_id={event_id}"
        super().__init__(message)


def envelope_to_silver_row(envelope: EventEnvelope) -> dict[str, Any]:
    """Convert a normalized EventEnvelope into a canonical Silver row."""
    silver_feed = resolve_silver_feed(envelope.feed)
    if silver_feed is None:
        raise UnmappedFeedError(f"unmapped_feed:{envelope.feed}")

    schema = SILVER_SCHEMAS[silver_feed]
    target_to_source = _target_to_source_map(silver_feed)

    row: dict[str, Any] = {
        "event_id": envelope.event_id,
        "provider": envelope.provider,
        "feed": silver_feed,
        "instrument_type": envelope.instrument_type,
        "instrument_key": envelope.instrument_key,
        "symbol": envelope.symbol,
        "ts_event": envelope.ts_event,
        "ts_ingest": envelope.ts_ingest,
        "ts_available": envelope.ts_available or envelope.ts_ingest,
        "source": envelope.source,
        "schema_version": envelope.schema_version,
        "quality_flags": envelope.quality_flags,
    }

    payload = envelope.payload

    for field in schema:
        field_name = field.name
        if field_name in row:
            continue

        source_names = list(target_to_source.get(field_name, []))
        if field_name not in source_names:
            source_names.append(field_name)

        value = None
        for source_name in source_names:
            value = payload.get(source_name)
            if value is not None:
                break

        row[field_name] = _coerce_value(value, field.type)

    return row


def enforce_required_non_null_fields(feed: str, row: Mapping[str, Any], event_id: str | None = None) -> None:
    """Raise when required Silver fields are missing for a feed."""
    missing = missing_required_non_null_fields(feed, row)
    if missing:
        raise MissingRequiredFieldsError(feed=feed, missing_fields=missing, event_id=event_id)


def missing_required_non_null_fields(feed: str, row: Mapping[str, Any]) -> list[str]:
    """Return sorted list of required fields that are null/blank in a row."""
    required = REQUIRED_NON_NULL_FIELDS.get(feed, set())
    missing: list[str] = []
    for field in required:
        value = row.get(field)
        if value is None:
            missing.append(field)
            continue
        if isinstance(value, str) and value.strip() == "":
            missing.append(field)
    return sorted(missing)


def _target_to_source_map(feed: str) -> dict[str, list[str]]:
    mapping = FIELD_MAPPINGS.get(feed, {})
    target_to_source: dict[str, list[str]] = {}
    for source_name, target_name in mapping.items():
        target_to_source.setdefault(target_name, []).append(source_name)
    return target_to_source


def _coerce_value(value: Any, arrow_type: pa.DataType) -> Any:
    if value is None:
        return None

    try:
        if pa.types.is_floating(arrow_type):
            return _coerce_float(value)
        if pa.types.is_integer(arrow_type):
            return _coerce_int(value)
        if pa.types.is_date(arrow_type):
            return _coerce_date(value)
        if pa.types.is_timestamp(arrow_type):
            return _coerce_timestamp(value)
        if pa.types.is_boolean(arrow_type):
            return _coerce_bool(value)
        if pa.types.is_list(arrow_type):
            return _coerce_list(value)
        if pa.types.is_string(arrow_type):
            return _coerce_string(value)
        return value
    except Exception:
        return None


def _coerce_float(value: Any) -> float | None:
    if value == "":
        return None
    return float(value)


def _coerce_int(value: Any) -> int | None:
    if value == "":
        return None
    return int(float(value))


def _coerce_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    return None


def _coerce_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, int | float):
        epoch = float(value)
        if epoch > 10_000_000_000:
            epoch /= 1000
        return datetime.fromtimestamp(epoch, tz=UTC)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return _coerce_timestamp(int(stripped))
        return datetime.fromisoformat(stripped.replace("Z", "+00:00"))
    return None


def _coerce_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "yes", "y"}:
        return True
    if text in {"0", "false", "f", "no", "n"}:
        return False
    return None


def _coerce_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple | set):
        return list(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return []
        try:
            parsed = json.loads(stripped)
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            pass
        return [value]
    return [value]


def _coerce_string(value: Any) -> str:
    if isinstance(value, dict | list):
        return json.dumps(value, default=str)
    return str(value)
