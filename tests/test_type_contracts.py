"""Type contract tests for heber/catalog/ and heber/models/.

Verifies that public functions and methods return the documented types
and handle edge cases correctly. Written as part of TDD-driven type
annotation improvements — these tests should pass BEFORE and AFTER
annotations are added since no runtime behavior changes.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

# ---------------------------------------------------------------------------
# heber.catalog.urn
# ---------------------------------------------------------------------------
from heber.catalog.urn import (
    PATH_TEMPLATES,
    DatasetURN,
    get_path_template,
    resolve_path,
)


class TestDatasetURN:
    """Type contracts for DatasetURN."""

    def test_parse_returns_dataset_urn(self) -> None:
        result = DatasetURN.parse("heber://silver/bars@v1")
        assert isinstance(result, DatasetURN)

    def test_parse_silver_fields(self) -> None:
        urn = DatasetURN.parse("heber://silver/bars@v1")
        assert isinstance(urn.layer, str)
        assert isinstance(urn.dataset, str)
        assert isinstance(urn.version, str)
        assert urn.project is None
        assert urn.layer == "silver"
        assert urn.dataset == "bars"
        assert urn.version == "v1"

    def test_parse_gold_with_project(self) -> None:
        urn = DatasetURN.parse("heber://gold/kairos/features@v2")
        assert isinstance(urn.project, str)
        assert urn.project == "kairos"
        assert urn.dataset == "features"
        assert urn.version == "v2"

    def test_parse_default_version(self) -> None:
        urn = DatasetURN.parse("heber://silver/bars")
        assert urn.version == "v1"

    def test_parse_invalid_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Invalid URN format"):
            DatasetURN.parse("not-a-valid-urn")

    def test_str_returns_string(self) -> None:
        urn = DatasetURN(layer="silver", dataset="bars", version="v1")
        result = str(urn)
        assert isinstance(result, str)
        assert result == "heber://silver/bars@v1"

    def test_str_gold_with_project(self) -> None:
        urn = DatasetURN(layer="gold", dataset="features", version="v1", project="kairos")
        assert str(urn) == "heber://gold/kairos/features@v1"

    def test_parse_roundtrip(self) -> None:
        original = "heber://silver/quotes@v1"
        urn = DatasetURN.parse(original)
        assert str(urn) == original


class TestGetPathTemplate:
    """Type contracts for get_path_template."""

    def test_returns_string(self) -> None:
        result = get_path_template("silver")
        assert isinstance(result, str)

    def test_silver_hourly_for_quotes(self) -> None:
        result = get_path_template("silver", "quotes")
        assert isinstance(result, str)
        assert "hour" in result

    def test_silver_hourly_for_trades(self) -> None:
        result = get_path_template("silver", "trades")
        assert "hour" in result

    def test_bronze_template(self) -> None:
        result = get_path_template("bronze")
        assert isinstance(result, str)
        assert "provider" in result

    def test_gold_template(self) -> None:
        result = get_path_template("gold")
        assert isinstance(result, str)
        assert "project" in result

    def test_unknown_layer_returns_string(self) -> None:
        result = get_path_template("unknown_layer")
        assert isinstance(result, str)


class TestResolvePath:
    """Type contracts for resolve_path."""

    def test_returns_path(self, tmp_path: Path) -> None:
        result = resolve_path("heber://silver/bars@v1", base_path=tmp_path)
        assert isinstance(result, Path)

    def test_with_date(self, tmp_path: Path) -> None:
        result = resolve_path("heber://silver/bars@v1", dt=date(2025, 1, 15), base_path=tmp_path)
        assert isinstance(result, Path)
        assert "2025-01-15" in str(result)

    def test_with_urn_object(self, tmp_path: Path) -> None:
        urn = DatasetURN(layer="silver", dataset="bars", version="v1")
        result = resolve_path(urn, base_path=tmp_path)
        assert isinstance(result, Path)

    def test_with_path_base_path(self, tmp_path: Path) -> None:
        result = resolve_path("heber://silver/bars@v1", base_path=tmp_path)
        assert isinstance(result, Path)
        assert str(result).startswith(str(tmp_path))


class TestPathTemplates:
    """Type contracts for PATH_TEMPLATES constant."""

    def test_is_dict_of_strings(self) -> None:
        assert isinstance(PATH_TEMPLATES, dict)
        for key, value in PATH_TEMPLATES.items():
            assert isinstance(key, str), f"Key {key!r} is not str"
            assert isinstance(value, str), f"Value for {key!r} is not str"


# ---------------------------------------------------------------------------
# heber.catalog.access_control
# ---------------------------------------------------------------------------
from heber.catalog.access_control import (
    AccessControlManager,
    AccessLevel,
    DatasetPermission,
    Project,
    SDKToken,
)
from heber.retention import DataLayer


class TestProject:
    """Type contracts for Project dataclass."""

    def test_to_dict_returns_dict(self) -> None:
        p = Project(project_id="p1", name="test", owner="admin")
        result = p.to_dict()
        assert isinstance(result, dict)
        for key in ("project_id", "name", "description", "owner", "created_at"):
            assert key in result

    def test_fields_have_correct_types(self) -> None:
        p = Project(project_id="p1", name="test")
        assert isinstance(p.project_id, str)
        assert isinstance(p.name, str)
        assert isinstance(p.description, str)
        assert isinstance(p.created_at, datetime)


class TestDatasetPermission:
    """Type contracts for DatasetPermission dataclass."""

    def test_to_dict_returns_dict(self) -> None:
        perm = DatasetPermission(
            project_id="p1",
            dataset="bars",
            layer=DataLayer.SILVER,
            access_level=AccessLevel.READ,
        )
        result = perm.to_dict()
        assert isinstance(result, dict)
        assert result["layer"] == "silver"
        assert result["access_level"] == "read"


class TestSDKToken:
    """Type contracts for SDKToken dataclass."""

    def test_is_valid_returns_bool(self) -> None:
        token = SDKToken(
            token_id="tok_1",
            project_id="p1",
            token_hash="abc",
            name="test",
        )
        assert isinstance(token.is_valid(), bool)
        assert token.is_valid() is True

    def test_expired_token_is_invalid(self) -> None:
        token = SDKToken(
            token_id="tok_1",
            project_id="p1",
            token_hash="abc",
            name="test",
            expires_at=datetime.now(UTC) - timedelta(days=1),
        )
        assert token.is_valid() is False

    def test_revoked_token_is_invalid(self) -> None:
        token = SDKToken(
            token_id="tok_1",
            project_id="p1",
            token_hash="abc",
            name="test",
            revoked=True,
        )
        assert token.is_valid() is False

    def test_has_scope_returns_bool(self) -> None:
        token = SDKToken(
            token_id="tok_1",
            project_id="p1",
            token_hash="abc",
            name="test",
            scopes=["read", "write"],
        )
        assert isinstance(token.has_scope("read"), bool)
        assert token.has_scope("read") is True
        assert token.has_scope("admin") is False

    def test_wildcard_scope(self) -> None:
        token = SDKToken(
            token_id="tok_1",
            project_id="p1",
            token_hash="abc",
            name="test",
            scopes=["*"],
        )
        assert token.has_scope("anything") is True

    def test_to_dict_returns_dict(self) -> None:
        token = SDKToken(
            token_id="tok_1",
            project_id="p1",
            token_hash="abc",
            name="test",
        )
        result = token.to_dict()
        assert isinstance(result, dict)
        assert "is_valid" in result
        assert isinstance(result["is_valid"], bool)


class TestAccessControlManager:
    """Type contracts for AccessControlManager."""

    def test_create_project_returns_project(self) -> None:
        mgr = AccessControlManager()
        result = mgr.create_project("p1", "Test Project", "admin")
        assert isinstance(result, Project)
        assert result.project_id == "p1"

    def test_get_project_returns_project_or_none(self) -> None:
        mgr = AccessControlManager()
        assert mgr.get_project("nonexistent") is None
        mgr.create_project("p1", "Test")
        result = mgr.get_project("p1")
        assert isinstance(result, Project)

    def test_grant_permission_returns_permission(self) -> None:
        mgr = AccessControlManager()
        mgr.create_project("p1", "Test")
        result = mgr.grant_permission("p1", "bars", DataLayer.GOLD, AccessLevel.READ)
        assert isinstance(result, DatasetPermission)

    def test_check_access_returns_bool(self) -> None:
        mgr = AccessControlManager()
        mgr.create_project("p1", "Test")
        result = mgr.check_access("p1", "bars", DataLayer.SILVER)
        assert isinstance(result, bool)

    def test_silver_shared_by_default(self) -> None:
        mgr = AccessControlManager()
        mgr.create_project("p1", "Test")
        assert mgr.check_access("p1", "bars", DataLayer.SILVER) is True

    def test_gold_denied_without_permission(self) -> None:
        mgr = AccessControlManager()
        mgr.create_project("p1", "Test")
        assert mgr.check_access("p1", "features", DataLayer.GOLD) is False

    def test_create_token_returns_tuple(self) -> None:
        mgr = AccessControlManager()
        mgr.create_project("p1", "Test")
        result = mgr.create_token("p1", "my-token")
        assert isinstance(result, tuple)
        assert len(result) == 2
        raw_token, sdk_token = result
        assert isinstance(raw_token, str)
        assert isinstance(sdk_token, SDKToken)

    def test_validate_token_returns_token_or_none(self) -> None:
        mgr = AccessControlManager()
        mgr.create_project("p1", "Test")
        raw_token, _ = mgr.create_token("p1", "my-token")
        result = mgr.validate_token(raw_token)
        assert isinstance(result, SDKToken)
        assert mgr.validate_token("invalid-token") is None

    def test_revoke_token_returns_bool(self) -> None:
        mgr = AccessControlManager()
        mgr.create_project("p1", "Test")
        _, sdk_token = mgr.create_token("p1", "my-token")
        assert isinstance(mgr.revoke_token(sdk_token.token_id), bool)
        assert mgr.revoke_token(sdk_token.token_id) is True
        assert mgr.revoke_token("nonexistent") is False

    def test_list_project_permissions_returns_list_of_dicts(self) -> None:
        mgr = AccessControlManager()
        mgr.create_project("p1", "Test")
        mgr.grant_permission("p1", "bars", DataLayer.SILVER, AccessLevel.READ)
        result = mgr.list_project_permissions("p1")
        assert isinstance(result, list)
        assert all(isinstance(item, dict) for item in result)

    def test_list_project_tokens_returns_list_of_dicts(self) -> None:
        mgr = AccessControlManager()
        mgr.create_project("p1", "Test")
        mgr.create_token("p1", "tok")
        result = mgr.list_project_tokens("p1")
        assert isinstance(result, list)
        assert all(isinstance(item, dict) for item in result)

    def test_generate_report_returns_dict(self) -> None:
        mgr = AccessControlManager()
        result = mgr.generate_report()
        assert isinstance(result, dict)
        assert "summary" in result
        assert "shared_layers" in result
        assert "projects" in result


# ---------------------------------------------------------------------------
# heber.catalog.datasources
# ---------------------------------------------------------------------------
from heber.catalog.datasources import (
    DataProvider,
    DatasetCatalog,
    DatasetSpec,
    DataTypeSpec,
    ProviderPriority,
    ProviderRegistry,
    StorageBoundary,
)


class TestDataProvider:
    """Type contracts for DataProvider."""

    def test_to_dict_returns_dict(self) -> None:
        p = DataProvider(
            name="test",
            capabilities=["bars"],
            priority=ProviderPriority.PRIMARY,
            streaming=True,
        )
        result = p.to_dict()
        assert isinstance(result, dict)
        assert isinstance(result["capabilities"], list)
        assert isinstance(result["priority"], int)


class TestDataTypeSpec:
    """Type contracts for DataTypeSpec."""

    def test_to_dict_returns_dict(self) -> None:
        spec = DataTypeSpec("market_data", StorageBoundary.HEBER, "Parquet", "Columnar")
        result = spec.to_dict()
        assert isinstance(result, dict)
        assert result["storage"] == "heber"


class TestDatasetSpec:
    """Type contracts for DatasetSpec."""

    def test_to_dict_returns_dict(self) -> None:
        spec = DatasetSpec("bars", "market", "OHLCV bars", ["Alpaca"], True)
        result = spec.to_dict()
        assert isinstance(result, dict)
        assert isinstance(result["providers"], list)
        assert isinstance(result["implemented"], bool)


class TestProviderRegistry:
    """Type contracts for ProviderRegistry."""

    def test_get_returns_provider_or_none(self) -> None:
        reg = ProviderRegistry()
        result = reg.get("Alpaca")
        assert isinstance(result, DataProvider)
        assert reg.get("nonexistent") is None

    def test_list_all_returns_list_of_dicts(self) -> None:
        reg = ProviderRegistry()
        result = reg.list_all()
        assert isinstance(result, list)
        assert all(isinstance(item, dict) for item in result)

    def test_get_by_capability_returns_list_of_providers(self) -> None:
        reg = ProviderRegistry()
        result = reg.get_by_capability("bars")
        assert isinstance(result, list)
        assert all(isinstance(p, DataProvider) for p in result)

    def test_get_primary_returns_list_of_providers(self) -> None:
        reg = ProviderRegistry()
        result = reg.get_primary()
        assert isinstance(result, list)
        assert all(isinstance(p, DataProvider) for p in result)


class TestDatasetCatalog:
    """Type contracts for DatasetCatalog."""

    def test_get_returns_spec_or_none(self) -> None:
        cat = DatasetCatalog()
        result = cat.get("bars")
        assert isinstance(result, DatasetSpec)
        assert cat.get("nonexistent") is None

    def test_list_all_returns_list_of_dicts(self) -> None:
        cat = DatasetCatalog()
        result = cat.list_all()
        assert isinstance(result, list)
        assert all(isinstance(item, dict) for item in result)

    def test_list_by_category_returns_list_of_specs(self) -> None:
        cat = DatasetCatalog()
        result = cat.list_by_category("market_data")
        assert isinstance(result, list)
        assert all(isinstance(s, DatasetSpec) for s in result)

    def test_list_implemented_returns_list_of_specs(self) -> None:
        cat = DatasetCatalog()
        result = cat.list_implemented()
        assert isinstance(result, list)
        assert all(isinstance(s, DatasetSpec) for s in result)

    def test_list_pending_returns_list_of_specs(self) -> None:
        cat = DatasetCatalog()
        result = cat.list_pending()
        assert isinstance(result, list)

    def test_get_storage_boundary_returns_enum_or_none(self) -> None:
        cat = DatasetCatalog()
        result = cat.get_storage_boundary("market_data")
        assert isinstance(result, StorageBoundary)
        assert cat.get_storage_boundary("nonexistent") is None

    def test_generate_report_returns_dict(self) -> None:
        cat = DatasetCatalog()
        result = cat.generate_report()
        assert isinstance(result, dict)
        assert "summary" in result
        assert "by_category" in result


# ---------------------------------------------------------------------------
# heber.catalog.seeds helpers
# ---------------------------------------------------------------------------
from heber.catalog.seeds import _arrow_type_to_str, _schema_to_json


class TestArrowTypeToStr:
    """Type contracts for _arrow_type_to_str."""

    def test_returns_string_for_string_type(self) -> None:
        result = _arrow_type_to_str(pa.string())
        assert isinstance(result, str)

    def test_returns_string_for_int_type(self) -> None:
        result = _arrow_type_to_str(pa.int64())
        assert isinstance(result, str)

    def test_returns_string_for_float_type(self) -> None:
        result = _arrow_type_to_str(pa.float64())
        assert isinstance(result, str)

    def test_returns_string_for_timestamp_type(self) -> None:
        result = _arrow_type_to_str(pa.timestamp("us", tz="UTC"))
        assert isinstance(result, str)


class TestSchemaToJson:
    """Type contracts for _schema_to_json."""

    def test_returns_dict_with_fields_key(self) -> None:
        schema = pa.schema([pa.field("col_a", pa.string()), pa.field("col_b", pa.int64())])
        result = _schema_to_json(schema)
        assert isinstance(result, dict)
        assert "fields" in result
        assert isinstance(result["fields"], list)

    def test_field_entries_have_correct_keys(self) -> None:
        schema = pa.schema([pa.field("col_a", pa.string(), nullable=False)])
        result = _schema_to_json(schema)
        field_entry = result["fields"][0]
        assert isinstance(field_entry, dict)
        assert "name" in field_entry
        assert "type" in field_entry
        assert "nullable" in field_entry
        assert isinstance(field_entry["name"], str)
        assert isinstance(field_entry["type"], str)
        assert isinstance(field_entry["nullable"], bool)


# ---------------------------------------------------------------------------
# heber.models.envelope
# ---------------------------------------------------------------------------
from heber.models.envelope import (
    ENVELOPE_SCHEMA_VERSION,
    EventEnvelope,
    Lineage,
    validate_instrument_key,
)


class TestValidateInstrumentKey:
    """Type contracts for validate_instrument_key."""

    def test_returns_bool(self) -> None:
        result = validate_instrument_key("equity:AAPL", "equity")
        assert isinstance(result, bool)

    def test_valid_equity(self) -> None:
        assert validate_instrument_key("equity:AAPL", "equity") is True

    def test_valid_crypto(self) -> None:
        assert validate_instrument_key("crypto:BTC-USD", "crypto") is True

    def test_valid_forex(self) -> None:
        assert validate_instrument_key("forex:USD-EUR", "forex") is True

    def test_valid_option(self) -> None:
        assert validate_instrument_key("option:OCC:AAPL260116C00200000", "option") is True

    def test_invalid_key(self) -> None:
        assert validate_instrument_key("bad_key", "equity") is False

    def test_unknown_type(self) -> None:
        assert validate_instrument_key("equity:AAPL", "unknown_type") is False


class TestLineage:
    """Type contracts for Lineage model."""

    def test_default_fields_are_none(self) -> None:
        lineage = Lineage()
        assert lineage.client_id is None
        assert lineage.project is None
        assert lineage.request_id is None

    def test_fields_accept_strings(self) -> None:
        lineage = Lineage(client_id="c1", project="proj", trace_id="t1")
        assert isinstance(lineage.client_id, str)
        assert isinstance(lineage.project, str)


class TestEventEnvelope:
    """Type contracts for EventEnvelope model."""

    @pytest.fixture()
    def base_envelope_data(self) -> dict[str, Any]:
        now = datetime.now(UTC)
        return {
            "event_id": "abc123",
            "provider": "alpaca",
            "feed": "bars",
            "source": "websocket",
            "instrument_type": "equity",
            "instrument_key": "equity:AAPL",
            "symbol": "AAPL",
            "ts_event": now,
            "ts_ingest": now,
            "payload": {"open": 150.0, "close": 151.0},
        }

    def test_construction(self, base_envelope_data: dict[str, Any]) -> None:
        envelope = EventEnvelope(**base_envelope_data)
        assert isinstance(envelope, EventEnvelope)

    def test_schema_version_default(self, base_envelope_data: dict[str, Any]) -> None:
        envelope = EventEnvelope(**base_envelope_data)
        assert isinstance(envelope.schema_version, str)
        assert envelope.schema_version == ENVELOPE_SCHEMA_VERSION

    def test_ts_available_default_none(self, base_envelope_data: dict[str, Any]) -> None:
        envelope = EventEnvelope(**base_envelope_data)
        assert envelope.ts_available is None

    def test_ts_effective_property_none_when_no_available(self, base_envelope_data: dict[str, Any]) -> None:
        envelope = EventEnvelope(**base_envelope_data)
        assert envelope.ts_effective is None

    def test_ts_effective_returns_datetime(self, base_envelope_data: dict[str, Any]) -> None:
        now = datetime.now(UTC)
        base_envelope_data["ts_available"] = now
        base_envelope_data["processing_delay_ms"] = 100
        envelope = EventEnvelope(**base_envelope_data)
        result = envelope.ts_effective
        assert isinstance(result, datetime)
        assert result == now + timedelta(milliseconds=100)

    def test_with_ts_available_returns_envelope(self, base_envelope_data: dict[str, Any]) -> None:
        envelope = EventEnvelope(**base_envelope_data)
        now = datetime.now(UTC)
        result = envelope.with_ts_available(now)
        assert isinstance(result, EventEnvelope)
        assert result.ts_available == now

    def test_is_valid_instrument_key_returns_bool(self, base_envelope_data: dict[str, Any]) -> None:
        envelope = EventEnvelope(**base_envelope_data)
        result = envelope.is_valid_instrument_key()
        assert isinstance(result, bool)

    def test_naive_datetime_gets_utc(self, base_envelope_data: dict[str, Any]) -> None:
        base_envelope_data["ts_event"] = datetime(2025, 1, 1, 12, 0, 0)
        base_envelope_data["ts_ingest"] = datetime(2025, 1, 1, 12, 0, 0)
        envelope = EventEnvelope(**base_envelope_data)
        assert envelope.ts_event.tzinfo is not None

    def test_lineage_default_empty_dict(self, base_envelope_data: dict[str, Any]) -> None:
        envelope = EventEnvelope(**base_envelope_data)
        assert isinstance(envelope.lineage, dict)

    def test_quality_flags_default_empty_list(self, base_envelope_data: dict[str, Any]) -> None:
        envelope = EventEnvelope(**base_envelope_data)
        assert isinstance(envelope.quality_flags, list)


# ---------------------------------------------------------------------------
# heber.models.silver
# ---------------------------------------------------------------------------
from heber.models.silver import (
    BarRecord,
    DarkpoolTradeRecord,
    FlowAlertRecord,
    QuoteRecord,
    SilverBase,
    TradeRecord,
    get_bars_schema,
    get_darkpool_schema,
    get_darkpool_trades_schema,
    get_option_contracts_schema,
)


class TestSilverBase:
    """Type contracts for SilverBase and schema version defaults."""

    def test_schema_version_defaults_dict_is_classvar(self) -> None:
        assert isinstance(SilverBase._SCHEMA_VERSION_DEFAULTS, dict)
        for key, value in SilverBase._SCHEMA_VERSION_DEFAULTS.items():
            assert isinstance(key, str)
            assert isinstance(value, str)


class TestSilverRecordConstruction:
    """Type contracts for Silver record construction."""

    @pytest.fixture()
    def base_fields(self) -> dict[str, Any]:
        now = datetime.now(UTC)
        return {
            "event_id": "e1",
            "provider": "alpaca",
            "feed": "bars",
            "instrument_type": "equity",
            "instrument_key": "equity:AAPL",
            "symbol": "AAPL",
            "ts_event": now,
            "ts_ingest": now,
            "ts_available": now,
            "source": "websocket",
        }

    def test_bar_record(self, base_fields: dict[str, Any]) -> None:
        base_fields["feed"] = "bars"
        rec = BarRecord(
            **base_fields,
            timeframe="1Min",
            bar_start_ts=datetime.now(UTC),
            open=150.0,
            high=152.0,
            low=149.0,
            close=151.0,
            volume=10000.0,
        )
        assert isinstance(rec, BarRecord)
        assert isinstance(rec, SilverBase)
        assert isinstance(rec.open, float)
        assert isinstance(rec.volume, float)

    def test_quote_record(self, base_fields: dict[str, Any]) -> None:
        base_fields["feed"] = "quotes"
        rec = QuoteRecord(
            **base_fields,
            bid_px=150.0,
            bid_sz=100.0,
            ask_px=150.05,
            ask_sz=200.0,
        )
        assert isinstance(rec, QuoteRecord)
        assert isinstance(rec.bid_px, float)

    def test_trade_record(self, base_fields: dict[str, Any]) -> None:
        base_fields["feed"] = "trades"
        rec = TradeRecord(
            **base_fields,
            price=150.25,
            size=50.0,
        )
        assert isinstance(rec, TradeRecord)

    def test_flow_alert_record(self, base_fields: dict[str, Any]) -> None:
        base_fields["feed"] = "flow_alerts"
        base_fields["provider"] = "unusual_whales"
        rec = FlowAlertRecord(
            **base_fields,
            underlying="AAPL",
            expiry=date(2026, 1, 16),
            strike=200.0,
            put_call="C",
            premium=50000.0,
            volume=1000.0,
            alert_type="SWEEP",
        )
        assert isinstance(rec, FlowAlertRecord)
        assert isinstance(rec.expiry, date)
        assert isinstance(rec.strike, float)

    def test_darkpool_trade_record(self, base_fields: dict[str, Any]) -> None:
        base_fields["feed"] = "darkpool"
        base_fields["provider"] = "unusual_whales"
        rec = DarkpoolTradeRecord(
            **base_fields,
            underlying="AAPL",
            price=150.0,
            size=10000.0,
        )
        assert isinstance(rec, DarkpoolTradeRecord)

    def test_lineage_dict_serialized_to_json_string(self, base_fields: dict[str, Any]) -> None:
        """Lineage dicts are serialized to JSON strings by the model validator."""
        base_fields["lineage"] = {"trace_id": "t1", "seq": 1}
        rec = BarRecord(
            **base_fields,
            timeframe="1Min",
            bar_start_ts=datetime.now(UTC),
            open=1.0,
            high=2.0,
            low=0.5,
            close=1.5,
            volume=100.0,
        )
        assert isinstance(rec.lineage, str)


class TestSchemaFunctions:
    """Type contracts for PyArrow schema factory functions."""

    def test_get_bars_schema_returns_pa_schema(self) -> None:
        result = get_bars_schema()
        assert isinstance(result, pa.Schema)

    def test_get_darkpool_schema_returns_pa_schema(self) -> None:
        result = get_darkpool_schema()
        assert isinstance(result, pa.Schema)

    def test_get_darkpool_trades_schema_returns_pa_schema(self) -> None:
        result = get_darkpool_trades_schema()
        assert isinstance(result, pa.Schema)

    def test_get_option_contracts_schema_returns_pa_schema(self) -> None:
        result = get_option_contracts_schema()
        assert isinstance(result, pa.Schema)

    def test_darkpool_trades_alias_equals_darkpool(self) -> None:
        """get_darkpool_trades_schema is an alias for get_darkpool_schema."""
        assert get_darkpool_trades_schema() == get_darkpool_schema()


# ---------------------------------------------------------------------------
# heber.catalog.openmetadata_client
# ---------------------------------------------------------------------------
from heber.catalog.openmetadata_client import (
    ColumnMetadata,
    LineageEdge,
    MockOpenMetadataClient,
    OpenMetadataCatalog,
    OpenMetadataConfig,
    TableMetadata,
    get_catalog,
)


class TestOpenMetadataConfig:
    """Type contracts for OpenMetadataConfig."""

    def test_defaults(self) -> None:
        config = OpenMetadataConfig()
        assert isinstance(config.host, str)
        assert isinstance(config.api_key, str)


class TestTableMetadata:
    """Type contracts for TableMetadata."""

    def test_defaults(self) -> None:
        tm = TableMetadata(name="bars", layer=DataLayer.SILVER)
        assert isinstance(tm.name, str)
        assert isinstance(tm.layer, DataLayer)
        assert isinstance(tm.tags, list)
        assert isinstance(tm.custom_properties, dict)
        assert isinstance(tm.zero_leakage_enabled, bool)
        assert isinstance(tm.freshness_sla_hours, int)


class TestColumnMetadata:
    """Type contracts for ColumnMetadata."""

    def test_defaults(self) -> None:
        cm = ColumnMetadata(name="col", data_type="string")
        assert isinstance(cm.name, str)
        assert isinstance(cm.is_nullable, bool)
        assert isinstance(cm.tags, list)


class TestLineageEdge:
    """Type contracts for LineageEdge."""

    def test_defaults(self) -> None:
        edge = LineageEdge(upstream_table="a", downstream_table="b")
        assert isinstance(edge.column_mapping, dict)
        assert edge.transformation is None


class TestMockOpenMetadataClient:
    """Type contracts for MockOpenMetadataClient."""

    def test_create_or_update_table(self) -> None:
        client = MockOpenMetadataClient()
        result = client.create_or_update_table(name="bars", database_schema="heber.silver")
        assert result is None

    def test_get_table_by_name_returns_none_for_missing(self) -> None:
        client = MockOpenMetadataClient()
        assert client.get_table_by_name("nonexistent") is None

    def test_list_tables_returns_list(self) -> None:
        client = MockOpenMetadataClient()
        result = client.list_tables()
        assert isinstance(result, list)

    def test_add_lineage_returns_none(self) -> None:
        client = MockOpenMetadataClient()
        result = client.add_lineage(from_entity="a", to_entity="b")
        assert result is None


class TestOpenMetadataCatalog:
    """Type contracts for OpenMetadataCatalog (uses mock client)."""

    def test_register_table_returns_str(self) -> None:
        catalog = OpenMetadataCatalog(config=OpenMetadataConfig())
        fqn = catalog.register_table(TableMetadata(name="bars", layer=DataLayer.SILVER))
        assert isinstance(fqn, str)
        assert "bars" in fqn

    def test_get_table_returns_none_for_mock(self) -> None:
        catalog = OpenMetadataCatalog(config=OpenMetadataConfig())
        result = catalog.get_table("heber.silver.bars")
        assert result is None

    def test_list_tables_returns_list(self) -> None:
        catalog = OpenMetadataCatalog(config=OpenMetadataConfig())
        result = catalog.list_tables()
        assert isinstance(result, list)

    def test_add_lineage_returns_none(self) -> None:
        catalog = OpenMetadataCatalog(config=OpenMetadataConfig())
        result = catalog.add_lineage(LineageEdge(upstream_table="a", downstream_table="b"))
        assert result is None

    def test_get_lineage_returns_list(self) -> None:
        catalog = OpenMetadataCatalog(config=OpenMetadataConfig())
        result = catalog.get_lineage("heber.silver.bars")
        assert isinstance(result, list)

    def test_search_returns_list(self) -> None:
        catalog = OpenMetadataCatalog(config=OpenMetadataConfig())
        result = catalog.search("bars")
        assert isinstance(result, list)


class TestGetCatalog:
    """Type contracts for get_catalog singleton."""

    def test_returns_catalog_instance(self) -> None:
        # Clear LRU cache for test isolation
        get_catalog.cache_clear()
        import heber.catalog.openmetadata_client as mod

        mod._catalog = None
        result = get_catalog()
        assert isinstance(result, OpenMetadataCatalog)
        # Cleanup
        get_catalog.cache_clear()
        mod._catalog = None
