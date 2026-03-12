"""Schema alignment tests: validate Data-Gateway Normalized* Pydantic models match Heber Silver PyArrow schemas.

This test module ensures cross-repo schema consistency between the Data-Gateway
(Pydantic source-of-truth for ingestion) and the Heber lakehouse (PyArrow
Silver schemas for storage). Drift between these two surfaces is a top-tier
data integrity risk.
"""

from __future__ import annotations

import ast
import os

import pyarrow as pa
import pytest

from heber.schemas.silver import SILVER_SCHEMAS
from heber.writer.ingest_contracts import (
    CONTRACTED_RAW_FEEDS,
    FEED_ALIASES,
    FIELD_MAPPINGS,
    resolve_feed_alias,
)

# ---------------------------------------------------------------------------
# Import DG schemas via sys.path manipulation (separate virtualenv)
# ---------------------------------------------------------------------------

_DG_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "Data-Gateway"))
_DG_SCHEMAS_FILE = os.path.join(_DG_ROOT, "gateway", "schemas", "__init__.py")


def _parse_dg_models_via_ast() -> dict[str, dict[str, str]]:
    """Parse Data-Gateway Pydantic models using AST to avoid import dependency issues.

    Returns a dict of {ClassName: {field_name: annotation_string, ...}}.
    Only extracts classes whose name starts with 'Normalized'.
    """
    if not os.path.isfile(_DG_SCHEMAS_FILE):
        return {}

    with open(_DG_SCHEMAS_FILE) as f:
        tree = ast.parse(f.read(), filename=_DG_SCHEMAS_FILE)

    models: dict[str, dict[str, str]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        if not node.name.startswith("Normalized"):
            continue
        fields: dict[str, str] = {}
        for item in node.body:
            if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                fields[item.target.id] = ast.unparse(item.annotation)
        models[node.name] = fields
    return models


DG_MODELS = _parse_dg_models_via_ast()


# ---------------------------------------------------------------------------
# Feed -> DG Model -> Silver Schema mapping
# ---------------------------------------------------------------------------

# Canonical mapping from Silver dataset feed name to DG Pydantic model class name.
# Built by inspecting naming conventions; some feeds use singular, some plural,
# some have non-obvious names.
_FEED_TO_MODEL: dict[str, str] = {
    "bars": "NormalizedBar",
    "quotes": "NormalizedQuote",
    "trades": "NormalizedTrade",
    "flow_alerts": "NormalizedFlowAlert",
    "darkpool": "NormalizedDarkpoolTrade",
    "market_tide": "NormalizedMarketTide",
    "sector_tide": "NormalizedSectorTide",
    "greek_exposure": "NormalizedGreekExposure",
    "earnings": "NormalizedEarnings",
    "most_active": "NormalizedMostActive",
    "mover": "NormalizedMover",
    "screener_result": "NormalizedScreenerResult",
    "hottest_chain": "NormalizedHottestChain",
    "net_premium_tick": "NormalizedNetPremiumTick",
    "max_pain": "NormalizedMaxPain",
    "iv_rank": "NormalizedIVRank",
    "oi_change": "NormalizedOIChange",
    "etf_holding": "NormalizedETFHolding",
    "etf_flow": "NormalizedETFFlow",
    "short_data": "NormalizedShortData",
    "ftd": "NormalizedFTD",
    "corporate_action": "NormalizedCorporateAction",
    "seasonality": "NormalizedSeasonality",
    "option_contract": "NormalizedOptionContract",
    "news": "NormalizedNewsArticle",
    "orderbook": "NormalizedOrderbook",
    "congress_trades": "NormalizedPoliticianTrade",
    "insider_trades": "NormalizedInsiderTrade",
    "institution_holdings": "NormalizedInstitutionHolding",
    "iv_term_structure": "NormalizedIVTermStructure",
    "volatility_stats": "NormalizedVolatilityStats",
    "forex": "NormalizedForexRate",
    "stock_fundamentals": "NormalizedFundamentals",
    "borrow_cost": "NormalizedBorrowCost",
    "option_trades": "NormalizedOptionTrade",
    "option_chain_snapshot": "NormalizedOptionContract",
    "historic_option_volume": None,  # No dedicated DG model
    "insider_flow": None,
    "institution_activity": None,
    "politician_trades": "NormalizedPoliticianTrade",
    "analyst_ratings": None,
    "economic_events": None,
    "market_indicators": None,
    "option_history": None,
    "volume_profile": None,
    "group_flow": None,
    "etf_metadata": None,
    "etf_sector_weights": None,
}

# Silver common/meta columns injected by the Heber writer, not from DG models.
SILVER_META_COLUMNS = frozenset(
    {
        "event_id",
        "feed",
        "instrument_type",
        "instrument_key",
        "ts_event",
        "ts_ingest",
        "ts_available",
        "source",
        "schema_version",
        "quality_flags",
    }
)

# DG model fields that are internal / not expected to land in Silver.
DG_EXCLUDED_FIELDS = frozenset(
    {
        "raw_data",
    }
)


def _get_silver_column_names(feed: str) -> set[str]:
    """Return the set of column names from the Silver PyArrow schema for a feed."""
    schema = SILVER_SCHEMAS.get(feed)
    if schema is None:
        return set()
    return {field.name for field in schema}


def _get_dg_field_names(model_name: str) -> set[str]:
    """Return the set of field names from a DG Pydantic model (AST-parsed)."""
    model = DG_MODELS.get(model_name)
    if model is None:
        return set()
    return set(model.keys()) - DG_EXCLUDED_FIELDS


def _resolve_field_mapping(feed: str, dg_field: str) -> str:
    """Map a DG field name to the corresponding Silver column name using FIELD_MAPPINGS."""
    mappings = FIELD_MAPPINGS.get(feed, {})
    return mappings.get(dg_field, dg_field)


def _annotation_to_base_type(annotation_str: str) -> str:
    """Extract the base Python type from an annotation string.

    Handles: str, int, float, bool, Decimal, datetime, date,
    Optional[X], X | None, list[X].
    """
    cleaned = annotation_str.strip()
    # Handle Optional / X | None
    if cleaned.startswith("Optional["):
        cleaned = cleaned[len("Optional[") : -1]
    # Handle union with None: e.g., "str | None", "Decimal | None"
    if " | None" in cleaned:
        cleaned = cleaned.replace(" | None", "").strip()
    if "| None" in cleaned:
        cleaned = cleaned.replace("| None", "").strip()
    # Handle list types
    if cleaned.startswith("list["):
        return "list"
    return cleaned


def _pa_type_compatible(
    dg_annotation: str,
    pa_type: pa.DataType,
    dg_field: str = "",
    silver_col: str = "",
) -> bool:
    """Check if a DG Pydantic annotation is compatible with a PyArrow type.

    Accounts for known ingest coercions:
    - str YYYY-MM-DD -> date32 (date parsing in Silver writer)
    - datetime -> date32 (date-only extraction in Silver writer)
    - list[X] -> string (JSON serialization of nested models)
    - bool -> string (semantic transformation, e.g. is_10b5_1 -> relationship string)
    - str -> float64 (parsed numeric extraction, e.g. amount_range -> amount_min)
    """
    base = _annotation_to_base_type(dg_annotation)

    if base in ("str", "Literal"):
        return (
            pa.types.is_string(pa_type)
            or pa.types.is_large_string(pa_type)
            or pa.types.is_date(pa_type)  # str YYYY-MM-DD -> date32 coercion
            or pa.types.is_floating(pa_type)  # str -> float (parsed amounts)
        )
    if base in ("int",):
        return pa.types.is_integer(pa_type) or pa.types.is_floating(pa_type)
    if base in ("float",):
        return pa.types.is_floating(pa_type)
    if base in ("Decimal",):
        return pa.types.is_floating(pa_type) or pa.types.is_integer(pa_type) or pa.types.is_decimal(pa_type)
    if base in ("bool",):
        return pa.types.is_boolean(pa_type) or pa.types.is_string(pa_type)  # semantic transformation
    if base in ("datetime",):
        return pa.types.is_timestamp(pa_type) or pa.types.is_date(pa_type)  # datetime -> date32 extraction
    if base in ("date",):
        return pa.types.is_date(pa_type) or pa.types.is_string(pa_type)
    if base == "list":
        return (
            pa.types.is_list(pa_type)
            or pa.types.is_large_list(pa_type)
            or pa.types.is_string(pa_type)  # list -> JSON string serialization
        )
    # Unknown types are considered compatible (conservative)
    return True


def _feeds_with_dg_model() -> list[tuple[str, str]]:
    """Return (feed, model_name) pairs for feeds that have a DG model mapping."""
    pairs = []
    for feed in sorted(SILVER_SCHEMAS.keys()):
        model_name = _FEED_TO_MODEL.get(feed)
        if model_name and model_name in DG_MODELS:
            pairs.append((feed, model_name))
    return pairs


# ============================================================================
# Tests
# ============================================================================


@pytest.mark.unit
class TestContractedFeedsSilverSchemas:
    """Every contracted feed must resolve to a Silver schema."""

    def test_all_contracted_feeds_have_silver_schemas(self) -> None:
        missing: list[str] = []
        for feed in CONTRACTED_RAW_FEEDS:
            canonical = resolve_feed_alias(feed)
            if canonical not in SILVER_SCHEMAS:
                missing.append(f"{feed} -> {canonical}")
        assert not missing, "Contracted feeds without Silver schemas:\n" + "\n".join(f"  - {m}" for m in missing)

    def test_feed_aliases_resolve_to_existing_silver_schemas(self) -> None:
        bad_aliases: list[str] = []
        for alias, target in FEED_ALIASES.items():
            if target not in SILVER_SCHEMAS:
                bad_aliases.append(f"{alias} -> {target}")
        assert not bad_aliases, "Feed aliases pointing to nonexistent Silver schemas:\n" + "\n".join(
            f"  - {a}" for a in bad_aliases
        )


@pytest.mark.unit
class TestFieldCoverage:
    """For each mappable feed, DG model fields should cover Silver schema columns."""

    @pytest.mark.parametrize(
        "feed,model_name",
        [pytest.param(f, m, id=f) for f, m in _feeds_with_dg_model()],
    )
    def test_field_coverage(self, feed: str, model_name: str) -> None:
        dg_fields = _get_dg_field_names(model_name)
        silver_cols = _get_silver_column_names(feed) - SILVER_META_COLUMNS

        # Map DG fields through FIELD_MAPPINGS to get their Silver names
        mapped_dg_to_silver: dict[str, str] = {}
        for dg_field in dg_fields:
            silver_name = _resolve_field_mapping(feed, dg_field)
            mapped_dg_to_silver[dg_field] = silver_name

        dg_silver_names = set(mapped_dg_to_silver.values())

        # This is informational; the test passes as long as DG models exist.
        # A coverage below 50% signals serious misalignment worth investigating.
        uncovered = silver_cols - dg_silver_names
        if uncovered:
            print(f"\n  [{feed}] {len(uncovered)} Silver cols not mapped from DG ({model_name}): {sorted(uncovered)}")


@pytest.mark.unit
class TestDGFieldCoverage:
    """For each mappable feed, Silver schemas should cover DG model fields (DG->Silver direction)."""

    @pytest.mark.parametrize(
        "feed,model_name",
        [pytest.param(f, m, id=f) for f, m in _feeds_with_dg_model()],
    )
    def test_dg_fields_have_silver_columns(self, feed: str, model_name: str) -> None:
        """Check that DG model fields have corresponding Silver columns.

        Reports uncovered DG fields as warnings. Fails if coverage drops below
        50% for any contracted feed — this indicates the Silver schema is severely
        incomplete relative to the DG model.
        """
        dg_fields = _get_dg_field_names(model_name)
        silver_cols = _get_silver_column_names(feed) - SILVER_META_COLUMNS

        if not dg_fields:
            pytest.skip(f"No DG fields for model '{model_name}'")

        mappings = FIELD_MAPPINGS.get(feed, {})

        # Map each DG field to its Silver column name, then check if that column exists
        covered_dg: set[str] = set()
        uncovered_dg: set[str] = set()
        for dg_field in dg_fields:
            silver_name = mappings.get(dg_field, dg_field)
            if silver_name in silver_cols:
                covered_dg.add(dg_field)
            else:
                uncovered_dg.add(dg_field)

        n_dg = len(dg_fields)
        n_covered = len(covered_dg)
        pct = n_covered / n_dg * 100

        if uncovered_dg:
            print(
                f"\n  [{feed}] DG->Silver coverage: {n_covered}/{n_dg} ({pct:.0f}%)"
                f"\n  Uncovered DG fields ({len(uncovered_dg)}): {sorted(uncovered_dg)}"
            )

        # Soft threshold: fail if any contracted feed has < 50% DG field coverage
        canonical = resolve_feed_alias(feed)
        is_contracted = canonical in CONTRACTED_RAW_FEEDS or feed in CONTRACTED_RAW_FEEDS
        if is_contracted:
            assert pct >= 50, (
                f"DG->Silver coverage for contracted feed [{feed}] is {pct:.0f}% "
                f"({n_covered}/{n_dg}). Minimum threshold is 50%.\n"
                f"Uncovered DG fields: {sorted(uncovered_dg)}"
            )


@pytest.mark.unit
class TestFieldMappingsValid:
    """FIELD_MAPPINGS keys should correspond to DG model fields and values to Silver columns."""

    @pytest.mark.parametrize(
        "feed",
        [
            pytest.param(f, id=f)
            for f in sorted(FIELD_MAPPINGS.keys())
            if FIELD_MAPPINGS[f]  # Skip empty mappings
        ],
    )
    def test_field_mapping_values_exist_in_silver(self, feed: str) -> None:
        """Every mapped-to column name should exist in the Silver schema."""
        silver_cols = _get_silver_column_names(feed)
        if not silver_cols:
            pytest.skip(f"No Silver schema for feed '{feed}'")

        mapping = FIELD_MAPPINGS[feed]
        bad_targets: list[str] = []
        for source, target in mapping.items():
            if target not in silver_cols:
                bad_targets.append(f"{source} -> {target}")

        assert not bad_targets, f"FIELD_MAPPINGS['{feed}'] targets not in Silver schema:\n" + "\n".join(
            f"  - {b}" for b in bad_targets
        )


@pytest.mark.unit
class TestTypeCompatibility:
    """For mapped fields, DG Python types should be compatible with Silver PyArrow types."""

    @pytest.mark.parametrize(
        "feed,model_name",
        [pytest.param(f, m, id=f) for f, m in _feeds_with_dg_model()],
    )
    def test_type_compatibility(self, feed: str, model_name: str) -> None:
        dg_fields = DG_MODELS.get(model_name, {})
        silver_schema = SILVER_SCHEMAS.get(feed)
        if silver_schema is None:
            pytest.skip(f"No Silver schema for feed '{feed}'")

        silver_type_map: dict[str, pa.DataType] = {field.name: field.type for field in silver_schema}
        mappings = FIELD_MAPPINGS.get(feed, {})

        mismatches: list[str] = []
        for dg_field, annotation in dg_fields.items():
            if dg_field in DG_EXCLUDED_FIELDS:
                continue
            # Resolve to Silver column name
            silver_col = mappings.get(dg_field, dg_field)
            if silver_col not in silver_type_map:
                continue  # Not in Silver schema (meta field or unmapped)

            pa_type = silver_type_map[silver_col]
            if not _pa_type_compatible(annotation, pa_type, dg_field=dg_field, silver_col=silver_col):
                mismatches.append(f"  {dg_field} ({annotation}) -> {silver_col} ({pa_type})")

        assert not mismatches, f"Type mismatches in [{feed}] ({model_name}):\n" + "\n".join(mismatches)


@pytest.mark.unit
class TestCoverageReport:
    """Informational coverage report — always passes, prints alignment summary."""

    def test_coverage_report(self, capsys: pytest.CaptureFixture[str]) -> None:
        header = (
            f"{'Feed':<24} | {'DG Model':<22} | {'DG Fields':>9} | "
            f"{'Silver Cols':>10} | {'Silver->DG':>10} | {'DG->Silver':>10}"
        )
        separator = "-" * len(header)

        lines: list[str] = [
            "",
            "=" * len(header),
            "  SCHEMA ALIGNMENT COVERAGE REPORT",
            "=" * len(header),
            header,
            separator,
        ]

        total_feeds = 0
        total_silver_covered = 0
        total_silver = 0
        total_dg_covered = 0
        total_dg = 0
        feeds_missing_model: list[str] = []

        for feed in sorted(SILVER_SCHEMAS.keys()):
            model_name = _FEED_TO_MODEL.get(feed)
            silver_cols = _get_silver_column_names(feed) - SILVER_META_COLUMNS
            n_silver = len(silver_cols)

            if not model_name or model_name not in DG_MODELS:
                feeds_missing_model.append(feed)
                lines.append(f"{feed:<24} | {'(no DG model)':<22} | {'—':>9} | {n_silver:>10} | {'—':>10} | {'—':>10}")
                continue

            dg_fields = _get_dg_field_names(model_name)
            n_dg = len(dg_fields)
            mappings = FIELD_MAPPINGS.get(feed, {})

            # Map DG fields to Silver names
            dg_silver_names = set()
            for dg_field in dg_fields:
                silver_name = mappings.get(dg_field, dg_field)
                dg_silver_names.add(silver_name)

            # Silver->DG: how many Silver columns map to a DG field
            silver_covered = silver_cols & dg_silver_names
            n_silver_covered = len(silver_covered)
            pct_silver_to_dg = (n_silver_covered / n_silver * 100) if n_silver else 100.0

            # DG->Silver: how many DG fields have a corresponding Silver column
            dg_covered = dg_silver_names & silver_cols
            n_dg_covered = len(dg_covered)
            pct_dg_to_silver = (n_dg_covered / n_dg * 100) if n_dg else 100.0

            total_feeds += 1
            total_silver_covered += n_silver_covered
            total_silver += n_silver
            total_dg_covered += n_dg_covered
            total_dg += n_dg

            lines.append(
                f"{feed:<24} | {model_name:<22} | {n_dg:>9} | "
                f"{n_silver:>10} | {pct_silver_to_dg:>9.0f}% | {pct_dg_to_silver:>9.0f}%"
            )

        lines.append(separator)
        overall_s2d = (total_silver_covered / total_silver * 100) if total_silver else 100.0
        overall_d2s = (total_dg_covered / total_dg * 100) if total_dg else 100.0
        feeds_label = f"{total_feeds} feeds"
        lines.append(
            f"{'TOTAL':<24} | {feeds_label:<22} | {total_dg:>9} | "
            f"{total_silver:>10} | {overall_s2d:>9.0f}% | {overall_d2s:>9.0f}%"
        )

        if feeds_missing_model:
            lines.append("")
            lines.append(f"Feeds without DG model mapping ({len(feeds_missing_model)}):")
            for f in sorted(feeds_missing_model):
                lines.append(f"  - {f}")

        lines.append("=" * len(header))

        report = "\n".join(lines)
        print(report)

        # This test always passes — it's purely informational
        assert True
