"""Tests for Gold Versioning (PRD §28)."""

import tempfile
from datetime import datetime, UTC
from pathlib import Path

import pytest

from heber.gold.versioning import (
    GoldVersion,
    VersionLineage,
    VersionManifest,
    UpstreamDependency,
    SchemaChange,
    CompatibilityResult,
    resolve_version,
    check_compatibility,
    list_available_versions,
    get_manifest_path,
)


class TestGoldVersion:
    """Test GoldVersion parsing and comparison."""
    
    def test_parse_with_v_prefix(self):
        v = GoldVersion.parse("v3.2.1")
        assert v.major == 3
        assert v.minor == 2
        assert v.patch == 1
    
    def test_parse_without_v_prefix(self):
        v = GoldVersion.parse("1.0.0")
        assert v.major == 1
        assert v.minor == 0
        assert v.patch == 0
    
    def test_parse_invalid_raises(self):
        with pytest.raises(ValueError):
            GoldVersion.parse("invalid")
    
    def test_str_representation(self):
        v = GoldVersion(major=3, minor=2, patch=1)
        assert str(v) == "v3.2.1"
    
    def test_comparison(self):
        v1 = GoldVersion.parse("v1.0.0")
        v2 = GoldVersion.parse("v2.0.0")
        v3 = GoldVersion.parse("v2.1.0")
        
        assert v1 < v2
        assert v2 < v3
        assert v1 <= v1
        assert v1 == GoldVersion.parse("v1.0.0")
    
    def test_wildcard_major(self):
        v = GoldVersion.parse("v3.5.2")
        assert v.matches_wildcard("v3.*")
        assert not v.matches_wildcard("v2.*")
    
    def test_wildcard_minor(self):
        v = GoldVersion.parse("v3.5.2")
        assert v.matches_wildcard("v3.5.*")
        assert not v.matches_wildcard("v3.4.*")
    
    def test_exact_match(self):
        v = GoldVersion.parse("v3.5.2")
        assert v.matches_wildcard("v3.5.2")
        assert not v.matches_wildcard("v3.5.1")


class TestVersionLineage:
    """Test VersionLineage serialization."""
    
    def test_to_dict(self):
        lineage = VersionLineage(
            upstream_deps=[
                UpstreamDependency(dataset="bars", layer="silver", version="v1.4"),
            ],
            code_commit="abc123",
            config_hash="def456",
        )
        
        d = lineage.to_dict()
        assert d["code_commit"] == "abc123"
        assert len(d["upstream_deps"]) == 1
        assert d["upstream_deps"][0]["dataset"] == "bars"
    
    def test_from_dict_roundtrip(self):
        lineage = VersionLineage(
            upstream_deps=[
                UpstreamDependency(dataset="bars", layer="silver", version="v1.4"),
            ],
            code_commit="abc123",
        )
        
        restored = VersionLineage.from_dict(lineage.to_dict())
        assert restored.code_commit == lineage.code_commit
        assert len(restored.upstream_deps) == 1


class TestResolveVersion:
    """Test version resolution with wildcards."""
    
    def test_resolve_latest(self):
        versions = [
            GoldVersion.parse("v1.0.0"),
            GoldVersion.parse("v2.0.0"),
            GoldVersion.parse("v3.2.1"),
        ]
        
        result = resolve_version(versions, None)
        assert result == GoldVersion.parse("v3.2.1")
    
    def test_resolve_major_wildcard(self):
        versions = [
            GoldVersion.parse("v2.0.0"),
            GoldVersion.parse("v3.2.1"),
            GoldVersion.parse("v3.5.0"),
        ]
        
        result = resolve_version(versions, "v3.*")
        assert result == GoldVersion.parse("v3.5.0")
    
    def test_resolve_minor_wildcard(self):
        versions = [
            GoldVersion.parse("v3.2.0"),
            GoldVersion.parse("v3.2.5"),
            GoldVersion.parse("v3.5.0"),
        ]
        
        result = resolve_version(versions, "v3.2.*")
        assert result == GoldVersion.parse("v3.2.5")
    
    def test_resolve_exact(self):
        versions = [
            GoldVersion.parse("v3.2.0"),
            GoldVersion.parse("v3.2.1"),
        ]
        
        result = resolve_version(versions, "v3.2.0")
        assert result == GoldVersion.parse("v3.2.0")
    
    def test_resolve_no_match(self):
        versions = [GoldVersion.parse("v1.0.0")]
        
        result = resolve_version(versions, "v3.*")
        assert result is None
    
    def test_resolve_empty_list(self):
        result = resolve_version([], "v1.0.0")
        assert result is None


class TestVersionManifest:
    """Test VersionManifest persistence and immutability."""
    
    def test_save_and_load(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest = VersionManifest(
                version=GoldVersion.parse("v1.0.0"),
                created_at=datetime.now(UTC),
                created_by="test_user",
                lineage=VersionLineage(code_commit="abc123"),
                schema_columns=["ts_event", "momentum_5d"],
                row_count=1000,
            )
            
            path = Path(tmpdir) / "_manifest.json"
            manifest.save(path)
            
            loaded = VersionManifest.load(path)
            assert loaded.version == manifest.version
            assert loaded.created_by == "test_user"
            assert loaded.schema_columns == ["ts_event", "momentum_5d"]
    
    def test_immutability_prevents_overwrite(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest = VersionManifest(
                version=GoldVersion.parse("v1.0.0"),
                created_at=datetime.now(UTC),
                created_by="test_user",
                lineage=VersionLineage(),
                immutable=True,
            )
            
            path = Path(tmpdir) / "_manifest.json"
            manifest.save(path)
            
            with pytest.raises(ValueError, match="immutable"):
                manifest.save(path)


class TestCheckCompatibility:
    """Test version compatibility checking (PRD §28.3)."""
    
    def test_compatible_added_column(self):
        from_manifest = VersionManifest(
            version=GoldVersion.parse("v3.2.0"),
            created_at=datetime.now(UTC),
            created_by="test",
            lineage=VersionLineage(),
            schema_columns=["ts_event", "momentum_5d"],
        )
        
        to_manifest = VersionManifest(
            version=GoldVersion.parse("v3.3.0"),
            created_at=datetime.now(UTC),
            created_by="test",
            lineage=VersionLineage(),
            schema_columns=["ts_event", "momentum_5d", "momentum_20d"],
        )
        
        result = check_compatibility(from_manifest, to_manifest)
        
        assert result.compatible is True
        assert result.breaking is False
        assert len(result.changes) == 1
        assert result.changes[0].change_type == "added_column"
    
    def test_breaking_removed_column(self):
        from_manifest = VersionManifest(
            version=GoldVersion.parse("v3.2.0"),
            created_at=datetime.now(UTC),
            created_by="test",
            lineage=VersionLineage(),
            schema_columns=["ts_event", "momentum_5d", "old_feature"],
        )
        
        to_manifest = VersionManifest(
            version=GoldVersion.parse("v4.0.0"),
            created_at=datetime.now(UTC),
            created_by="test",
            lineage=VersionLineage(),
            schema_columns=["ts_event", "momentum_5d"],
        )
        
        result = check_compatibility(from_manifest, to_manifest)
        
        assert result.compatible is False
        assert result.breaking is True
        assert any(c.change_type == "removed_column" for c in result.changes)
    
    def test_major_version_change_incompatible(self):
        from_manifest = VersionManifest(
            version=GoldVersion.parse("v2.0.0"),
            created_at=datetime.now(UTC),
            created_by="test",
            lineage=VersionLineage(),
            schema_columns=["ts_event"],
        )
        
        to_manifest = VersionManifest(
            version=GoldVersion.parse("v3.0.0"),
            created_at=datetime.now(UTC),
            created_by="test",
            lineage=VersionLineage(),
            schema_columns=["ts_event"],
        )
        
        result = check_compatibility(from_manifest, to_manifest)
        assert result.compatible is False


def run_all_gold_versioning_tests() -> dict[str, bool]:
    """Run all Gold versioning tests.
    
    Returns:
        Dict mapping test name to pass/fail
    """
    results = {}
    
    test_classes = [
        TestGoldVersion,
        TestVersionLineage,
        TestResolveVersion,
        TestVersionManifest,
        TestCheckCompatibility,
    ]
    
    for test_class in test_classes:
        instance = test_class()
        for method_name in dir(instance):
            if method_name.startswith("test_"):
                try:
                    getattr(instance, method_name)()
                    results[f"{test_class.__name__}.{method_name}"] = True
                except Exception as e:
                    results[f"{test_class.__name__}.{method_name}"] = False
                    print(f"FAILED: {test_class.__name__}.{method_name}: {e}")
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"\nGold Versioning Tests: {passed}/{total} passed")
    
    return results


if __name__ == "__main__":
    run_all_gold_versioning_tests()
