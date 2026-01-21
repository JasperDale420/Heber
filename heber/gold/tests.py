"""Tests for Gold Versioning using lakeFS (PRD §28).

These tests verify the lakeFS-based versioning system that replaces
the legacy GoldVersion/VersionManifest implementation.
"""

from unittest.mock import patch

import pytest

from heber.versioning import (
    GoldCommit,
    GoldTag,
    LakeFSConfig,
    LakeFSVersionManager,
)


class TestLakeFSConfig:
    """Test lakeFS configuration loading."""

    def test_from_env_defaults(self):
        """Test default configuration values."""
        with patch.dict("os.environ", {}, clear=True):
            config = LakeFSConfig()
            assert config.endpoint == "http://localhost:8000"
            assert config.default_repo == "heber-gold"

    def test_from_env_custom(self):
        """Test loading configuration from environment."""
        env = {
            "LAKEFS_ENDPOINT": "http://lakefs.example.com",
            "LAKEFS_ACCESS_KEY": "test-key",
            "LAKEFS_SECRET_KEY": "test-secret",  # pragma: allowlist secret
            "LAKEFS_DEFAULT_REPO": "custom-repo",
        }
        with patch.dict("os.environ", env, clear=True):
            config = LakeFSConfig.from_env()
            assert config.endpoint == "http://lakefs.example.com"
            assert config.access_key == "test-key"
            assert config.default_repo == "custom-repo"


class TestGoldCommit:
    """Test GoldCommit dataclass."""

    def test_commit_fields(self):
        from datetime import UTC, datetime

        commit = GoldCommit(
            commit_id="abc123",
            message="Add momentum features",
            committer="ml-pipeline",
            creation_date=datetime.now(UTC),
            metadata={"run_id": "123"},
            parents=["def456"],
        )
        assert commit.commit_id == "abc123"
        assert commit.message == "Add momentum features"
        assert len(commit.parents) == 1


class TestGoldTag:
    """Test GoldTag dataclass."""

    def test_tag_fields(self):
        tag = GoldTag(name="momentum_features/v1.0.0", commit_id="abc123")
        assert tag.name == "momentum_features/v1.0.0"
        assert tag.commit_id == "abc123"


class TestLakeFSVersionManager:
    """Test LakeFSVersionManager operations (mocked)."""

    @pytest.fixture
    def mock_manager(self):
        """Create manager with mocked lakeFS client."""
        manager = LakeFSVersionManager(
            config=LakeFSConfig(
                endpoint="http://localhost:8000",
                access_key="test",
                secret_key="test",  # pragma: allowlist secret
            )
        )
        return manager

    def test_checkout_returns_lakefs_uri(self, mock_manager):
        """Test checkout returns proper lakeFS URI."""
        uri = mock_manager.checkout("v1.0.0")
        assert uri == "lakefs://heber-gold/v1.0.0/"

    def test_checkout_custom_repo(self, mock_manager):
        """Test checkout with custom repository."""
        uri = mock_manager.checkout("main", repo="custom-repo")
        assert uri == "lakefs://custom-repo/main/"


def run_all_gold_versioning_tests() -> dict[str, bool]:
    """Run all Gold versioning tests.

    Returns:
        Dict mapping test name to pass/fail
    """
    results = {}

    test_classes = [
        TestLakeFSConfig,
        TestGoldCommit,
        TestGoldTag,
        TestLakeFSVersionManager,
    ]

    for test_class in test_classes:
        instance = test_class()
        for method_name in dir(instance):
            if method_name.startswith("test_"):
                try:
                    method = getattr(instance, method_name)
                    # Handle pytest fixtures
                    if "mock_manager" in method.__code__.co_varnames:
                        method(instance.mock_manager())
                    else:
                        method()
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
