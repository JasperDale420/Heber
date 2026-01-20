"""Tests for Data Sources and Environments (PRD §52, §55-57)."""

from datetime import datetime, UTC

import pytest

from heber.catalog.datasources import (
    ProviderPriority,
    StorageBoundary,
    DataProvider,
    DataTypeSpec,
    DatasetSpec,
    ProviderRegistry,
    DatasetCatalog,
)
from heber.testing.environments import (
    EnvironmentType,
    EnvironmentConfig,
    StagingConfig,
    DockerComposeService,
    EnvironmentManager,
)


class TestProviderRegistry:
    """Test ProviderRegistry."""
    
    def test_list_all(self):
        registry = ProviderRegistry()
        
        providers = registry.list_all()
        
        assert len(providers) == 7
    
    def test_get_provider(self):
        registry = ProviderRegistry()
        
        alpaca = registry.get("Alpaca")
        
        assert alpaca is not None
        assert alpaca.streaming is True
    
    def test_get_by_capability(self):
        registry = ProviderRegistry()
        
        providers = registry.get_by_capability("bars")
        
        assert len(providers) >= 2
        assert any(p.name == "Alpaca" for p in providers)
    
    def test_get_primary(self):
        registry = ProviderRegistry()
        
        primary = registry.get_primary()
        
        assert len(primary) >= 3  # Alpaca, UW, News API, SEC
        assert all(p.priority == ProviderPriority.PRIMARY for p in primary)


class TestDatasetCatalog:
    """Test DatasetCatalog."""
    
    def test_list_all(self):
        catalog = DatasetCatalog()
        
        datasets = catalog.list_all()
        
        assert len(datasets) >= 25
    
    def test_get_dataset(self):
        catalog = DatasetCatalog()
        
        bars = catalog.get("bars")
        
        assert bars is not None
        assert bars.implemented is True
    
    def test_list_by_category(self):
        catalog = DatasetCatalog()
        
        market = catalog.list_by_category("market_data")
        
        assert len(market) >= 3
    
    def test_list_implemented(self):
        catalog = DatasetCatalog()
        
        implemented = catalog.list_implemented()
        
        assert len(implemented) >= 3  # bars, quotes, trades
    
    def test_list_pending(self):
        catalog = DatasetCatalog()
        
        pending = catalog.list_pending()
        
        assert len(pending) > 0
    
    def test_storage_boundary(self):
        catalog = DatasetCatalog()
        
        heber_storage = catalog.get_storage_boundary("market_data")
        doc_storage = catalog.get_storage_boundary("news_body")
        
        assert heber_storage == StorageBoundary.HEBER
        assert doc_storage == StorageBoundary.DOCUMENT_STORE
    
    def test_generate_report(self):
        catalog = DatasetCatalog()
        
        report = catalog.generate_report()
        
        assert "summary" in report
        assert "by_category" in report
        assert "storage_boundaries" in report


class TestEnvironmentManager:
    """Test EnvironmentManager."""
    
    def test_list_all(self):
        manager = EnvironmentManager()
        
        envs = manager.list_all()
        
        assert len(envs) == 4
    
    def test_get_environment(self):
        manager = EnvironmentManager()
        
        local = manager.get_environment(EnvironmentType.LOCAL)
        
        assert local is not None
        assert local.purpose == "Developer testing"
    
    def test_get_local_services(self):
        manager = EnvironmentManager()
        
        services = manager.get_local_services()
        
        assert len(services) >= 4  # postgres, redis, minio, clickhouse
    
    def test_get_staging_config(self):
        manager = EnvironmentManager()
        
        config = manager.get_staging_config()
        
        assert len(config) >= 5
    
    def test_generate_docker_compose(self):
        manager = EnvironmentManager()
        
        yaml = manager.generate_docker_compose()
        
        assert "version:" in yaml
        assert "services:" in yaml
        assert "postgres:" in yaml
        assert "redis:" in yaml
    
    def test_generate_report(self):
        manager = EnvironmentManager()
        
        report = manager.generate_report()
        
        assert "environments" in report
        assert "staging_config" in report
        assert "local_services" in report


def run_all_datasource_tests() -> dict[str, bool]:
    """Run all data source tests."""
    results = {}
    
    test_classes = [
        TestProviderRegistry,
        TestDatasetCatalog,
        TestEnvironmentManager,
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
    print(f"\nData Source Tests: {passed}/{total} passed")
    
    return results


if __name__ == "__main__":
    run_all_datasource_tests()
