"""Tests for Access Control (PRD §11.9)."""

from datetime import UTC, datetime, timedelta

from heber.catalog.access_control import (
    AccessControlManager,
    AccessLevel,
    DataLayer,
    Project,
    SDKToken,
)


class TestProject:
    """Test Project dataclass."""
    
    def test_to_dict(self):
        project = Project(
            project_id="proj-001",
            name="Alpha Strategy",
            owner="team-a",
        )
        
        d = project.to_dict()
        
        assert d["project_id"] == "proj-001"
        assert d["name"] == "Alpha Strategy"


class TestSDKToken:
    """Test SDKToken dataclass."""
    
    def test_is_valid_active(self):
        token = SDKToken(
            token_id="tok-001",
            project_id="proj-001",
            token_hash="abc123",
            name="Test Token",
        )
        
        assert token.is_valid()
    
    def test_is_valid_revoked(self):
        token = SDKToken(
            token_id="tok-001",
            project_id="proj-001",
            token_hash="abc123",
            name="Test Token",
            revoked=True,
        )
        
        assert not token.is_valid()
    
    def test_is_valid_expired(self):
        token = SDKToken(
            token_id="tok-001",
            project_id="proj-001",
            token_hash="abc123",
            name="Test Token",
            expires_at=datetime.now(UTC) - timedelta(days=1),
        )
        
        assert not token.is_valid()
    
    def test_has_scope(self):
        token = SDKToken(
            token_id="tok-001",
            project_id="proj-001",
            token_hash="abc123",
            name="Test Token",
            scopes=["read", "write"],
        )
        
        assert token.has_scope("read")
        assert token.has_scope("write")
        assert not token.has_scope("admin")
    
    def test_has_wildcard_scope(self):
        token = SDKToken(
            token_id="tok-001",
            project_id="proj-001",
            token_hash="abc123",
            name="Test Token",
            scopes=["*"],
        )
        
        assert token.has_scope("anything")


class TestAccessControlManager:
    """Test AccessControlManager."""
    
    def test_create_project(self):
        manager = AccessControlManager()
        
        project = manager.create_project("proj-001", "Alpha Strategy", "team-a")
        
        assert project.project_id == "proj-001"
        assert manager.get_project("proj-001") is not None
    
    def test_silver_shared_by_default(self):
        manager = AccessControlManager()
        manager.create_project("proj-001", "Test")
        
        # Silver is shared - read access for all projects
        has_access = manager.check_access(
            "proj-001", "bars", DataLayer.SILVER, AccessLevel.READ
        )
        
        assert has_access
    
    def test_gold_requires_permission(self):
        manager = AccessControlManager()
        manager.create_project("proj-001", "Test")
        
        # Gold is NOT shared - requires explicit permission
        has_access = manager.check_access(
            "proj-001", "features", DataLayer.GOLD, AccessLevel.READ
        )
        
        assert not has_access
    
    def test_grant_gold_permission(self):
        manager = AccessControlManager()
        manager.create_project("proj-001", "Test")
        
        # Grant Gold read access
        manager.grant_permission(
            "proj-001", "features", DataLayer.GOLD, AccessLevel.READ
        )
        
        has_access = manager.check_access(
            "proj-001", "features", DataLayer.GOLD, AccessLevel.READ
        )
        
        assert has_access
    
    def test_wildcard_permission(self):
        manager = AccessControlManager()
        manager.create_project("proj-001", "Test")
        
        # Grant wildcard Gold access
        manager.grant_permission("proj-001", "*", DataLayer.GOLD, AccessLevel.READ)
        
        # Should have access to any Gold dataset
        assert manager.check_access("proj-001", "any_dataset", DataLayer.GOLD, AccessLevel.READ)
    
    def test_create_and_validate_token(self):
        manager = AccessControlManager()
        manager.create_project("proj-001", "Test")
        
        raw_token, token = manager.create_token("proj-001", "API Token")
        
        validated = manager.validate_token(raw_token)
        
        assert validated is not None
        assert validated.token_id == token.token_id
    
    def test_validate_invalid_token(self):
        manager = AccessControlManager()
        
        validated = manager.validate_token("invalid-token")
        
        assert validated is None
    
    def test_revoke_token(self):
        manager = AccessControlManager()
        manager.create_project("proj-001", "Test")
        
        raw_token, token = manager.create_token("proj-001", "API Token")
        
        manager.revoke_token(token.token_id)
        
        validated = manager.validate_token(raw_token)
        
        assert validated is None
    
    def test_token_with_expiry(self):
        manager = AccessControlManager()
        manager.create_project("proj-001", "Test")
        
        raw_token, token = manager.create_token(
            "proj-001", "Short-lived Token", expires_in_days=30
        )
        
        assert token.expires_at is not None
        assert token.is_valid()
    
    def test_list_project_permissions(self):
        manager = AccessControlManager()
        manager.create_project("proj-001", "Test")
        manager.grant_permission("proj-001", "features", DataLayer.GOLD, AccessLevel.READ)
        manager.grant_permission("proj-001", "labels", DataLayer.GOLD, AccessLevel.WRITE)
        
        permissions = manager.list_project_permissions("proj-001")
        
        assert len(permissions) == 2
    
    def test_generate_report(self):
        manager = AccessControlManager()
        manager.create_project("proj-001", "Alpha")
        manager.create_project("proj-002", "Beta")
        manager.create_token("proj-001", "Token 1")
        
        report = manager.generate_report()
        
        assert report["summary"]["total_projects"] == 2
        assert report["summary"]["total_tokens"] == 1


def run_all_access_control_tests() -> dict[str, bool]:
    """Run all access control tests."""
    results = {}
    
    test_classes = [
        TestProject,
        TestSDKToken,
        TestAccessControlManager,
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
    print(f"\nAccess Control Tests: {passed}/{total} passed")
    
    return results


if __name__ == "__main__":
    run_all_access_control_tests()
