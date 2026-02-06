"""Regression tests for configurable lakeFS storage namespaces."""

from __future__ import annotations

import sys
from types import SimpleNamespace

from heber.versioning import LakeFSConfig, LakeFSVersionManager


def test_resolve_storage_namespace_uses_base_by_default() -> None:
    config = LakeFSConfig(storage_namespace_base="s3://bucket-a/lake")

    namespace = config.resolve_storage_namespace("repo-1")

    assert namespace == "s3://bucket-a/lake/repo-1"


def test_resolve_storage_namespace_uses_template_when_provided() -> None:
    config = LakeFSConfig(
        storage_namespace_base="s3://ignored-base",
        storage_namespace_template="s3://bucket-b/env/prod/{repo}",
    )

    namespace = config.resolve_storage_namespace("repo-2")

    assert namespace == "s3://bucket-b/env/prod/repo-2"


def test_get_repo_creation_uses_resolved_storage_namespace(monkeypatch) -> None:
    created: dict[str, str] = {}
    attempts = {"count": 0}

    class _FakeRepo:
        def __init__(self, repo_name: str) -> None:
            self.repo_name = repo_name

        def create(self, storage_namespace: str, default_branch: str) -> _FakeRepo:
            created["repo_name"] = self.repo_name
            created["storage_namespace"] = storage_namespace
            created["default_branch"] = default_branch
            return self

    def _repository_factory(repo_name: str, client=None):  # noqa: ANN001
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("repo missing")
        return _FakeRepo(repo_name)

    fake_lakefs = SimpleNamespace(
        Client=lambda **_kwargs: object(),
        Repository=_repository_factory,
    )
    monkeypatch.setitem(sys.modules, "lakefs", fake_lakefs)

    config = LakeFSConfig(storage_namespace_template="s3://bucket-c/team/{repo}")
    manager = LakeFSVersionManager(config=config)
    manager._get_repo("repo-3")

    assert created["repo_name"] == "repo-3"
    assert created["storage_namespace"] == "s3://bucket-c/team/repo-3"
    assert created["default_branch"] == "main"
