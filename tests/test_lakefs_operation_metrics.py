"""Regression tests for lakeFS operation metrics coverage."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from heber.versioning import GoldCommit, GoldTag, LakeFSConfig, LakeFSVersionManager


class _CounterHandle:
    def __init__(self, records: list[dict[str, str]], labels: dict[str, str]) -> None:
        self._records = records
        self._labels = labels

    def inc(self) -> None:
        self._records.append(dict(self._labels))


class _CounterMetric:
    def __init__(self) -> None:
        self.records: list[dict[str, str]] = []

    def labels(self, **labels: str) -> _CounterHandle:
        return _CounterHandle(self.records, labels)


class _DurationHandle:
    def __init__(self, records: list[dict[str, float | str]], operation: str) -> None:
        self._records = records
        self._operation = operation

    def observe(self, value: float) -> None:
        self._records.append({"operation": self._operation, "value": value})


class _DurationMetric:
    def __init__(self) -> None:
        self.records: list[dict[str, float | str]] = []

    def labels(self, **labels: str) -> _DurationHandle:
        return _DurationHandle(self.records, labels["operation"])


@pytest.fixture
def metric_spies(monkeypatch):
    counter = _CounterMetric()
    duration = _DurationMetric()

    monkeypatch.setattr("heber.versioning.lakefs_operations", counter)
    monkeypatch.setattr("heber.versioning.lakefs_operation_duration", duration)

    return counter.records, duration.records


def _new_manager() -> LakeFSVersionManager:
    return LakeFSVersionManager(config=LakeFSConfig(default_repo="repo-default"))


def _assert_metrics(
    counter_records: list[dict[str, str]],
    duration_records: list[dict[str, float | str]],
    *,
    operation: str,
    status: str,
    repository: str,
) -> None:
    assert counter_records == [
        {
            "operation": operation,
            "repository": repository,
            "status": status,
        }
    ]

    assert len(duration_records) == 1
    assert duration_records[0]["operation"] == operation
    assert float(duration_records[0]["value"]) >= 0.0


def test_create_tag_records_success_metrics(monkeypatch, metric_spies) -> None:
    counter_records, duration_records = metric_spies
    manager = _new_manager()
    created: dict[str, str] = {}

    class _FakeTag:
        def __init__(self, tag_name: str) -> None:
            self.tag_name = tag_name

        def create(self, commit_id: str) -> None:
            created["tag_name"] = self.tag_name
            created["commit_id"] = commit_id

    class _FakeRepo:
        def tag(self, tag_name: str) -> _FakeTag:
            return _FakeTag(tag_name)

    monkeypatch.setattr(manager, "_get_repo", lambda _repo=None: _FakeRepo())

    tag = manager.create_tag("v1.2.3", "commit-1", repo="repo-a")

    assert tag == GoldTag(name="v1.2.3", commit_id="commit-1")
    assert created == {"tag_name": "v1.2.3", "commit_id": "commit-1"}
    _assert_metrics(
        counter_records,
        duration_records,
        operation="create_tag",
        status="success",
        repository="repo-a",
    )


def test_list_tags_records_success_metrics(monkeypatch, metric_spies) -> None:
    counter_records, duration_records = metric_spies
    manager = _new_manager()

    class _FakeRepo:
        def tags(self):
            return [
                SimpleNamespace(id="v1.0.0", commit_id="commit-a"),
                SimpleNamespace(id="v1.1.0", commit_id="commit-b"),
            ]

    monkeypatch.setattr(manager, "_get_repo", lambda _repo=None: _FakeRepo())

    tags = manager.list_tags(repo="repo-a")

    assert [(tag.name, tag.commit_id) for tag in tags] == [
        ("v1.0.0", "commit-a"),
        ("v1.1.0", "commit-b"),
    ]
    _assert_metrics(
        counter_records,
        duration_records,
        operation="list_tags",
        status="success",
        repository="repo-a",
    )


def test_merge_records_success_metrics(monkeypatch, metric_spies) -> None:
    counter_records, duration_records = metric_spies
    manager = _new_manager()
    merge_calls: list[str] = []

    class _FakeBranch:
        def merge(self, source_branch: str) -> str:
            merge_calls.append(source_branch)
            return "merge-commit-123"

    class _FakeRepo:
        def branch(self, name: str) -> _FakeBranch:
            assert name == "main"
            return _FakeBranch()

    expected_commit = GoldCommit(
        commit_id="merge-commit-123",
        message="merge",
        committer="bot",
        creation_date=datetime.now(UTC),
    )

    monkeypatch.setattr(manager, "_get_repo", lambda _repo=None: _FakeRepo())
    monkeypatch.setattr(manager, "get_commit", lambda _ref, _repo=None: expected_commit)

    commit = manager.merge("feature", repo="repo-a")

    assert commit == expected_commit
    assert merge_calls == ["feature"]
    _assert_metrics(
        counter_records,
        duration_records,
        operation="merge",
        status="success",
        repository="repo-a",
    )


def test_diff_records_success_metrics(monkeypatch, metric_spies) -> None:
    counter_records, duration_records = metric_spies
    manager = _new_manager()

    class _FakeRef:
        def diff(self, other_ref: str):
            assert other_ref == "main"
            return [
                SimpleNamespace(path="gold/foo.parquet", type="added"),
                SimpleNamespace(path="gold/bar.parquet", type="modified"),
            ]

    class _FakeRepo:
        def ref(self, ref_name: str) -> _FakeRef:
            assert ref_name == "feature"
            return _FakeRef()

    monkeypatch.setattr(manager, "_get_repo", lambda _repo=None: _FakeRepo())

    changes = manager.diff("feature", "main", repo="repo-a")

    assert changes == [
        {"path": "gold/foo.parquet", "type": "added"},
        {"path": "gold/bar.parquet", "type": "modified"},
    ]
    _assert_metrics(
        counter_records,
        duration_records,
        operation="diff",
        status="success",
        repository="repo-a",
    )


@pytest.mark.parametrize(
    ("operation", "invoke"),
    [
        ("create_tag", lambda manager: manager.create_tag("v1.0.0", "commit-1", repo="repo-a")),
        ("list_tags", lambda manager: manager.list_tags(repo="repo-a")),
        ("merge", lambda manager: manager.merge("feature", repo="repo-a")),
        ("diff", lambda manager: manager.diff("feature", "main", repo="repo-a")),
    ],
)
def test_operation_metrics_record_error_when_repo_resolution_fails(
    monkeypatch,
    metric_spies,
    operation: str,
    invoke: Callable[[LakeFSVersionManager], object],
) -> None:
    counter_records, duration_records = metric_spies
    manager = _new_manager()

    def _raise_repo_error(_repo=None):
        raise RuntimeError("lakefs unavailable")

    monkeypatch.setattr(manager, "_get_repo", _raise_repo_error)

    with pytest.raises(RuntimeError, match="lakefs unavailable"):
        invoke(manager)

    _assert_metrics(
        counter_records,
        duration_records,
        operation=operation,
        status="error",
        repository="repo-a",
    )
