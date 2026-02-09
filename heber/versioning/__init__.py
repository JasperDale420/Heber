"""lakeFS Client for Data Versioning.

This module provides Git-like versioning for Gold datasets using lakeFS,
replacing custom GoldVersion management with branches, commits, and tags.

Phase 2 of OSS Migration Roadmap.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any

import structlog
from prometheus_client import Counter, Histogram

logger = structlog.get_logger(__name__)

# Metrics
lakefs_operations = Counter(
    "heber_lakefs_operations_total",
    "Total lakeFS operations",
    ["operation", "repository", "status"],
)

lakefs_operation_duration = Histogram(
    "heber_lakefs_operation_duration_seconds",
    "Time for lakeFS operations",
    ["operation"],
    buckets=[0.1, 0.5, 1, 2, 5, 10],
)


@dataclass
class LakeFSConfig:
    """lakeFS connection configuration.

    Environment Variables:
        LAKEFS_ENDPOINT: lakeFS server URL (default: http://localhost:8000)
        LAKEFS_ACCESS_KEY: Access key ID
        LAKEFS_SECRET_KEY: Secret access key
        LAKEFS_DEFAULT_REPO: Default repository name
        LAKEFS_STORAGE_NAMESPACE_BASE: Storage namespace base (default: s3://heber-lakehouse)
        LAKEFS_STORAGE_NAMESPACE_TEMPLATE: Optional template with {repo} placeholder
    """

    endpoint: str = "http://localhost:8000"
    access_key: str = ""
    secret_key: str = ""
    default_repo: str = "heber-gold"
    storage_namespace_base: str = "s3://heber-lakehouse"
    storage_namespace_template: str | None = None

    @classmethod
    def from_env(cls) -> LakeFSConfig:
        """Load configuration from Heber settings."""
        from heber.config import get_settings

        settings = get_settings()
        return cls(
            endpoint=settings.lakefs_endpoint,
            access_key=settings.lakefs_access_key,
            secret_key=settings.lakefs_secret_key,
            default_repo=settings.lakefs_default_repo,
            storage_namespace_base=settings.lakefs_storage_namespace_base,
            storage_namespace_template=settings.lakefs_storage_namespace_template,
        )

    def resolve_storage_namespace(self, repo_name: str) -> str:
        """Resolve storage namespace for repository creation."""
        if self.storage_namespace_template:
            if "{repo}" in self.storage_namespace_template:
                return self.storage_namespace_template.format(repo=repo_name)
            return f"{self.storage_namespace_template.rstrip('/')}/{repo_name}"

        return f"{self.storage_namespace_base.rstrip('/')}/{repo_name}"


@dataclass
class GoldCommit:
    """Represents a Gold dataset commit in lakeFS.

    This replaces the custom GoldVersion/VersionManifest with lakeFS commits.
    """

    commit_id: str
    message: str
    committer: str
    creation_date: datetime
    metadata: dict[str, str] = field(default_factory=dict)
    parents: list[str] = field(default_factory=list)


@dataclass
class GoldTag:
    """Represents a version tag (e.g., v1.0.0) in lakeFS.

    Tags provide semantic versioning on top of commits.
    """

    name: str  # e.g., "v1.0.0"
    commit_id: str


class LakeFSVersionManager:
    """Manages Gold dataset versioning using lakeFS.

    This replaces custom GoldVersion tracking with Git-like operations:
    - Branches for development/experiments
    - Commits for atomic snapshots
    - Tags for semantic versions (v1.0.0, v1.1.0)

    Example:
        manager = LakeFSVersionManager()

        # Create experiment branch
        manager.create_branch("experiment-new-features", from_branch="main")

        # Upload data and commit
        manager.upload_dataframe("features/momentum.parquet", df, branch="experiment-new-features")
        commit = manager.commit("experiment-new-features", "Add momentum features v2")

        # Tag as version
        manager.create_tag("v1.2.0", commit.commit_id)
    """

    def __init__(self, config: LakeFSConfig | None = None) -> None:
        """Initialize the lakeFS version manager.

        Args:
            config: Optional configuration. If None, loads from environment.
        """
        if config is None:
            config = LakeFSConfig.from_env()

        self.config = config
        self._client: Any = None
        self._repo: Any = None

    def _get_client(self) -> Any:
        """Lazily initialize the lakeFS client."""
        if self._client is None:
            import lakefs

            self._client = lakefs.Client(
                host=self.config.endpoint,
                username=self.config.access_key,
                password=self.config.secret_key,
            )
            logger.info(
                "lakefs_client_initialized",
                endpoint=self.config.endpoint,
            )
        return self._client

    def _get_repo(self, repo_name: str | None = None) -> Any:
        """Get or create a lakeFS repository."""
        import lakefs

        client = self._get_client()
        repo_name = repo_name or self.config.default_repo

        try:
            return lakefs.Repository(repo_name, client=client)
        except Exception:
            # Repository doesn't exist, create it
            logger.info("creating_lakefs_repository", repo=repo_name)
            return lakefs.Repository(repo_name, client=client).create(
                storage_namespace=self.config.resolve_storage_namespace(repo_name),
                default_branch="main",
            )

    def list_branches(self, repo: str | None = None) -> list[str]:
        """List all branches in the repository.

        Args:
            repo: Repository name. Uses default if not specified.

        Returns:
            List of branch names
        """
        repository = self._get_repo(repo)
        return [b.id for b in repository.branches()]

    def create_branch(
        self,
        branch_name: str,
        from_branch: str = "main",
        repo: str | None = None,
    ) -> None:
        """Create a new branch from an existing branch.

        Args:
            branch_name: Name for the new branch
            from_branch: Source branch (default: main)
            repo: Repository name
        """
        start_time = datetime.now(UTC)
        repository = self._get_repo(repo)

        try:
            source = repository.branch(from_branch)
            source.branch(branch_name)

            duration = (datetime.now(UTC) - start_time).total_seconds()
            lakefs_operations.labels(
                operation="create_branch",
                repository=repo or self.config.default_repo,
                status="success",
            ).inc()
            lakefs_operation_duration.labels(operation="create_branch").observe(duration)

            logger.info(
                "lakefs_branch_created",
                branch=branch_name,
                from_branch=from_branch,
            )
        except Exception as e:
            lakefs_operations.labels(
                operation="create_branch",
                repository=repo or self.config.default_repo,
                status="error",
            ).inc()
            logger.error("lakefs_branch_create_failed", error=str(e))
            raise

    def commit(
        self,
        branch: str,
        message: str,
        metadata: dict[str, str] | None = None,
        repo: str | None = None,
    ) -> GoldCommit:
        """Create a commit on a branch.

        Args:
            branch: Branch name to commit to
            message: Commit message
            metadata: Optional metadata (e.g., {"author": "ml-pipeline", "run_id": "123"})
            repo: Repository name

        Returns:
            GoldCommit with commit details
        """
        start_time = datetime.now(UTC)
        repository = self._get_repo(repo)
        branch_ref = repository.branch(branch)

        try:
            commit_ref = branch_ref.commit(
                message=message,
                metadata=metadata or {},
            )
            commit = commit_ref.get_commit()

            duration = (datetime.now(UTC) - start_time).total_seconds()
            lakefs_operations.labels(
                operation="commit",
                repository=repo or self.config.default_repo,
                status="success",
            ).inc()
            lakefs_operation_duration.labels(operation="commit").observe(duration)

            logger.info(
                "lakefs_commit_created",
                branch=branch,
                commit_id=commit.id,
                message=message,
            )

            return GoldCommit(
                commit_id=commit.id,
                message=commit.message,
                committer=commit.committer,
                creation_date=datetime.fromtimestamp(commit.creation_date, tz=UTC),
                metadata=dict(commit.metadata) if commit.metadata else {},
                parents=list(commit.parents) if commit.parents else [],
            )
        except Exception as e:
            lakefs_operations.labels(
                operation="commit",
                repository=repo or self.config.default_repo,
                status="error",
            ).inc()
            logger.error("lakefs_commit_failed", error=str(e))
            raise

    def create_tag(
        self,
        tag_name: str,
        commit_id: str,
        repo: str | None = None,
    ) -> GoldTag:
        """Create a semantic version tag pointing to a commit.

        Args:
            tag_name: Tag name (e.g., "v1.0.0")
            commit_id: Commit ID to tag
            repo: Repository name

        Returns:
            GoldTag with tag details
        """
        start_time = datetime.now(UTC)
        repo_name = repo or self.config.default_repo

        try:
            repository = self._get_repo(repo)
            repository.tag(tag_name).create(commit_id)

            duration = (datetime.now(UTC) - start_time).total_seconds()
            lakefs_operations.labels(
                operation="create_tag",
                repository=repo_name,
                status="success",
            ).inc()
            lakefs_operation_duration.labels(operation="create_tag").observe(duration)

            logger.info(
                "lakefs_tag_created",
                tag=tag_name,
                commit_id=commit_id,
            )

            return GoldTag(name=tag_name, commit_id=commit_id)
        except Exception as e:
            duration = (datetime.now(UTC) - start_time).total_seconds()
            lakefs_operations.labels(
                operation="create_tag",
                repository=repo_name,
                status="error",
            ).inc()
            lakefs_operation_duration.labels(operation="create_tag").observe(duration)
            logger.error("lakefs_tag_create_failed", error=str(e))
            raise

    def list_tags(self, repo: str | None = None) -> list[GoldTag]:
        """List all tags in the repository.

        Args:
            repo: Repository name

        Returns:
            List of GoldTag objects
        """
        start_time = datetime.now(UTC)
        repo_name = repo or self.config.default_repo

        try:
            repository = self._get_repo(repo)
            tags = [GoldTag(name=t.id, commit_id=t.commit_id) for t in repository.tags()]

            duration = (datetime.now(UTC) - start_time).total_seconds()
            lakefs_operations.labels(
                operation="list_tags",
                repository=repo_name,
                status="success",
            ).inc()
            lakefs_operation_duration.labels(operation="list_tags").observe(duration)
            return tags
        except Exception:
            duration = (datetime.now(UTC) - start_time).total_seconds()
            lakefs_operations.labels(
                operation="list_tags",
                repository=repo_name,
                status="error",
            ).inc()
            lakefs_operation_duration.labels(operation="list_tags").observe(duration)
            raise

    def get_commit(
        self,
        ref: str,
        repo: str | None = None,
    ) -> GoldCommit:
        """Get commit details by reference (branch, tag, or commit ID).

        Args:
            ref: Reference (branch name, tag name, or commit ID)
            repo: Repository name

        Returns:
            GoldCommit with commit details
        """
        repository = self._get_repo(repo)
        commit = repository.ref(ref).get_commit()

        return GoldCommit(
            commit_id=commit.id,
            message=commit.message,
            committer=commit.committer,
            creation_date=datetime.fromtimestamp(commit.creation_date, tz=UTC),
            metadata=dict(commit.metadata) if commit.metadata else {},
            parents=list(commit.parents) if commit.parents else [],
        )

    def merge(
        self,
        source_branch: str,
        dest_branch: str = "main",
        repo: str | None = None,
    ) -> GoldCommit:
        """Merge a source branch into destination.

        Args:
            source_branch: Branch to merge from
            dest_branch: Branch to merge into (default: main)
            repo: Repository name

        Returns:
            GoldCommit of the merge commit
        """
        start_time = datetime.now(UTC)
        repo_name = repo or self.config.default_repo

        try:
            repository = self._get_repo(repo)
            dest = repository.branch(dest_branch)
            result = dest.merge(source_branch)

            duration = (datetime.now(UTC) - start_time).total_seconds()
            lakefs_operations.labels(
                operation="merge",
                repository=repo_name,
                status="success",
            ).inc()
            lakefs_operation_duration.labels(operation="merge").observe(duration)

            logger.info(
                "lakefs_merge_completed",
                source=source_branch,
                dest=dest_branch,
                commit_id=result,
            )

            return self.get_commit(result, repo)
        except Exception as e:
            duration = (datetime.now(UTC) - start_time).total_seconds()
            lakefs_operations.labels(
                operation="merge",
                repository=repo_name,
                status="error",
            ).inc()
            lakefs_operation_duration.labels(operation="merge").observe(duration)
            logger.error(
                "lakefs_merge_failed",
                source=source_branch,
                dest=dest_branch,
                error=str(e),
            )
            raise

    def diff(
        self,
        ref1: str,
        ref2: str,
        repo: str | None = None,
    ) -> list[dict[str, str]]:
        """Get diff between two references.

        Args:
            ref1: First reference (older)
            ref2: Second reference (newer)
            repo: Repository name

        Returns:
            List of changed paths with their types
        """
        start_time = datetime.now(UTC)
        repo_name = repo or self.config.default_repo
        try:
            repository = self._get_repo(repo)
            changes = repository.ref(ref1).diff(ref2)
            formatted = [{"path": change.path, "type": change.type} for change in changes]

            duration = (datetime.now(UTC) - start_time).total_seconds()
            lakefs_operations.labels(
                operation="diff",
                repository=repo_name,
                status="success",
            ).inc()
            lakefs_operation_duration.labels(operation="diff").observe(duration)
            return formatted
        except Exception:
            duration = (datetime.now(UTC) - start_time).total_seconds()
            lakefs_operations.labels(
                operation="diff",
                repository=repo_name,
                status="error",
            ).inc()
            lakefs_operation_duration.labels(operation="diff").observe(duration)
            raise

    def checkout(
        self,
        ref: str,
        repo: str | None = None,
    ) -> str:
        """Get the storage path for a specific reference.

        Use this to point Iceberg/Parquet readers to versioned data.

        Args:
            ref: Reference (branch, tag, or commit ID)
            repo: Repository name

        Returns:
            Storage path (e.g., "lakefs://heber-gold/v1.0.0/")
        """
        repo_name = repo or self.config.default_repo
        return f"lakefs://{repo_name}/{ref}/"


# Singleton
_manager: LakeFSVersionManager | None = None


@lru_cache(maxsize=1)
def get_version_manager() -> LakeFSVersionManager:
    """Get the singleton version manager instance."""
    global _manager
    if _manager is None:
        _manager = LakeFSVersionManager()
    return _manager
