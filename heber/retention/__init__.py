"""Retention and lifecycle management for Heber per PRD §15.

Provides:
- TTL policies per dataset and layer
- Partition cleanup automation (heber-reaper)
- Archive to cold storage
- Retention metadata in Catalog
"""

import asyncio
import json
import shutil
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

import structlog
from prometheus_client import Counter, Gauge, Histogram

logger = structlog.get_logger(__name__)


# Prometheus metrics
partitions_deleted = Counter(
    "heber_retention_partitions_deleted_total",
    "Partitions deleted by retention policy",
    ["dataset", "layer"],
)

files_deleted = Counter(
    "heber_retention_files_deleted_total",
    "Files deleted by retention policy",
    ["dataset", "layer"],
)

bytes_reclaimed = Counter(
    "heber_retention_bytes_reclaimed_total",
    "Bytes reclaimed by retention policy",
    ["dataset", "layer"],
)

partitions_archived = Counter(
    "heber_retention_partitions_archived_total",
    "Partitions archived to cold storage",
    ["dataset", "layer"],
)

reaper_runs = Counter(
    "heber_reaper_runs_total",
    "Reaper runs",
    ["status"],
)

reaper_duration_seconds = Histogram(
    "heber_reaper_duration_seconds",
    "Duration of reaper runs",
    buckets=[60, 300, 600, 1800, 3600],
)

pending_deletions = Gauge(
    "heber_retention_pending_deletions",
    "Partitions pending deletion",
    ["dataset", "layer"],
)


class DataLayer(str, Enum):
    """Data layers per PRD §15.1."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    HOT_STORE = "hot_store"
    DLQ = "dlq"


class LifecycleAction(str, Enum):
    """Lifecycle actions per PRD §15.3."""

    DELETE = "delete"  # Permanently remove files
    ARCHIVE = "archive"  # Move to cold storage
    COMPRESS = "compress"  # Re-encode with higher compression


# Default retention per PRD §15.1
DEFAULT_RETENTION = {
    DataLayer.BRONZE: {"retention_days": 90, "action": LifecycleAction.DELETE},
    DataLayer.SILVER: {"retention_days": None, "action": LifecycleAction.ARCHIVE},
    DataLayer.GOLD: {
        "retention_versions": 5,
        "retention_days": 365,
        "action": LifecycleAction.DELETE,
    },
    DataLayer.HOT_STORE: {"retention_days": 7, "action": LifecycleAction.DELETE},
    DataLayer.DLQ: {"retention_days": 30, "action": LifecycleAction.DELETE},
}


@dataclass
class RetentionPolicy:
    """Retention policy for a layer per PRD §15.2."""

    retention_days: int | None = None  # None = forever
    retention_versions: int | None = None  # For Gold layer
    action: LifecycleAction = LifecycleAction.DELETE

    def to_dict(self) -> dict[str, Any]:
        return {
            "retention_days": self.retention_days,
            "retention_versions": self.retention_versions,
            "action": self.action.value,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RetentionPolicy":
        return cls(
            retention_days=data.get("retention_days"),
            retention_versions=data.get("retention_versions"),
            action=LifecycleAction(data.get("action", "delete")),
        )


@dataclass
class DatasetRetentionConfig:
    """Complete retention config for a dataset per PRD §15.2."""

    dataset: str
    bronze: RetentionPolicy = field(default_factory=lambda: RetentionPolicy(90, action=LifecycleAction.DELETE))
    silver: RetentionPolicy = field(default_factory=lambda: RetentionPolicy(None, action=LifecycleAction.ARCHIVE))
    gold: RetentionPolicy = field(default_factory=lambda: RetentionPolicy(365, 5, LifecycleAction.DELETE))
    pinned_versions: list[str] = field(default_factory=list)  # Per PRD §15.6

    def to_json(self) -> str:
        return json.dumps(
            {
                "bronze": self.bronze.to_dict(),
                "silver": self.silver.to_dict(),
                "gold": self.gold.to_dict(),
                "pinned_versions": self.pinned_versions,
            },
            indent=2,
        )


@dataclass
class PartitionInfo:
    """Information about a partition for retention analysis."""

    path: Path
    dataset: str
    layer: DataLayer
    partition_date: date
    version: str | None = None  # For Gold layer
    file_count: int = 0
    total_bytes: int = 0
    is_pinned: bool = False


@dataclass
class ReaperResult:
    """Result of a reaper run."""

    started_at: datetime
    completed_at: datetime | None = None
    partitions_scanned: int = 0
    partitions_deleted: int = 0
    partitions_archived: int = 0
    files_deleted: int = 0
    bytes_reclaimed: int = 0
    errors: list[str] = field(default_factory=list)


class DeletionSafetyChecker:
    """Safety gates before deletion per PRD §15.5."""

    def __init__(self, gold_lineage: dict[str, set[str]] | None = None):
        self.gold_lineage = gold_lineage or {}

    def check_safe_to_delete(
        self,
        partition: PartitionInfo,
        dry_run: bool = False,
    ) -> tuple[bool, str | None]:
        """Check if partition is safe to delete per PRD §15.5."""
        # Check if pinned
        if partition.is_pinned:
            return False, f"Partition is pinned: {partition.path}"

        # Check Gold lineage for Silver partitions
        if partition.layer == DataLayer.SILVER:
            dependent_gold = self._find_gold_dependencies(partition)
            if dependent_gold:
                return False, f"Silver partition has Gold dependencies: {dependent_gold}"

        if dry_run:
            logger.info(
                "dry_run_would_delete",
                path=str(partition.path),
                layer=partition.layer.value,
                bytes=partition.total_bytes,
            )

        return True, None

    def _find_gold_dependencies(self, partition: PartitionInfo) -> list[str]:
        """Find Gold datasets that depend on this partition."""
        partition_key = f"{partition.dataset}/{partition.partition_date.isoformat()}"
        return [gold_ds for gold_ds, sources in self.gold_lineage.items() if partition_key in sources]


class Archiver:
    """Archives data to cold storage per PRD §15.3."""

    def __init__(
        self,
        archive_root: str = "/data/heber/archive",
        compress_on_archive: bool = True,
    ):
        self.archive_root = Path(archive_root)
        self.compress_on_archive = compress_on_archive

    async def archive_partition(
        self,
        partition: PartitionInfo,
    ) -> tuple[bool, int]:
        """Archive a partition to cold storage.

        Returns (success, bytes_archived).
        """
        archive_path = self.archive_root / partition.layer.value / partition.dataset
        archive_path.mkdir(parents=True, exist_ok=True)

        # Generate archive filename
        archive_name = f"{partition.partition_date.isoformat()}"
        if partition.version:
            archive_name += f"_v{partition.version}"

        try:
            if self.compress_on_archive:
                # Create compressed archive
                archive_path / f"{archive_name}.tar.gz"
                shutil.make_archive(
                    str(archive_path / archive_name),
                    "gztar",
                    str(partition.path.parent),
                    partition.path.name,
                )
            else:
                # Just move files
                dest = archive_path / archive_name
                shutil.copytree(partition.path, dest)

            partitions_archived.labels(
                dataset=partition.dataset,
                layer=partition.layer.value,
            ).inc()

            logger.info(
                "partition_archived",
                source=str(partition.path),
                destination=str(archive_path),
                bytes=partition.total_bytes,
            )

            return True, partition.total_bytes

        except Exception as e:
            logger.error(
                "archive_failed",
                path=str(partition.path),
                error=str(e),
                exc_info=True,
            )
            return False, 0


class ReaperWorker:
    """Executes retention policy per PRD §15.4."""

    def __init__(
        self,
        storage_root: str = "/data/heber",
        safety_checker: DeletionSafetyChecker | None = None,
        archiver: Archiver | None = None,
        dry_run: bool = False,
    ):
        self.storage_root = Path(storage_root)
        self.safety_checker = safety_checker or DeletionSafetyChecker()
        self.archiver = archiver or Archiver()
        self.dry_run = dry_run

    def scan_partitions(
        self,
        dataset: str,
        layer: DataLayer,
    ) -> list[PartitionInfo]:
        """Scan partitions for a dataset/layer."""
        layer_path = self.storage_root / layer.value / dataset

        if not layer_path.exists():
            return []

        partitions = []
        for dt_dir in layer_path.glob("dt=*"):
            try:
                dt_str = dt_dir.name.replace("dt=", "")
                partition_date = date.fromisoformat(dt_str)

                # Count files and bytes
                file_count = 0
                total_bytes = 0
                for f in dt_dir.glob("**/*.parquet"):
                    file_count += 1
                    total_bytes += f.stat().st_size

                partitions.append(
                    PartitionInfo(
                        path=dt_dir,
                        dataset=dataset,
                        layer=layer,
                        partition_date=partition_date,
                        file_count=file_count,
                        total_bytes=total_bytes,
                    )
                )
            except ValueError:
                continue

        return partitions

    def find_expired_partitions(
        self,
        partitions: list[PartitionInfo],
        policy: RetentionPolicy,
        reference_date: date | None = None,
    ) -> list[PartitionInfo]:
        """Find partitions that exceed retention policy."""
        if reference_date is None:
            reference_date = date.today()

        expired = []

        if policy.retention_days is not None:
            cutoff = reference_date - timedelta(days=policy.retention_days)
            for p in partitions:
                if p.partition_date < cutoff:
                    expired.append(p)

        return expired

    def find_expired_versions(
        self,
        partitions: list[PartitionInfo],
        policy: RetentionPolicy,
        pinned_versions: list[str],
    ) -> list[PartitionInfo]:
        """Find Gold versions that exceed retention per PRD §15.6."""
        if policy.retention_versions is None:
            return []

        # Group by version
        by_version: dict[str, list[PartitionInfo]] = {}
        for p in partitions:
            v = p.version or "default"
            if v not in by_version:
                by_version[v] = []
            by_version[v].append(p)

        # Find versions to delete
        expired = []
        sorted_versions = sorted(by_version.keys(), reverse=True)

        for v in sorted_versions[policy.retention_versions :]:
            if v not in pinned_versions:
                expired.extend(by_version[v])

        return expired

    async def delete_partition(
        self,
        partition: PartitionInfo,
    ) -> tuple[bool, int]:
        """Delete a partition."""
        if self.dry_run:
            logger.info(
                "dry_run_skip_delete",
                path=str(partition.path),
            )
            return True, partition.total_bytes

        try:
            shutil.rmtree(partition.path)

            partitions_deleted.labels(
                dataset=partition.dataset,
                layer=partition.layer.value,
            ).inc()

            files_deleted.labels(
                dataset=partition.dataset,
                layer=partition.layer.value,
            ).inc(partition.file_count)

            bytes_reclaimed.labels(
                dataset=partition.dataset,
                layer=partition.layer.value,
            ).inc(partition.total_bytes)

            logger.info(
                "partition_deleted",
                path=str(partition.path),
                files=partition.file_count,
                bytes=partition.total_bytes,
            )

            return True, partition.total_bytes

        except Exception as e:
            logger.error(
                "delete_failed",
                path=str(partition.path),
                error=str(e),
                exc_info=True,
            )
            return False, 0

    async def apply_policy(
        self,
        partition: PartitionInfo,
        action: LifecycleAction,
    ) -> tuple[bool, int]:
        """Apply lifecycle action to partition."""
        # Safety check first
        safe, reason = self.safety_checker.check_safe_to_delete(partition, dry_run=self.dry_run)
        if not safe:
            logger.warning(
                "partition_not_safe_to_delete",
                path=str(partition.path),
                reason=reason,
            )
            return False, 0

        if action == LifecycleAction.DELETE:
            return await self.delete_partition(partition)
        elif action == LifecycleAction.ARCHIVE:
            # Archive first, then delete
            archived, bytes_archived = await self.archiver.archive_partition(partition)
            if archived:
                return await self.delete_partition(partition)
            return False, 0
        elif action == LifecycleAction.COMPRESS:
            # Recompress in place (not yet implemented)
            logger.warning(
                "compress_action_not_implemented",
                path=str(partition.path),
            )
            return False, 0

        return False, 0


class ReaperScheduler:
    """Schedules and runs retention enforcement per PRD §15.4."""

    def __init__(
        self,
        worker: ReaperWorker,
        retention_configs: dict[str, DatasetRetentionConfig] | None = None,
        run_interval_hours: int = 24,
    ):
        self.worker = worker
        self.retention_configs = retention_configs or {}
        self.run_interval_hours = run_interval_hours
        self._running = False

    def add_dataset_config(
        self,
        config: DatasetRetentionConfig,
    ) -> None:
        """Add retention config for a dataset."""
        self.retention_configs[config.dataset] = config

    async def run_once(self) -> ReaperResult:
        """Run a single reaper pass per PRD §15.4 workflow."""
        result = ReaperResult(started_at=datetime.now(UTC))

        try:
            for dataset, config in self.retention_configs.items():
                # Process each layer
                for layer, policy in [
                    (DataLayer.BRONZE, config.bronze),
                    (DataLayer.SILVER, config.silver),
                    (DataLayer.GOLD, config.gold),
                ]:
                    if policy.retention_days is None and policy.retention_versions is None:
                        continue  # No retention policy

                    partitions = self.worker.scan_partitions(dataset, layer)
                    result.partitions_scanned += len(partitions)

                    # Find expired
                    if layer == DataLayer.GOLD and policy.retention_versions:
                        expired = self.worker.find_expired_versions(partitions, policy, config.pinned_versions)
                    else:
                        expired = self.worker.find_expired_partitions(partitions, policy)

                    pending_deletions.labels(
                        dataset=dataset,
                        layer=layer.value,
                    ).set(len(expired))

                    # Apply policy
                    for partition in expired:
                        success, reclaimed = await self.worker.apply_policy(partition, policy.action)

                        if success:
                            if policy.action == LifecycleAction.ARCHIVE:
                                result.partitions_archived += 1
                            else:
                                result.partitions_deleted += 1
                            result.files_deleted += partition.file_count
                            result.bytes_reclaimed += reclaimed
                        else:
                            result.errors.append(f"Failed: {partition.path}")

            result.completed_at = datetime.now(UTC)
            reaper_runs.labels(status="success").inc()

            duration = (result.completed_at - result.started_at).total_seconds()
            reaper_duration_seconds.observe(duration)

            logger.info(
                "reaper_run_complete",
                partitions_scanned=result.partitions_scanned,
                partitions_deleted=result.partitions_deleted,
                bytes_reclaimed=result.bytes_reclaimed,
                duration_seconds=duration,
            )

        except Exception as e:
            result.completed_at = datetime.now(UTC)
            result.errors.append(str(e))
            reaper_runs.labels(status="error").inc()
            logger.error("reaper_run_failed", error=str(e), exc_info=True)

        return result

    async def run_scheduled(self) -> None:
        """Run reaper on schedule."""
        self._running = True

        while self._running:
            await self.run_once()
            await asyncio.sleep(self.run_interval_hours * 3600)

    def stop(self) -> None:
        """Stop scheduled runs."""
        self._running = False


# Factory functions


def create_reaper(
    storage_root: str = "/data/heber",
    archive_root: str = "/data/heber/archive",
    dry_run: bool = False,
) -> ReaperScheduler:
    """Create a configured reaper scheduler."""
    worker = ReaperWorker(
        storage_root=storage_root,
        archiver=Archiver(archive_root),
        dry_run=dry_run,
    )
    return ReaperScheduler(worker)


def get_default_retention(layer: DataLayer) -> RetentionPolicy:
    """Get default retention policy for a layer."""
    config = DEFAULT_RETENTION.get(layer, {})
    return RetentionPolicy(
        retention_days=config.get("retention_days"),
        retention_versions=config.get("retention_versions"),
        action=config.get("action", LifecycleAction.DELETE),
    )
