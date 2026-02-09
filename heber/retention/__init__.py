"""Retention and lifecycle management for Heber per PRD §15.

Provides:
- TTL policies per dataset and layer
- Partition cleanup automation (heber-reaper)
- Archive to cold storage
- Retention metadata in Catalog
"""

import asyncio
import json
import re
import shutil
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

import structlog
from prometheus_client import Counter, Gauge, Histogram

logger = structlog.get_logger(__name__)

# Default storage root
DEFAULT_STORAGE_ROOT = "/data/heber"


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
    """Retention policy for a layer per PRD §15.2.

    Attributes:
        retention_days: Days to retain data (None = forever)
        retention_versions: Number of versions to keep (for Gold layer)
        action: Lifecycle action to perform when retention expires
    """

    retention_days: int | None = None
    retention_versions: int | None = None
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
    hot_store: RetentionPolicy = field(default_factory=lambda: RetentionPolicy(7, action=LifecycleAction.DELETE))
    dlq: RetentionPolicy = field(default_factory=lambda: RetentionPolicy(30, action=LifecycleAction.DELETE))
    pinned_versions: list[str] = field(default_factory=list)  # Per PRD §15.6

    def to_json(self) -> str:
        return json.dumps(
            {
                "bronze": self.bronze.to_dict(),
                "silver": self.silver.to_dict(),
                "gold": self.gold.to_dict(),
                "hot_store": self.hot_store.to_dict(),
                "dlq": self.dlq.to_dict(),
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

    def archive_partition(
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
        storage_root: str | Path | None = None,
        safety_checker: DeletionSafetyChecker | None = None,
        archiver: Archiver | None = None,
        dry_run: bool = False,
    ):
        self.storage_root = self._resolve_storage_root(storage_root)
        self.safety_checker = safety_checker or DeletionSafetyChecker()
        self.archiver = archiver or Archiver(archive_root=str(self.storage_root / "archive"))
        self.dry_run = dry_run

    @staticmethod
    def _resolve_storage_root(storage_root: str | Path | None) -> Path:
        """Resolve storage root from explicit arg or shared settings."""
        if storage_root is not None:
            return Path(storage_root)

        try:
            from heber.config import get_settings

            return Path(get_settings().data_root)
        except Exception:
            return Path(DEFAULT_STORAGE_ROOT)

    def scan_partitions(
        self,
        dataset: str,
        layer: DataLayer,
    ) -> list[PartitionInfo]:
        """Scan partitions for a dataset/layer."""
        if layer == DataLayer.GOLD:
            return self._scan_gold_partitions(dataset)

        layer_paths = self._candidate_layer_paths(layer, dataset)
        if not layer_paths:
            return []

        partitions = []
        for layer_path in layer_paths:
            for dt_dir in layer_path.glob("dt=*"):
                try:
                    dt_str = dt_dir.name.replace("dt=", "")
                    partition_date = date.fromisoformat(dt_str)
                    file_count, total_bytes = self._summarize_parquet_tree(dt_dir)

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

    def _candidate_layer_paths(
        self,
        layer: DataLayer,
        dataset: str,
    ) -> list[Path]:
        """Return existing paths for a layer/dataset across legacy and key-value layouts."""
        normalized = dataset.replace("dataset=", "", 1)
        candidates = [
            self.storage_root / layer.value / dataset,
            self.storage_root / layer.value / normalized,
            self.storage_root / layer.value / f"dataset={normalized}",
        ]
        paths: list[Path] = []
        seen: set[str] = set()
        for path in candidates:
            path_str = str(path)
            if path.exists() and path_str not in seen:
                paths.append(path)
                seen.add(path_str)
        return paths

    def _scan_gold_partitions(self, dataset: str) -> list[PartitionInfo]:
        """Scan Gold datasets across project/type/version layouts."""
        dataset_name = dataset.replace("dataset=", "", 1)
        dataset_paths = self._candidate_layer_paths(DataLayer.GOLD, dataset_name)
        if not dataset_paths:
            return []

        partitions: list[PartitionInfo] = []
        seen: set[str] = set()

        for dataset_path in dataset_paths:
            version_dirs = [
                *dataset_path.glob("project=*/version=*"),
                *dataset_path.glob("type=*/version=*"),
                *dataset_path.glob("version=*"),
            ]
            for version_dir in version_dirs:
                if not version_dir.is_dir():
                    continue
                version = self._extract_version(version_dir.name)
                dt_dirs = [dt_dir for dt_dir in version_dir.glob("dt=*") if dt_dir.is_dir()]

                if dt_dirs:
                    for dt_dir in dt_dirs:
                        key = str(dt_dir)
                        if key in seen:
                            continue
                        try:
                            partition_date = date.fromisoformat(dt_dir.name.replace("dt=", ""))
                        except ValueError:
                            continue
                        file_count, total_bytes = self._summarize_parquet_tree(dt_dir)
                        partitions.append(
                            PartitionInfo(
                                path=dt_dir,
                                dataset=dataset_name,
                                layer=DataLayer.GOLD,
                                partition_date=partition_date,
                                version=version,
                                file_count=file_count,
                                total_bytes=total_bytes,
                            )
                        )
                        seen.add(key)
                else:
                    key = str(version_dir)
                    if key in seen:
                        continue
                    file_count, total_bytes = self._summarize_parquet_tree(version_dir)
                    if file_count == 0:
                        continue
                    partition_date = self._infer_partition_date(version_dir)
                    partitions.append(
                        PartitionInfo(
                            path=version_dir,
                            dataset=dataset_name,
                            layer=DataLayer.GOLD,
                            partition_date=partition_date,
                            version=version,
                            file_count=file_count,
                            total_bytes=total_bytes,
                        )
                    )
                    seen.add(key)

        return partitions

    @staticmethod
    def _extract_version(path_name: str) -> str | None:
        if path_name.startswith("version="):
            return path_name.replace("version=", "", 1)
        return None

    @staticmethod
    def _summarize_parquet_tree(path: Path) -> tuple[int, int]:
        file_count = 0
        total_bytes = 0
        for file_path in path.glob("**/*.parquet"):
            if not file_path.is_file():
                continue
            file_count += 1
            total_bytes += file_path.stat().st_size
        return file_count, total_bytes

    @staticmethod
    def _infer_partition_date(path: Path) -> date:
        parquet_files = [f for f in path.glob("**/*.parquet") if f.is_file()]
        if parquet_files:
            latest_mtime = max(f.stat().st_mtime for f in parquet_files)
            return datetime.fromtimestamp(latest_mtime, tz=UTC).date()
        return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).date()

    @staticmethod
    def _version_sort_key(version: str) -> tuple[int, int, int, int, str]:
        clean = version.strip()
        semver_match = re.match(r"^v?(\d+)\.(\d+)\.(\d+)(?:[-+].*)?$", clean)
        if semver_match:
            major, minor, patch = semver_match.groups()
            return (1, int(major), int(minor), int(patch), clean)
        return (0, 0, 0, 0, clean)

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
        sorted_versions = sorted(by_version.keys(), key=self._version_sort_key, reverse=True)

        for v in sorted_versions[policy.retention_versions :]:
            if v not in pinned_versions:
                expired.extend(by_version[v])

        return expired

    def delete_partition(
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

    def apply_policy(
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
            return self.delete_partition(partition)
        elif action == LifecycleAction.ARCHIVE:
            # Archive first, then delete
            archived, _ = self.archiver.archive_partition(partition)
            if archived:
                return self.delete_partition(partition)
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

    def run_once(self) -> ReaperResult:
        """Run a single reaper pass per PRD §15.4 workflow."""
        result = ReaperResult(started_at=datetime.now(UTC))

        try:
            for dataset, config in self.retention_configs.items():
                self._process_dataset(dataset, config, result)

            result.completed_at = datetime.now(UTC)
            self._log_success(result)

        except Exception as e:
            result.completed_at = datetime.now(UTC)
            result.errors.append(str(e))
            reaper_runs.labels(status="error").inc()
            logger.error("reaper_run_failed", error=str(e), exc_info=True)

        return result

    def _process_dataset(
        self,
        dataset: str,
        config: DatasetRetentionConfig,
        result: ReaperResult,
    ) -> None:
        """Process retention for a single dataset across all layers."""
        layer_policies = [
            (DataLayer.BRONZE, config.bronze),
            (DataLayer.SILVER, config.silver),
            (DataLayer.GOLD, config.gold),
            (DataLayer.HOT_STORE, config.hot_store),
            (DataLayer.DLQ, config.dlq),
        ]
        for layer, policy in layer_policies:
            if policy.retention_days is None and policy.retention_versions is None:
                continue

            partitions = self.worker.scan_partitions(dataset, layer)
            result.partitions_scanned += len(partitions)

            expired = self._find_expired(layer, partitions, policy, config.pinned_versions)
            pending_deletions.labels(dataset=dataset, layer=layer.value).set(len(expired))

            self._apply_policies(expired, policy, result)

    def _find_expired(
        self,
        layer: DataLayer,
        partitions: list[PartitionInfo],
        policy: RetentionPolicy,
        pinned_versions: list[str],
    ) -> list[PartitionInfo]:
        """Find expired partitions based on layer and policy."""
        if layer == DataLayer.GOLD and policy.retention_versions:
            return self.worker.find_expired_versions(partitions, policy, pinned_versions)
        return self.worker.find_expired_partitions(partitions, policy)

    def _apply_policies(
        self,
        partitions: list[PartitionInfo],
        policy: RetentionPolicy,
        result: ReaperResult,
    ) -> None:
        """Apply retention policy to expired partitions."""
        for partition in partitions:
            success, reclaimed = self.worker.apply_policy(partition, policy.action)
            if success:
                if policy.action == LifecycleAction.ARCHIVE:
                    result.partitions_archived += 1
                else:
                    result.partitions_deleted += 1
                result.files_deleted += partition.file_count
                result.bytes_reclaimed += reclaimed
            else:
                result.errors.append(f"Failed: {partition.path}")

    def _log_success(self, result: ReaperResult) -> None:
        """Log successful reaper completion."""
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

    async def run_scheduled(self) -> None:
        """Run reaper on schedule."""
        self._running = True

        while self._running:
            self.run_once()
            await asyncio.sleep(self.run_interval_hours * 3600)

    def stop(self) -> None:
        """Stop scheduled runs."""
        self._running = False


# Factory functions


def create_reaper(
    storage_root: str | Path | None = None,
    archive_root: str | Path | None = None,
    dry_run: bool = False,
) -> ReaperScheduler:
    """Create a configured reaper scheduler."""
    resolved_storage_root = ReaperWorker._resolve_storage_root(storage_root)
    resolved_archive_root = Path(archive_root) if archive_root is not None else resolved_storage_root / "archive"
    worker = ReaperWorker(
        storage_root=resolved_storage_root,
        archiver=Archiver(str(resolved_archive_root)),
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
