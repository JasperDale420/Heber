"""HeberClient - Main SDK client for accessing Heber Data Lakehouse.

Provides safe, point-in-time correct access to Silver and Gold datasets.
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import httpx
import pandas as pd
import pyarrow.parquet as pq
import structlog

from heber.config import settings
from heber.gold.versioning import (
    GoldVersion,
    VersionManifest,
    CompatibilityResult,
    resolve_version,
    check_compatibility,
    list_available_versions,
    get_manifest_path,
)

logger = structlog.get_logger(__name__)


class HeberClient:
    """Client for reading and writing Heber datasets.
    
    Example:
        client = HeberClient()
        
        # Read Silver data (point-in-time correct)
        bars = client.read_asof(
            dataset="bars",
            asof_time=datetime(2025, 1, 15),
            instrument_keys=["equity:AAPL"],
        )
        
        # Write Gold features
        client.write_gold(
            dataset="momentum_features",
            df=features,
            project="kairos",
            version="v1",
        )
    """

    def __init__(
        self,
        catalog_url: str | None = None,
        data_root: Path | None = None,
        api_key: str | None = None,
    ):
        """Initialize HeberClient.
        
        Args:
            catalog_url: URL of the Catalog API. Defaults to settings.
            data_root: Root path for data. Defaults to settings.
            api_key: API key for authentication.
        """
        self.catalog_url = catalog_url or f"http://localhost:{settings.api_port}/api/v1"
        self.data_root = data_root or settings.data_root
        self.api_key = api_key
        self._http_client: httpx.Client | None = None

    @property
    def http_client(self) -> httpx.Client:
        """Lazy HTTP client initialization."""
        if self._http_client is None:
            headers = {}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            self._http_client = httpx.Client(
                base_url=self.catalog_url,
                headers=headers,
                timeout=30.0,
            )
        return self._http_client

    def close(self):
        """Close the HTTP client."""
        if self._http_client:
            self._http_client.close()
            self._http_client = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # Catalog operations
    def list_datasets(self, layer: str | None = None) -> list[dict]:
        """List all datasets, optionally filtered by layer."""
        params = {}
        if layer:
            params["layer"] = layer
        response = self.http_client.get("/datasets", params=params)
        response.raise_for_status()
        return response.json()["data"]

    def get_dataset(self, name: str) -> dict:
        """Get dataset metadata by name."""
        response = self.http_client.get(f"/datasets/{name}")
        response.raise_for_status()
        return response.json()["data"]

    def resolve_instrument(self, symbol: str) -> str | None:
        """Resolve symbol to canonical instrument_key."""
        response = self.http_client.post("/instruments/lookup", json={"symbols": [symbol]})
        response.raise_for_status()
        data = response.json()["data"]
        if data:
            return data[0]["instrument_key"]
        return None

    def discover(
        self,
        dataset_name: str,
        layer: str = "silver",
        schema_version: str = "latest",
    ) -> dict:
        """Discover dataset paths, schema, and partitions (PRD §11.6).
        
        Args:
            dataset_name: Name of dataset (e.g., "bars", "quotes")
            layer: Storage layer (bronze, silver, gold)
            schema_version: Schema version or "latest"
            
        Returns:
            dict with keys:
              - paths: list of partition paths
              - schema: schema definition
              - partitions: list of partition values
        """
        # Get dataset metadata
        dataset = self.get_dataset(dataset_name)
        
        # Get schema version
        response = self.http_client.get(f"/datasets/{dataset_name}/versions")
        response.raise_for_status()
        versions = response.json()["data"]
        
        if schema_version == "latest":
            schema = next((v for v in versions if v.get("is_current")), versions[0] if versions else None)
        else:
            schema = next((v for v in versions if v.get("schema_version") == schema_version), None)
        
        # Build base path
        base_path = self.data_root / layer / f"feed={dataset_name}"
        
        # Discover partitions
        partitions = []
        if base_path.exists():
            for partition_dir in base_path.glob("**/dt=*"):
                partition_parts = {}
                for part in str(partition_dir).split("/"):
                    if "=" in part:
                        key, value = part.split("=", 1)
                        partition_parts[key] = value
                partitions.append(partition_parts)
        
        return {
            "dataset": dataset,
            "layer": layer,
            "paths": [str(base_path)],
            "schema": schema,
            "partitions": partitions,
        }

    def asof_join(
        self,
        left: pd.DataFrame,
        right: pd.DataFrame,
        on_keys: list[str] = None,
        left_time: str = "ts_event",
        right_time: str = "ts_event",
        right_available: str = "ts_available",
        tolerance: str | None = None,
        suffix: str = "_right",
    ) -> pd.DataFrame:
        """Point-in-time correct as-of join (PRD §10.4, §11.6).
        
        Joins left to the most recent prior row from right where:
        - ts_event_right <= left_time
        - ts_available_right <= left_time
        
        Args:
            left: Left DataFrame (driving table)
            right: Right DataFrame (lookup table)
            on_keys: Join key columns (e.g., ["instrument_key"])
            left_time: Time column in left for join
            right_time: Time column in right for join
            right_available: Availability column in right
            tolerance: Max time difference (e.g., "1h", "30m")
            suffix: Suffix for right columns
            
        Returns:
            Joined DataFrame with anti-leakage guarantee
        """
        if on_keys is None:
            on_keys = ["instrument_key"]
        
        # Create a safe join time for right table
        # This is the max of ts_event and ts_available
        right = right.copy()
        right["_safe_time"] = right[[right_time, right_available]].max(axis=1)
        
        # Sort both tables
        left = left.sort_values(left_time)
        right = right.sort_values("_safe_time")
        
        # Perform pandas merge_asof
        result = pd.merge_asof(
            left,
            right,
            left_on=left_time,
            right_on="_safe_time",
            by=on_keys,
            tolerance=pd.Timedelta(tolerance) if tolerance else None,
            direction="backward",
            suffixes=("", suffix),
        )
        
        # Drop helper column
        if "_safe_time" + suffix in result.columns:
            result = result.drop(columns=["_safe_time" + suffix])
        elif "_safe_time" in result.columns:
            result = result.drop(columns=["_safe_time"])
        
        logger.debug(
            "asof_join complete",
            left_rows=len(left),
            right_rows=len(right),
            result_rows=len(result),
        )
        
        return result

    # Silver layer reads
    def read_silver(
        self,
        dataset: str,
        time_range: tuple[datetime | str, datetime | str] | None = None,
        instrument_keys: list[str] | None = None,
        instrument_type: str | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Read from Silver layer.
        
        Args:
            dataset: Dataset name (e.g., "bars", "quotes", "trades")
            time_range: (start, end) datetime range
            instrument_keys: Filter to specific instruments
            instrument_type: Filter by instrument type
            columns: Columns to read (None for all)
            
        Returns:
            DataFrame with Silver data
        """
        silver_path = self.data_root / "silver" / f"feed={dataset}"
        
        if not silver_path.exists():
            return pd.DataFrame()

        # Build filters
        filters = []
        if instrument_keys:
            filters.append(("instrument_key", "in", instrument_keys))
        if instrument_type:
            filters.append(("instrument_type", "==", instrument_type))

        try:
            table = pq.read_table(
                silver_path,
                columns=columns,
                filters=filters if filters else None,
            )
            df = table.to_pandas()

            # Apply time range filter
            if time_range and not df.empty:
                start, end = time_range
                if isinstance(start, str):
                    start = pd.Timestamp(start)
                if isinstance(end, str):
                    end = pd.Timestamp(end)
                df = df[(df["ts_event"] >= start) & (df["ts_event"] <= end)]

            return df

        except Exception as e:
            logger.error("Failed to read Silver", dataset=dataset, error=str(e))
            raise

    def read_asof(
        self,
        dataset: str,
        asof_time: datetime | str,
        instrument_keys: list[str] | None = None,
        time_range: tuple[datetime | str, datetime | str] | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Read Silver data with point-in-time correctness.
        
        Only returns rows where ts_available <= asof_time.
        This is the primary read method for training and backtesting.
        
        Args:
            dataset: Dataset name
            asof_time: Point-in-time cutoff (only data available by this time)
            instrument_keys: Filter to specific instruments
            time_range: (start, end) for ts_event range
            columns: Columns to read
            
        Returns:
            DataFrame with point-in-time correct data
        """
        if isinstance(asof_time, str):
            asof_time = pd.Timestamp(asof_time)

        # Read base data
        df = self.read_silver(
            dataset=dataset,
            time_range=time_range,
            instrument_keys=instrument_keys,
            columns=columns,
        )

        if df.empty:
            return df

        # Apply point-in-time filter (the critical anti-leakage gate)
        df = df[df["ts_available"] <= asof_time]

        logger.debug(
            "read_asof complete",
            dataset=dataset,
            asof_time=asof_time,
            rows=len(df),
        )

        return df

    # Gold layer operations
    def read_gold(
        self,
        dataset: str,
        project: str | None = None,
        version: str | None = None,
        time_range: tuple[datetime | str, datetime | str] | None = None,
        instrument_keys: list[str] | None = None,
        asof_time: datetime | str | None = None,
    ) -> pd.DataFrame:
        """Read from Gold layer (features/labels).
        
        Args:
            dataset: Dataset name (e.g., "momentum_features")
            project: Filter by project
            version: Specific version (None for latest)
            time_range: (start, end) datetime range
            instrument_keys: Filter to specific instruments
            asof_time: Point-in-time cutoff (optional)
            
        Returns:
            DataFrame with Gold data
        """
        gold_path = self.data_root / "gold" / f"dataset={dataset}"
        
        if project:
            gold_path = gold_path / f"project={project}"
        if version:
            gold_path = gold_path / f"version={version}"

        if not gold_path.exists():
            return pd.DataFrame()

        # Build filters
        filters = []
        if instrument_keys:
            filters.append(("instrument_key", "in", instrument_keys))

        try:
            table = pq.read_table(
                gold_path,
                filters=filters if filters else None,
            )
            df = table.to_pandas()

            # Apply time range filter
            if time_range and not df.empty:
                start, end = time_range
                if isinstance(start, str):
                    start = pd.Timestamp(start)
                if isinstance(end, str):
                    end = pd.Timestamp(end)
                time_col = "ts_event" if "ts_event" in df.columns else "ts_label"
                df = df[(df[time_col] >= start) & (df[time_col] <= end)]

            # Apply point-in-time filter
            if asof_time and "ts_available" in df.columns:
                if isinstance(asof_time, str):
                    asof_time = pd.Timestamp(asof_time)
                df = df[df["ts_available"] <= asof_time]

            return df

        except Exception as e:
            logger.error("Failed to read Gold", dataset=dataset, error=str(e))
            raise

    def write_gold(
        self,
        dataset: str,
        df: pd.DataFrame,
        project: str,
        version: str,
        metadata: dict[str, Any] | None = None,
    ) -> Path:
        """Write features/labels to Gold layer.
        
        Args:
            dataset: Dataset name
            df: DataFrame to write (must include instrument_key, ts_event, ts_available)
            project: Project name
            version: Version string
            metadata: Additional metadata to log
            
        Returns:
            Path to written file
        """
        # Validate required columns
        required_cols = {"instrument_key", "ts_event", "ts_available"}
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        # Validate point-in-time correctness
        if (df["ts_available"] < df["ts_event"]).any():
            raise ValueError("ts_available cannot be before ts_event (leakage detected)")

        # Determine partition (by date)
        df = df.copy()
        df["dt"] = pd.to_datetime(df["ts_event"]).dt.date

        # Write partitioned by date
        total_rows = 0
        output_paths = []

        for dt, group_df in df.groupby("dt"):
            partition_path = (
                self.data_root
                / "gold"
                / f"dataset={dataset}"
                / f"project={project}"
                / f"version={version}"
                / f"dt={dt}"
            )
            partition_path.mkdir(parents=True, exist_ok=True)

            ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            file_path = partition_path / f"part-{ts}.parquet"

            # Drop temporary dt column
            write_df = group_df.drop(columns=["dt"])
            write_df.to_parquet(file_path, compression="snappy")

            total_rows += len(write_df)
            output_paths.append(file_path)

        logger.info(
            "Wrote Gold dataset",
            dataset=dataset,
            project=project,
            version=version,
            rows=total_rows,
            files=len(output_paths),
        )

        return output_paths[0] if output_paths else None

    def list_gold_versions(self, dataset: str) -> list[str]:
        """List all available versions for a Gold dataset (PRD §28.2).
        
        Args:
            dataset: Dataset name
            
        Returns:
            List of version strings, newest first (e.g., ["v3.5.0", "v3.2.1", "v1.0.0"])
        """
        gold_root = self.data_root / "gold"
        versions = list_available_versions(gold_root, dataset)
        return [str(v) for v in versions]

    def check_version_compatibility(
        self,
        dataset: str,
        from_version: str,
        to_version: str,
    ) -> dict:
        """Check compatibility between two Gold versions (PRD §28.3).
        
        Args:
            dataset: Dataset name
            from_version: Source version (e.g., "v3.2.1")
            to_version: Target version (e.g., "v3.5.0")
            
        Returns:
            Dict with keys: compatible, breaking, changes
        """
        gold_root = self.data_root / "gold"
        
        from_manifest_path = get_manifest_path(
            gold_root, dataset, GoldVersion.parse(from_version)
        )
        to_manifest_path = get_manifest_path(
            gold_root, dataset, GoldVersion.parse(to_version)
        )
        
        if not from_manifest_path.exists():
            raise ValueError(f"Version {from_version} not found for {dataset}")
        if not to_manifest_path.exists():
            raise ValueError(f"Version {to_version} not found for {dataset}")
        
        from_manifest = VersionManifest.load(from_manifest_path)
        to_manifest = VersionManifest.load(to_manifest_path)
        
        result = check_compatibility(from_manifest, to_manifest)
        return result.to_dict()

    def get_version_lineage(self, dataset: str, version: str) -> dict:
        """Get lineage metadata for a Gold version (PRD §28.4).
        
        Args:
            dataset: Dataset name
            version: Version string
            
        Returns:
            Lineage dict with upstream_deps, code_commit, config_hash
        """
        gold_root = self.data_root / "gold"
        manifest_path = get_manifest_path(
            gold_root, dataset, GoldVersion.parse(version)
        )
        
        if not manifest_path.exists():
            raise ValueError(f"Version {version} not found for {dataset}")
        
        manifest = VersionManifest.load(manifest_path)
        return {
            "version": str(manifest.version),
            "created_at": manifest.created_at.isoformat(),
            "created_by": manifest.created_by,
            **manifest.lineage.to_dict(),
        }

    def read_gold_versioned(
        self,
        dataset: str,
        version: str | None = None,
        asof_time: datetime | str | None = None,
        time_range: tuple[datetime | str, datetime | str] | None = None,
        instrument_keys: list[str] | None = None,
    ) -> pd.DataFrame:
        """Read Gold data with semantic version resolution (PRD §28.2).
        
        Supports:
        - Exact version: version="v3.2.1"
        - Wildcard: version="v3.*" (latest v3.x)
        - Latest: version=None
        
        Args:
            dataset: Dataset name
            version: Version string or wildcard pattern
            asof_time: Point-in-time cutoff
            time_range: (start, end) datetime range
            instrument_keys: Filter to specific instruments
            
        Returns:
            DataFrame with Gold data
        """
        gold_root = self.data_root / "gold"
        available = list_available_versions(gold_root, dataset)
        
        if not available:
            logger.warning("No versions found for Gold dataset", dataset=dataset)
            return pd.DataFrame()
        
        resolved = resolve_version(available, version)
        if resolved is None:
            logger.warning(
                "Version pattern matched no versions",
                dataset=dataset,
                pattern=version,
            )
            return pd.DataFrame()
        
        logger.info(
            "Resolved Gold version",
            dataset=dataset,
            pattern=version,
            resolved=str(resolved),
        )
        
        return self.read_gold(
            dataset=dataset,
            version=str(resolved),
            asof_time=asof_time,
            time_range=time_range,
            instrument_keys=instrument_keys,
        )
