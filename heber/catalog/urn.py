"""Dataset URN and path utilities per PRD §11.4.

URNs provide a stable identifier for datasets:
- heber://silver/bars@v1
- heber://silver/quotes@v1
- heber://gold/{project}/features@v1

Path templates translate URNs to actual file system paths.
"""

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import structlog

from heber.config import settings

logger = structlog.get_logger(__name__)


@dataclass
class DatasetURN:
    """Parsed dataset URN (PRD §11.4).
    
    Format: heber://{layer}/{dataset}@{version}
    
    Examples:
        - heber://silver/bars@v1
        - heber://gold/kairos/features@v1
    """
    layer: str  # bronze, silver, gold
    dataset: str  # bars, quotes, etc.
    version: str = "v1"
    project: str | None = None  # For gold datasets
    
    @classmethod
    def parse(cls, urn: str) -> "DatasetURN":
        """Parse a URN string into components.
        
        Args:
            urn: URN string like "heber://silver/bars@v1"
            
        Returns:
            DatasetURN instance
            
        Raises:
            ValueError: If URN format is invalid
        """
        pattern = r"^heber://(\w+)/(.+?)(?:@(\w+))?$"
        match = re.match(pattern, urn)
        
        if not match:
            raise ValueError(f"Invalid URN format: {urn}")
        
        layer = match.group(1)
        dataset_part = match.group(2)
        version = match.group(3) or "v1"
        
        # Check for gold project prefix (e.g., "kairos/features")
        project = None
        dataset = dataset_part
        if layer == "gold" and "/" in dataset_part:
            parts = dataset_part.split("/", 1)
            project = parts[0]
            dataset = parts[1]
        
        return cls(layer=layer, dataset=dataset, version=version, project=project)
    
    def __str__(self) -> str:
        """Convert to URN string."""
        if self.project:
            return f"heber://{self.layer}/{self.project}/{self.dataset}@{self.version}"
        return f"heber://{self.layer}/{self.dataset}@{self.version}"


# Path templates per layer (PRD §11.4)
PATH_TEMPLATES = {
    "bronze": "{layer}/provider={provider}/feed={feed}/dt={dt}/hour={hour}/",
    "silver": "{layer}/feed={feed}/instrument_type={instrument_type}/dt={dt}/",
    "silver_hourly": "{layer}/feed={feed}/instrument_type={instrument_type}/dt={dt}/hour={hour}/",
    "gold": "{layer}/dataset={dataset}/project={project}/version={version}/dt={dt}/",
}


def get_path_template(layer: str, feed: str | None = None) -> str:
    """Get the path template for a layer/feed combination.
    
    Args:
        layer: bronze, silver, gold
        feed: Feed name (used to determine if hourly partitioning)
        
    Returns:
        Path template string
    """
    if layer == "silver" and feed in ("quotes", "trades"):
        return PATH_TEMPLATES["silver_hourly"]
    return PATH_TEMPLATES.get(layer, PATH_TEMPLATES["silver"])


def resolve_path(
    urn: str | DatasetURN,
    dt: date | None = None,
    hour: int | None = None,
    instrument_type: str = "equity",
    provider: str | None = None,
    base_path: str | Path | None = None,
) -> Path:
    """Resolve a URN to an actual file system path.
    
    Args:
        urn: Dataset URN (string or parsed)
        dt: Date partition value
        hour: Hour partition value (for bronze/high-vol silver)
        instrument_type: Instrument type for silver partitioning
        provider: Provider for bronze partitioning
        base_path: Base storage path (defaults to settings.storage_base_path)
        
    Returns:
        Resolved Path object
    """
    if isinstance(urn, str):
        urn = DatasetURN.parse(urn)
    
    if base_path is None:
        base_path = Path(settings.storage_base_path)
    else:
        base_path = Path(base_path)
    
    template = get_path_template(urn.layer, urn.dataset)
    
    # Build partition values
    dt_str = dt.isoformat() if dt else "*"
    hour_str = f"{hour:02d}" if hour is not None else "*"
    
    path_str = template.format(
        layer=urn.layer,
        feed=urn.dataset,
        dataset=urn.dataset,
        instrument_type=instrument_type,
        dt=dt_str,
        hour=hour_str,
        provider=provider or "*",
        project=urn.project or "shared",
        version=urn.version,
    )
    
    return base_path / path_str


def list_partitions(
    urn: str | DatasetURN,
    base_path: str | Path | None = None,
) -> list[dict[str, str]]:
    """List available partitions for a dataset (PRD §11.5 Pattern A).
    
    Args:
        urn: Dataset URN
        base_path: Base storage path
        
    Returns:
        List of partition dictionaries with keys like {dt, hour, instrument_type}
    """
    if isinstance(urn, str):
        urn = DatasetURN.parse(urn)
    
    path = resolve_path(urn, base_path=base_path)
    
    # Find all matching partitions
    # This is a simplified implementation - real version would glob the fs
    partitions = []
    
    # For now, return empty list as placeholder
    # In production, this would scan the filesystem
    logger.debug("list_partitions", urn=str(urn), path=str(path))
    
    return partitions


# Discovery pattern helpers (PRD §11.5)

def discover_by_instrument(
    instrument_key: str,
    dt_start: date | None = None,
    dt_end: date | None = None,
) -> list[dict]:
    """Pattern A: Query by instrument + time range.
    
    Returns list of datasets/partitions containing data for this instrument.
    """
    # This would query data_coverage table
    return []


def discover_by_symbol(
    symbol: str,
    dt_start: date | None = None,
    dt_end: date | None = None,
) -> list[dict]:
    """Pattern B: Query by symbol + date range.
    
    First resolves symbol to instrument_key, then queries coverage.
    """
    # This would:
    # 1. Query instrument_registry for canonical_symbol = symbol
    # 2. Call discover_by_instrument with result
    return []


def trace_by_request(request_id: str) -> dict:
    """Pattern C: Trace by request_id.
    
    Returns the request metadata and any data it produced.
    """
    # This would query requests table
    return {}
