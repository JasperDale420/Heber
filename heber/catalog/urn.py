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
        base_path: Base storage path (defaults to settings.data_root)

    Returns:
        Resolved Path object
    """
    if isinstance(urn, str):
        urn = DatasetURN.parse(urn)

    if base_path is None:
        base_path = Path(settings.data_root)
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
