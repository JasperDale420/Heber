"""Feature Templates Package (PRD §32).

Ready-to-use feature computation templates for ML pipelines.
"""

from heber.features.templates.cross_asset import compute_relative_features
from heber.features.templates.flow import compute_flow_features
from heber.features.templates.labels import (
    compute_classification_labels,
    compute_return_labels,
)
from heber.features.templates.microstructure import compute_microstructure_features
from heber.features.templates.momentum import (
    compute_momentum_features,
    compute_rsi,
)
from heber.features.templates.volatility import (
    compute_atr,
    compute_parkinson_vol,
    compute_volatility_features,
)

__all__ = [
    "compute_momentum_features",
    "compute_rsi",
    "compute_volatility_features",
    "compute_atr",
    "compute_parkinson_vol",
    "compute_flow_features",
    "compute_microstructure_features",
    "compute_relative_features",
    "compute_return_labels",
    "compute_classification_labels",
]
