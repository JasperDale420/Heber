"""SRE Module (PRD §37-38).

Service Level Objectives, error budgets, and reliability engineering.
"""

from heber.sre.slo import (
    SLOWindow,
    AlertSeverity,
    SLI,
    SLO,
    BurnRateAlert,
    SLOStatus,
    SLOManager,
    DEFAULT_SLIS,
    DEFAULT_SLOS,
    DEFAULT_BURN_RATE_ALERTS,
)
from heber.sre.error_budget import (
    BudgetState,
    BudgetPolicy,
    DeployRisk,
    DeployApproval,
    ErrorBudget,
    ErrorBudgetManager,
    DEFAULT_POLICIES,
    DEFAULT_DEPLOY_APPROVALS,
)

__all__ = [
    # SLO Framework
    "SLOWindow",
    "AlertSeverity",
    "SLI",
    "SLO",
    "BurnRateAlert",
    "SLOStatus",
    "SLOManager",
    "DEFAULT_SLIS",
    "DEFAULT_SLOS",
    "DEFAULT_BURN_RATE_ALERTS",
    # Error Budget
    "BudgetState",
    "BudgetPolicy",
    "DeployRisk",
    "DeployApproval",
    "ErrorBudget",
    "ErrorBudgetManager",
    "DEFAULT_POLICIES",
    "DEFAULT_DEPLOY_APPROVALS",
]
