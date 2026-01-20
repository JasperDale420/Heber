"""SRE Module (PRD §37-40).

Service Level Objectives, error budgets, runbooks, and on-call management.
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
from heber.sre.runbooks import (
    IncidentSeverity,
    TriageStep,
    ResolutionAction,
    Runbook,
    RunbookRegistry,
    DEFAULT_RUNBOOKS,
)
from heber.sre.oncall import (
    OnCallRole,
    OnCallSchedule,
    EscalationPolicy,
    CommunicationChannel,
    ChannelConfig,
    Incident,
    OnCallManager,
    DEFAULT_ESCALATION_POLICIES,
    DEFAULT_CHANNEL_CONFIGS,
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
    # Runbooks
    "IncidentSeverity",
    "TriageStep",
    "ResolutionAction",
    "Runbook",
    "RunbookRegistry",
    "DEFAULT_RUNBOOKS",
    # On-Call
    "OnCallRole",
    "OnCallSchedule",
    "EscalationPolicy",
    "CommunicationChannel",
    "ChannelConfig",
    "Incident",
    "OnCallManager",
    "DEFAULT_ESCALATION_POLICIES",
    "DEFAULT_CHANNEL_CONFIGS",
]
