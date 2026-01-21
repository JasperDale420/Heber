"""SRE Module (PRD §37-42).

Service Level Objectives, error budgets, runbooks, on-call, chaos, and capacity planning.
"""

from heber.sre.capacity import (
    DEFAULT_BASELINES,
    DEFAULT_BOTTLENECKS,
    DEFAULT_FORECASTS,
    DEFAULT_TRIGGERS,
    BaselineMetric,
    BottleneckAnalysis,
    CapacityForecast,
    CapacityPlanner,
    CostProjection,
    ResourceType,
    ScalingAction,
    ScalingTrigger,
)
from heber.sre.chaos import (
    DEFAULT_EXPERIMENTS,
    ChaosExperiment,
    ChaosRegistry,
    ExperimentFrequency,
    ExperimentRun,
    ExperimentScope,
    ExperimentStatus,
    SuccessCriterion,
)
from heber.sre.error_budget import (
    DEFAULT_DEPLOY_APPROVALS,
    DEFAULT_POLICIES,
    BudgetPolicy,
    BudgetState,
    DeployApproval,
    DeployRisk,
    ErrorBudget,
    ErrorBudgetManager,
)
from heber.sre.oncall import (
    DEFAULT_CHANNEL_CONFIGS,
    DEFAULT_ESCALATION_POLICIES,
    ChannelConfig,
    CommunicationChannel,
    EscalationPolicy,
    Incident,
    OnCallManager,
    OnCallRole,
    OnCallSchedule,
)
from heber.sre.runbooks import (
    DEFAULT_RUNBOOKS,
    IncidentSeverity,
    ResolutionAction,
    Runbook,
    RunbookRegistry,
    TriageStep,
)
from heber.sre.slo import (
    DEFAULT_BURN_RATE_ALERTS,
    DEFAULT_SLIS,
    DEFAULT_SLOS,
    SLI,
    SLO,
    AlertSeverity,
    BurnRateAlert,
    SLOManager,
    SLOStatus,
    SLOWindow,
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
    # Chaos Engineering
    "ExperimentFrequency",
    "ExperimentScope",
    "ExperimentStatus",
    "SuccessCriterion",
    "ChaosExperiment",
    "ExperimentRun",
    "ChaosRegistry",
    "DEFAULT_EXPERIMENTS",
    # Capacity Planning
    "ResourceType",
    "ScalingAction",
    "BaselineMetric",
    "ScalingTrigger",
    "CapacityForecast",
    "BottleneckAnalysis",
    "CostProjection",
    "CapacityPlanner",
    "DEFAULT_BASELINES",
    "DEFAULT_TRIGGERS",
    "DEFAULT_FORECASTS",
    "DEFAULT_BOTTLENECKS",
]
