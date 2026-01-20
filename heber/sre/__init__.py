"""SRE Module (PRD §37-42).

Service Level Objectives, error budgets, runbooks, on-call, chaos, and capacity planning.
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
from heber.sre.chaos import (
    ExperimentFrequency,
    ExperimentScope,
    ExperimentStatus,
    SuccessCriterion,
    ChaosExperiment,
    ExperimentRun,
    ChaosRegistry,
    DEFAULT_EXPERIMENTS,
)
from heber.sre.capacity import (
    ResourceType,
    ScalingAction,
    BaselineMetric,
    ScalingTrigger,
    CapacityForecast,
    BottleneckAnalysis,
    CostProjection,
    CapacityPlanner,
    DEFAULT_BASELINES,
    DEFAULT_TRIGGERS,
    DEFAULT_FORECASTS,
    DEFAULT_BOTTLENECKS,
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
