"""Error Budget Policy (PRD §38).

Error budget calculation, tracking, and policy enforcement.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class BudgetState(StrEnum):
    """Error budget states (PRD §38.2)."""

    HEALTHY = "healthy"  # > 50% remaining
    WARNING = "warning"  # 25-50% remaining
    CRITICAL = "critical"  # < 25% remaining
    EXHAUSTED = "exhausted"  # 0% remaining

    @classmethod
    def from_remaining(cls, remaining: float) -> BudgetState:
        """Get state from remaining budget ratio."""
        if remaining <= 0:
            return cls.EXHAUSTED
        elif remaining < 0.25:
            return cls.CRITICAL
        elif remaining < 0.50:
            return cls.WARNING
        else:
            return cls.HEALTHY


@dataclass
class BudgetPolicy:
    """Policy for a budget state (PRD §38.2)."""

    state: BudgetState
    description: str
    actions: list[str]
    deploy_allowed: bool
    risky_deploy_allowed: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "state": self.state.value,
            "description": self.description,
            "actions": self.actions,
            "deploy_allowed": self.deploy_allowed,
            "risky_deploy_allowed": self.risky_deploy_allowed,
        }


# Default policies from PRD §38.2
DEFAULT_POLICIES: dict[BudgetState, BudgetPolicy] = {
    BudgetState.HEALTHY: BudgetPolicy(
        state=BudgetState.HEALTHY,
        description="Normal operations, feature velocity",
        actions=["Normal development", "Standard deploys allowed"],
        deploy_allowed=True,
        risky_deploy_allowed=True,
    ),
    BudgetState.WARNING: BudgetPolicy(
        state=BudgetState.WARNING,
        description="Pause risky deploys, prioritize reliability",
        actions=["Pause risky deploys", "Review reliability work"],
        deploy_allowed=True,
        risky_deploy_allowed=False,
    ),
    BudgetState.CRITICAL: BudgetPolicy(
        state=BudgetState.CRITICAL,
        description="Freeze features, all-hands on reliability",
        actions=["Feature freeze", "Focus on reliability fixes only"],
        deploy_allowed=True,  # Only reliability fixes
        risky_deploy_allowed=False,
    ),
    BudgetState.EXHAUSTED: BudgetPolicy(
        state=BudgetState.EXHAUSTED,
        description="Incident review required before resuming",
        actions=["Mandatory incident review", "No deploys until postmortem complete"],
        deploy_allowed=False,
        risky_deploy_allowed=False,
    ),
}


class DeployRisk(StrEnum):
    """Deploy risk level (PRD §38.4)."""

    STANDARD = "standard"  # 0-1% budget cost
    HIGH_RISK = "high_risk"  # 1-5% budget cost
    BREAKING_CHANGE = "breaking_change"  # 5-10% budget cost
    INFRASTRUCTURE = "infrastructure"  # 10-25% budget cost


@dataclass
class DeployApproval:
    """Deploy approval requirement (PRD §38.4)."""

    risk_level: DeployRisk
    budget_cost_min: float
    budget_cost_max: float
    approval_required: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "risk_level": self.risk_level.value,
            "budget_cost": f"{self.budget_cost_min * 100:.0f}-{self.budget_cost_max * 100:.0f}%",
            "approval_required": self.approval_required,
        }


DEFAULT_DEPLOY_APPROVALS: list[DeployApproval] = [
    DeployApproval(DeployRisk.STANDARD, 0.0, 0.01, "None"),
    DeployApproval(DeployRisk.HIGH_RISK, 0.01, 0.05, "Tech lead"),
    DeployApproval(DeployRisk.BREAKING_CHANGE, 0.05, 0.10, "Engineering manager"),
    DeployApproval(DeployRisk.INFRASTRUCTURE, 0.10, 0.25, "Director"),
]


@dataclass
class ErrorBudget:
    """Error budget for an SLO (PRD §38.1).

    Monthly error budget = (1 - SLO target) × total requests
    """

    slo_name: str
    target: float
    total_requests: int
    error_count: int
    period_start: datetime
    period_end: datetime

    @property
    def allowed_errors(self) -> float:
        """Total allowed errors for the period."""
        return (1 - self.target) * self.total_requests

    @property
    def remaining_errors(self) -> float:
        """Remaining allowed errors."""
        return max(0, self.allowed_errors - self.error_count)

    @property
    def consumed_ratio(self) -> float:
        """Ratio of budget consumed (0-1+)."""
        if self.allowed_errors == 0:
            return 1.0 if self.error_count > 0 else 0.0
        return self.error_count / self.allowed_errors

    @property
    def remaining_ratio(self) -> float:
        """Ratio of budget remaining (0-1)."""
        return max(0, 1 - self.consumed_ratio)

    @property
    def state(self) -> BudgetState:
        """Current budget state."""
        return BudgetState.from_remaining(self.remaining_ratio)

    def to_dict(self) -> dict[str, Any]:
        return {
            "slo_name": self.slo_name,
            "target": f"{self.target * 100:.2f}%",
            "total_requests": self.total_requests,
            "error_count": self.error_count,
            "allowed_errors": int(self.allowed_errors),
            "remaining_errors": int(self.remaining_errors),
            "consumed": f"{self.consumed_ratio * 100:.1f}%",
            "remaining": f"{self.remaining_ratio * 100:.1f}%",
            "state": self.state.value,
            "period_start": self.period_start.isoformat(),
            "period_end": self.period_end.isoformat(),
        }


class ErrorBudgetManager:
    """Manage error budgets and enforce policies."""

    def __init__(
        self,
        policies: dict[BudgetState, BudgetPolicy] | None = None,
        deploy_approvals: list[DeployApproval] | None = None,
    ):
        self.policies = policies or DEFAULT_POLICIES
        self.deploy_approvals = deploy_approvals or DEFAULT_DEPLOY_APPROVALS

    def calculate_budget(
        self,
        slo_name: str,
        target: float,
        total_requests: int,
        error_count: int,
        period_days: int = 30,
    ) -> ErrorBudget:
        """Calculate error budget for an SLO.

        Args:
            slo_name: SLO name
            target: SLO target (e.g., 0.999)
            total_requests: Total requests in period
            error_count: Errors in period
            period_days: Period length in days

        Returns:
            ErrorBudget with calculations
        """
        now = datetime.now(UTC)
        period_start = now - timedelta(days=period_days)

        return ErrorBudget(
            slo_name=slo_name,
            target=target,
            total_requests=total_requests,
            error_count=error_count,
            period_start=period_start,
            period_end=now,
        )

    def get_policy(self, budget: ErrorBudget) -> BudgetPolicy:
        """Get the policy for a budget's current state."""
        return self.policies[budget.state]

    def can_deploy(
        self,
        budget: ErrorBudget,
        risk_level: DeployRisk = DeployRisk.STANDARD,
    ) -> tuple[bool, str]:
        """Check if a deploy is allowed given current budget.

        Args:
            budget: Current error budget
            risk_level: Deploy risk level

        Returns:
            Tuple of (allowed, reason)
        """
        policy = self.get_policy(budget)

        if not policy.deploy_allowed:
            return False, f"Deploys blocked: budget {budget.state.value}, {policy.description}"

        if risk_level != DeployRisk.STANDARD and not policy.risky_deploy_allowed:
            return False, f"Risky deploys blocked: budget {budget.state.value}"

        return True, "Deploy allowed"

    def get_approval_required(self, risk_level: DeployRisk) -> str:
        """Get approval required for a deploy risk level."""
        for approval in self.deploy_approvals:
            if approval.risk_level == risk_level:
                return approval.approval_required
        return "Unknown"

    def generate_report(self, budgets: list[ErrorBudget]) -> dict[str, Any]:
        """Generate error budget report.

        Args:
            budgets: List of error budgets

        Returns:
            Report with summary and per-SLO details
        """
        summary = {
            "healthy": 0,
            "warning": 0,
            "critical": 0,
            "exhausted": 0,
        }

        details = []
        for budget in budgets:
            summary[budget.state.value] += 1
            policy = self.get_policy(budget)
            details.append(
                {
                    **budget.to_dict(),
                    "policy": policy.to_dict(),
                }
            )

        overall_state = BudgetState.HEALTHY
        if summary["exhausted"] > 0:
            overall_state = BudgetState.EXHAUSTED
        elif summary["critical"] > 0:
            overall_state = BudgetState.CRITICAL
        elif summary["warning"] > 0:
            overall_state = BudgetState.WARNING

        return {
            "overall_state": overall_state.value,
            "summary": summary,
            "budgets": details,
            "generated_at": datetime.now(UTC).isoformat(),
        }
