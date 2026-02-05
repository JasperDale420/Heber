"""Incident Runbooks (PRD §39).

Structured runbook definitions for incident response.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class IncidentSeverity(str, Enum):
    """Incident severity levels (PRD §40.3)."""

    P1_CRITICAL = "P1"  # Data loss risk or total outage
    P2_HIGH = "P2"  # Significant degradation
    P3_MEDIUM = "P3"  # Partial impact
    P4_LOW = "P4"  # Minor issue


@dataclass
class TriageStep:
    """A single triage step in a runbook."""

    step_number: int
    command: str
    description: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "step": self.step_number,
            "command": self.command,
            "description": self.description,
        }


@dataclass
class ResolutionAction:
    """Resolution action for a specific cause."""

    cause: str
    fix: str

    def to_dict(self) -> dict[str, Any]:
        return {"cause": self.cause, "fix": self.fix}


@dataclass
class Runbook:
    """Incident runbook definition (PRD §39).

    Provides structured response procedures for common incidents.
    """

    title: str
    alert_name: str
    severity: IncidentSeverity
    symptoms: list[str]
    triage_steps: list[TriageStep]
    common_causes: list[str]
    resolutions: list[ResolutionAction]
    escalation_threshold: str = "15 minutes"
    fallback: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "title": self.title,
            "alert_name": self.alert_name,
            "severity": self.severity.value,
            "symptoms": self.symptoms,
            "triage_steps": [s.to_dict() for s in self.triage_steps],
            "common_causes": self.common_causes,
            "resolutions": [r.to_dict() for r in self.resolutions],
            "escalation_threshold": self.escalation_threshold,
            "fallback": self.fallback,
        }

    def to_markdown(self) -> str:
        """Generate markdown-formatted runbook."""
        lines = [
            f"# {self.title}",
            "",
            f"**Alert:** `{self.alert_name}`",
            f"**Severity:** {self.severity.value}",
            "",
            "## Symptoms",
            "",
        ]
        for symptom in self.symptoms:
            lines.append(f"- {symptom}")

        lines.extend(["", "## Triage", ""])
        for step in self.triage_steps:
            lines.append(f"{step.step_number}. {step.description}")
            if step.command:
                lines.append("   ```bash")
                lines.append(f"   {step.command}")
                lines.append("   ```")

        lines.extend(["", "## Common Causes", ""])
        for cause in self.common_causes:
            lines.append(f"- {cause}")

        lines.extend(["", "## Resolution", "", "| Cause | Fix |", "|-------|-----|"])
        for res in self.resolutions:
            lines.append(f"| {res.cause} | {res.fix} |")

        lines.extend(
            [
                "",
                f"**Escalation:** If unresolved in {self.escalation_threshold} → page secondary on-call",
            ]
        )

        if self.fallback:
            lines.extend(["", f"**Fallback:** {self.fallback}"])

        return "\n".join(lines)


# Default runbooks from PRD §39
CONSUMER_LAG_RUNBOOK = Runbook(
    title="Consumer Lag Spike",
    alert_name="HeberConsumerLagHigh",
    severity=IncidentSeverity.P2_HIGH,
    symptoms=[
        "heber_consumer_lag_seconds > 60s (warning) or > 300s (critical)",
        "Data freshness degraded",
    ],
    triage_steps=[
        TriageStep(1, "kubectl get pods -l app=heber-consumer", "Check consumer pod health"),
        TriageStep(2, "redis-cli XPENDING heber:events heber-writers", "Check Redis Streams backlog"),
        TriageStep(3, "kubectl logs -l app=heber-consumer | grep rebalance", "Check if rebalancing"),
    ],
    common_causes=[
        "Consumer pod restart / OOM kill",
        "Redis Streams slow (network, memory)",
        "Upstream provider burst",
    ],
    resolutions=[
        ResolutionAction("Pod OOM", "Increase memory limit, restart"),
        ResolutionAction("Redis slow", "Check ElastiCache metrics, scale if needed"),
        ResolutionAction("Provider burst", "Verify burst is temporary, consider scaling consumers"),
        ResolutionAction("Rebalancing", "Wait for rebalance to complete (~30s)"),
    ],
)

DLQ_GROWING_RUNBOOK = Runbook(
    title="DLQ Growing",
    alert_name="HeberDLQGrowing",
    severity=IncidentSeverity.P2_HIGH,
    symptoms=[
        "heber_dlq_events_total increasing",
        "Events not reaching Silver",
    ],
    triage_steps=[
        TriageStep(1, "aws s3 ls s3://heber/quarantine/ | head -20", "Sample DLQ events"),
        TriageStep(2, "jq . quarantine_sample.json", "Identify error pattern"),
        TriageStep(3, "", "Check upstream provider for changes"),
    ],
    common_causes=[
        "Provider schema change (new field, type change)",
        "Malformed events from gateway",
        "Heber consumer bug",
    ],
    resolutions=[
        ResolutionAction("Schema change", "Update Heber schema, reprocess DLQ"),
        ResolutionAction("Malformed events", "Fix gateway, purge bad events"),
        ResolutionAction("Consumer bug", "Fix, deploy, reprocess DLQ"),
    ],
)

HOTSTORE_SYNC_RUNBOOK = Runbook(
    title="Hot Store Sync Failure",
    alert_name="HeberHotStoreLagHigh",
    severity=IncidentSeverity.P2_HIGH,
    symptoms=[
        "heber_hotstore_lag_seconds > 300s",
        "Hot Store queries return stale data",
    ],
    triage_steps=[
        TriageStep(1, "kubectl logs -l app=heber-hotloader", "Check hotloader pod"),
        TriageStep(2, "clickhouse-client -q 'SELECT 1'", "Check ClickHouse health"),
        TriageStep(3, "", "Check network between EKS and ClickHouse"),
    ],
    common_causes=[
        "ClickHouse cluster unhealthy",
        "Hotloader pod crash",
        "Network partition",
    ],
    resolutions=[
        ResolutionAction("ClickHouse down", "Check CH logs, restart if needed"),
        ResolutionAction("Hotloader OOM", "Increase memory, restart"),
        ResolutionAction("Network issue", "Check security groups, VPC endpoints"),
    ],
    fallback="If Hot Store is down, queries should fall back to Silver (slower but correct)",
)

CATALOG_UNREACHABLE_RUNBOOK = Runbook(
    title="Catalog Unreachable",
    alert_name="HeberCatalogDown",
    severity=IncidentSeverity.P2_HIGH,
    symptoms=[
        'up{job="heber-catalog"} == 0',
        "SDK discovery calls fail",
    ],
    triage_steps=[
        TriageStep(1, "kubectl get pods -l app=heber-catalog", "Check catalog pods"),
        TriageStep(2, "pg_isready -h heber-catalog-rds.internal", "Check RDS health"),
        TriageStep(3, "", "Check network/security groups"),
    ],
    common_causes=[
        "Catalog pod crash",
        "RDS unhealthy",
        "Connection pool exhausted",
    ],
    resolutions=[
        ResolutionAction("Pod crash", "Check logs, restart"),
        ResolutionAction("RDS down", "AWS console, failover to standby"),
        ResolutionAction("Connection pool exhausted", "Increase pool size, check for leaks"),
    ],
    fallback="Writers continue (degraded mode, skip catalog updates). SDK uses cached metadata.",
)

COMPACTION_STUCK_RUNBOOK = Runbook(
    title="Compaction Stuck",
    alert_name="HeberCompactionFailed",
    severity=IncidentSeverity.P3_MEDIUM,
    symptoms=[
        'heber_compactor_runs_total{status="error"} increasing',
        "Small files accumulating in partitions",
    ],
    triage_steps=[
        TriageStep(1, "kubectl logs -l app=heber-compactor", "Check compactor logs for error"),
        TriageStep(2, "aws s3 cat s3://heber/silver/bars/.../manifest.json", "Check manifest file"),
        TriageStep(3, "", "Check for orphaned files"),
    ],
    common_causes=[
        "Corrupted Parquet file",
        "Manifest lock stuck",
        "S3 rate limiting",
    ],
    resolutions=[
        ResolutionAction("Corrupted file", "Identify and quarantine, recompact"),
        ResolutionAction("Stuck lock", "Check lock timestamp, force release if stale"),
        ResolutionAction("S3 throttle", "Backoff, spread compaction windows"),
    ],
)

LEAKAGE_VIOLATION_RUNBOOK = Runbook(
    title="Leakage Violation Detected",
    alert_name="HeberLeakageViolation",
    severity=IncidentSeverity.P1_CRITICAL,
    symptoms=[
        "heber_leakage_violations_total > 0",
        "Query attempted with ts_available > asof_time",
    ],
    triage_steps=[
        TriageStep(1, "", "Identify source: which SDK client? which query?"),
        TriageStep(2, "", "Check if data was actually used (audit log)"),
        TriageStep(3, "", "Assess impact on downstream systems"),
    ],
    common_causes=[
        "SDK bug (should be impossible if SDK is correct)",
        "Direct S3 access bypassing SDK",
        "Clock skew between systems",
    ],
    resolutions=[
        ResolutionAction("SDK bug", "Fix SDK, deploy hotfix, notify all users"),
        ResolutionAction("Direct S3 access", "Block violating client, review access policies"),
        ResolutionAction("Clock skew", "Fix NTP, audit affected queries"),
    ],
    escalation_threshold="5 minutes",
)

# All default runbooks
DEFAULT_RUNBOOKS: dict[str, Runbook] = {
    "consumer_lag": CONSUMER_LAG_RUNBOOK,
    "dlq_growing": DLQ_GROWING_RUNBOOK,
    "hotstore_sync": HOTSTORE_SYNC_RUNBOOK,
    "catalog_unreachable": CATALOG_UNREACHABLE_RUNBOOK,
    "compaction_stuck": COMPACTION_STUCK_RUNBOOK,
    "leakage_violation": LEAKAGE_VIOLATION_RUNBOOK,
}


class RunbookRegistry:
    """Registry of available runbooks."""

    def __init__(self, runbooks: dict[str, Runbook] | None = None):
        self.runbooks = runbooks or DEFAULT_RUNBOOKS

    def get(self, key: str) -> Runbook | None:
        """Get runbook by key."""
        return self.runbooks.get(key)

    def get_by_alert(self, alert_name: str) -> Runbook | None:
        """Get runbook by alert name."""
        for runbook in self.runbooks.values():
            if runbook.alert_name == alert_name:
                return runbook
        return None

    def list_all(self) -> list[dict[str, Any]]:
        """List all runbooks."""
        return [{"key": k, **rb.to_dict()} for k, rb in self.runbooks.items()]

    def export_all_markdown(self) -> str:
        """Export all runbooks as markdown."""
        parts = []
        for runbook in self.runbooks.values():
            parts.append(runbook.to_markdown())
            parts.append("\n---\n")
        return "\n".join(parts)
