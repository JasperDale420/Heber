"""On-Call and Escalation (PRD §40).

On-call rotation and incident escalation management.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, UTC
from enum import Enum
from typing import Any

import structlog

from heber.sre.runbooks import IncidentSeverity

logger = structlog.get_logger(__name__)


class OnCallRole(str, Enum):
    """On-call roles (PRD §40.1)."""
    
    PRIMARY = "primary"  # 24/7 weekly rotation
    SECONDARY = "secondary"  # Business hours backup
    TECH_LEAD = "tech_lead"  # Escalation


@dataclass
class OnCallSchedule:
    """On-call schedule entry."""
    
    role: OnCallRole
    user: str
    start_time: datetime
    end_time: datetime
    
    def is_active(self, at_time: datetime | None = None) -> bool:
        """Check if schedule is active at given time."""
        at_time = at_time or datetime.now(UTC)
        return self.start_time <= at_time < self.end_time
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "role": self.role.value,
            "user": self.user,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
        }


@dataclass
class EscalationPolicy:
    """Escalation policy for a severity level (PRD §40.2)."""
    
    severity: IncidentSeverity
    initial_response_minutes: int
    escalation_trigger_minutes: int
    escalation_target: OnCallRole
    description: str
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "severity": self.severity.value,
            "initial_response": f"{self.initial_response_minutes} min",
            "escalation_trigger": f"{self.escalation_trigger_minutes} min",
            "escalation_target": self.escalation_target.value,
            "description": self.description,
        }


# Default escalation policies from PRD §40.2
DEFAULT_ESCALATION_POLICIES: list[EscalationPolicy] = [
    EscalationPolicy(
        severity=IncidentSeverity.P1_CRITICAL,
        initial_response_minutes=5,
        escalation_trigger_minutes=15,
        escalation_target=OnCallRole.SECONDARY,
        description="Data loss risk or total outage",
    ),
    EscalationPolicy(
        severity=IncidentSeverity.P2_HIGH,
        initial_response_minutes=15,
        escalation_trigger_minutes=60,
        escalation_target=OnCallRole.SECONDARY,
        description="Significant degradation",
    ),
    EscalationPolicy(
        severity=IncidentSeverity.P3_MEDIUM,
        initial_response_minutes=60,
        escalation_trigger_minutes=240,
        escalation_target=OnCallRole.TECH_LEAD,
        description="Partial impact",
    ),
    EscalationPolicy(
        severity=IncidentSeverity.P4_LOW,
        initial_response_minutes=1440,  # Next business day
        escalation_trigger_minutes=0,  # Track in backlog
        escalation_target=OnCallRole.PRIMARY,
        description="Minor issue",
    ),
]


class CommunicationChannel(str, Enum):
    """Communication channels (PRD §40.4)."""
    
    PAGERDUTY = "pagerduty"
    SLACK_INCIDENTS = "slack_incidents"
    SLACK_ALERTS = "slack_alerts"
    EMAIL = "email"


@dataclass
class ChannelConfig:
    """Configuration for a communication channel."""
    
    channel: CommunicationChannel
    use_for: str
    severities: list[IncidentSeverity]
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "channel": self.channel.value,
            "use_for": self.use_for,
            "severities": [s.value for s in self.severities],
        }


DEFAULT_CHANNEL_CONFIGS: list[ChannelConfig] = [
    ChannelConfig(
        channel=CommunicationChannel.PAGERDUTY,
        use_for="P1/P2 alerts",
        severities=[IncidentSeverity.P1_CRITICAL, IncidentSeverity.P2_HIGH],
    ),
    ChannelConfig(
        channel=CommunicationChannel.SLACK_INCIDENTS,
        use_for="Real-time incident coordination",
        severities=[IncidentSeverity.P1_CRITICAL, IncidentSeverity.P2_HIGH, IncidentSeverity.P3_MEDIUM],
    ),
    ChannelConfig(
        channel=CommunicationChannel.SLACK_ALERTS,
        use_for="Non-paging alerts",
        severities=[IncidentSeverity.P3_MEDIUM, IncidentSeverity.P4_LOW],
    ),
    ChannelConfig(
        channel=CommunicationChannel.EMAIL,
        use_for="Postmortem distribution",
        severities=[IncidentSeverity.P1_CRITICAL, IncidentSeverity.P2_HIGH],
    ),
]


@dataclass
class Incident:
    """An active incident."""
    
    id: str
    title: str
    severity: IncidentSeverity
    alert_name: str
    created_at: datetime
    acknowledged_at: datetime | None = None
    resolved_at: datetime | None = None
    assigned_to: str | None = None
    escalated: bool = False
    
    @property
    def is_open(self) -> bool:
        return self.resolved_at is None
    
    @property
    def time_to_acknowledge(self) -> timedelta | None:
        if self.acknowledged_at:
            return self.acknowledged_at - self.created_at
        return None
    
    @property
    def time_to_resolve(self) -> timedelta | None:
        if self.resolved_at:
            return self.resolved_at - self.created_at
        return None
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "severity": self.severity.value,
            "alert_name": self.alert_name,
            "created_at": self.created_at.isoformat(),
            "acknowledged_at": self.acknowledged_at.isoformat() if self.acknowledged_at else None,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "assigned_to": self.assigned_to,
            "escalated": self.escalated,
            "is_open": self.is_open,
        }


class OnCallManager:
    """Manage on-call schedules and incidents."""
    
    def __init__(
        self,
        escalation_policies: list[EscalationPolicy] | None = None,
        channel_configs: list[ChannelConfig] | None = None,
    ):
        self.escalation_policies = {
            p.severity: p for p in (escalation_policies or DEFAULT_ESCALATION_POLICIES)
        }
        self.channel_configs = channel_configs or DEFAULT_CHANNEL_CONFIGS
        self.schedules: list[OnCallSchedule] = []
        self.incidents: dict[str, Incident] = {}
    
    def add_schedule(self, schedule: OnCallSchedule) -> None:
        """Add an on-call schedule entry."""
        self.schedules.append(schedule)
    
    def get_on_call(
        self,
        role: OnCallRole,
        at_time: datetime | None = None,
    ) -> str | None:
        """Get current on-call user for a role."""
        at_time = at_time or datetime.now(UTC)
        for schedule in self.schedules:
            if schedule.role == role and schedule.is_active(at_time):
                return schedule.user
        return None
    
    def get_escalation_policy(self, severity: IncidentSeverity) -> EscalationPolicy:
        """Get escalation policy for a severity."""
        return self.escalation_policies[severity]
    
    def should_escalate(self, incident: Incident) -> bool:
        """Check if an incident should be escalated."""
        if incident.resolved_at or incident.escalated:
            return False
        
        policy = self.get_escalation_policy(incident.severity)
        elapsed = datetime.now(UTC) - incident.created_at
        return elapsed.total_seconds() / 60 > policy.escalation_trigger_minutes
    
    def get_channels_for_severity(
        self,
        severity: IncidentSeverity,
    ) -> list[CommunicationChannel]:
        """Get appropriate communication channels for a severity."""
        channels = []
        for config in self.channel_configs:
            if severity in config.severities:
                channels.append(config.channel)
        return channels
    
    def create_incident(
        self,
        incident_id: str,
        title: str,
        severity: IncidentSeverity,
        alert_name: str,
    ) -> Incident:
        """Create a new incident."""
        incident = Incident(
            id=incident_id,
            title=title,
            severity=severity,
            alert_name=alert_name,
            created_at=datetime.now(UTC),
        )
        self.incidents[incident_id] = incident
        
        # Auto-assign to primary on-call
        primary = self.get_on_call(OnCallRole.PRIMARY)
        if primary:
            incident.assigned_to = primary
        
        logger.info(
            "Incident created",
            incident_id=incident_id,
            severity=severity.value,
            assigned_to=incident.assigned_to,
        )
        
        return incident
    
    def acknowledge_incident(self, incident_id: str, user: str) -> bool:
        """Acknowledge an incident."""
        incident = self.incidents.get(incident_id)
        if not incident or incident.acknowledged_at:
            return False
        
        incident.acknowledged_at = datetime.now(UTC)
        incident.assigned_to = user
        
        logger.info(
            "Incident acknowledged",
            incident_id=incident_id,
            user=user,
            tta_seconds=incident.time_to_acknowledge.total_seconds() if incident.time_to_acknowledge else None,
        )
        
        return True
    
    def resolve_incident(self, incident_id: str) -> bool:
        """Resolve an incident."""
        incident = self.incidents.get(incident_id)
        if not incident or incident.resolved_at:
            return False
        
        incident.resolved_at = datetime.now(UTC)
        
        logger.info(
            "Incident resolved",
            incident_id=incident_id,
            ttr_seconds=incident.time_to_resolve.total_seconds() if incident.time_to_resolve else None,
        )
        
        return True
    
    def get_open_incidents(self) -> list[Incident]:
        """Get all open incidents."""
        return [i for i in self.incidents.values() if i.is_open]
