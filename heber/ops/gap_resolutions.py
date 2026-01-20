"""Gap Resolution Summaries (PRD §17, §27, §36, §44, §54, §62).

Documentation of resolved design decisions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class DecisionRecord:
    """Record of a design decision."""
    
    id: str
    category: str
    question: str
    decision: str
    rationale: str
    alternatives_considered: list[str] = field(default_factory=list)
    resolved_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "category": self.category,
            "question": self.question,
            "decision": self.decision,
            "rationale": self.rationale,
            "alternatives_considered": self.alternatives_considered,
            "resolved_at": self.resolved_at.isoformat(),
        }


# Gap resolutions from PRD sections
DATA_MODEL_DECISIONS: list[DecisionRecord] = [
    DecisionRecord(
        id="DM-001",
        category="Data Model",
        question="How to partition Silver data?",
        decision="Partition by date (dt) and symbol for market data",
        rationale="Optimizes for common query pattern (symbol + date range)",
        alternatives_considered=["Hour-level partitioning", "Symbol-only"],
    ),
    DecisionRecord(
        id="DM-002",
        category="Data Model",
        question="ts_available vs ts_event?",
        decision="Use ts_available for all point-in-time queries",
        rationale="Prevents look-ahead bias in backtesting",
    ),
    DecisionRecord(
        id="DM-003",
        category="Data Model",
        question="Schema evolution strategy?",
        decision="Semver for schemas (v<major>.<minor>), backward compatible by default",
        rationale="Allows schema changes without breaking existing consumers",
    ),
]

INFRASTRUCTURE_DECISIONS: list[DecisionRecord] = [
    DecisionRecord(
        id="INF-001",
        category="Infrastructure",
        question="Cold storage tier?",
        decision="S3 Glacier for data older than 1 year",
        rationale="Cost optimization while maintaining data availability",
    ),
    DecisionRecord(
        id="INF-002",
        category="Infrastructure",
        question="Hot store technology?",
        decision="ClickHouse for real-time queries",
        rationale="Columnar storage optimized for analytical queries",
        alternatives_considered=["TimescaleDB", "InfluxDB", "Druid"],
    ),
    DecisionRecord(
        id="INF-003",
        category="Infrastructure",
        question="Event bus technology?",
        decision="Redis Streams",
        rationale="Simple, fast, good consumer group support",
        alternatives_considered=["Kafka", "Pulsar", "NATS"],
    ),
]

ML_QUANT_DECISIONS: list[DecisionRecord] = [
    DecisionRecord(
        id="ML-001",
        category="ML/Quant",
        question="Feature versioning strategy?",
        decision="GoldVersion with lineage tracking",
        rationale="Enables reproducibility and feature governance",
    ),
    DecisionRecord(
        id="ML-002",
        category="ML/Quant",
        question="Train/test split approach?",
        decision="Walk-forward with embargo period",
        rationale="Prevents data leakage in time-series data",
    ),
    DecisionRecord(
        id="ML-003",
        category="ML/Quant",
        question="Label semantic?",
        decision="Forward-looking with explicit ts_available at label computation time",
        rationale="Ensures labels represent future returns not available at train time",
    ),
]

RELIABILITY_DECISIONS: list[DecisionRecord] = [
    DecisionRecord(
        id="REL-001",
        category="Reliability",
        question="SLO targets for ingestion?",
        decision="99.9% availability, 10K events/sec throughput",
        rationale="Balances reliability with infrastructure cost",
    ),
    DecisionRecord(
        id="REL-002",
        category="Reliability",
        question="Error budget policy?",
        decision="Freeze deploys when budget < 10%",
        rationale="Protects users from reliability degradation",
    ),
    DecisionRecord(
        id="REL-003",
        category="Reliability",
        question="Circuit breaker thresholds?",
        decision="Open after 5 failures in 30s, half-open after 60s",
        rationale="Fast failure detection with reasonable recovery time",
    ),
]

TESTING_DECISIONS: list[DecisionRecord] = [
    DecisionRecord(
        id="TEST-001",
        category="Testing",
        question="Test pyramid distribution?",
        decision="70% unit, 25% integration, 5% E2E",
        rationale="Fast feedback with comprehensive coverage",
    ),
    DecisionRecord(
        id="TEST-002",
        category="Testing",
        question="Leakage test policy?",
        decision="0% tolerance, all leakage tests must pass",
        rationale="Zero-leakage is a critical invariant",
    ),
    DecisionRecord(
        id="TEST-003",
        category="Testing",
        question="Flaky test handling?",
        decision="Quarantine at >5% flake rate, fix within 3 days",
        rationale="Maintains CI reliability while allowing investigation",
    ),
]

DATA_SOURCE_DECISIONS: list[DecisionRecord] = [
    DecisionRecord(
        id="DS-001",
        category="Data Sources",
        question="Structured vs unstructured boundary?",
        decision="Heber for structured (Parquet), Document Store for text",
        rationale="Optimizes each storage for its primary use case",
    ),
    DecisionRecord(
        id="DS-002",
        category="Data Sources",
        question="Cross-reference mechanism?",
        decision="doc_store_id in Heber metadata pointing to Document Store",
        rationale="Single source of truth for metadata with external text storage",
    ),
    DecisionRecord(
        id="DS-003",
        category="Data Sources",
        question="Provider priority?",
        decision="Alpaca (1), Unusual Whales (1), Finnhub (2), Alpha Vantage (3)",
        rationale="Based on data quality, reliability, and coverage",
    ),
]


class GapResolutionRegistry:
    """Registry of all gap resolution decisions."""
    
    def __init__(self):
        self.decisions: dict[str, list[DecisionRecord]] = {
            "data_model": DATA_MODEL_DECISIONS,
            "infrastructure": INFRASTRUCTURE_DECISIONS,
            "ml_quant": ML_QUANT_DECISIONS,
            "reliability": RELIABILITY_DECISIONS,
            "testing": TESTING_DECISIONS,
            "data_sources": DATA_SOURCE_DECISIONS,
        }
    
    def get_by_category(self, category: str) -> list[DecisionRecord]:
        """Get decisions by category."""
        return self.decisions.get(category, [])
    
    def get_by_id(self, decision_id: str) -> DecisionRecord | None:
        """Get decision by ID."""
        for decisions in self.decisions.values():
            for d in decisions:
                if d.id == decision_id:
                    return d
        return None
    
    def list_all(self) -> dict[str, list[dict[str, Any]]]:
        """List all decisions by category."""
        return {
            category: [d.to_dict() for d in decisions]
            for category, decisions in self.decisions.items()
        }
    
    def generate_report(self) -> dict[str, Any]:
        """Generate gap resolution report."""
        total = sum(len(d) for d in self.decisions.values())
        
        return {
            "summary": {
                "total_decisions": total,
                "by_category": {cat: len(decs) for cat, decs in self.decisions.items()},
            },
            "decisions": self.list_all(),
            "generated_at": datetime.now(UTC).isoformat(),
        }
