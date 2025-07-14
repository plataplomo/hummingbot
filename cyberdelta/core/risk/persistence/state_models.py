"""State models for risk manager persistence."""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from cyberdelta.core.risk.orchestrator.risk_manager_orchestrator import ProcessingStatus


class CheckStatus(Enum):
    """Status of individual checks."""

    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"
    TIMEOUT = "timeout"


@dataclass
class CheckResult:
    """Result of an individual risk check."""

    checker_name: str
    status: CheckStatus
    message: str
    execution_time_ms: float
    timestamp: datetime
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "checker_name": self.checker_name,
            "status": self.status.value,
            "message": self.message,
            "execution_time_ms": self.execution_time_ms,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CheckResult":
        """Create from dictionary."""
        return cls(
            checker_name=data["checker_name"],
            status=CheckStatus(data["status"]),
            message=data["message"],
            execution_time_ms=data["execution_time_ms"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            metadata=data.get("metadata", {}),
        )


@dataclass
class SizingResult:
    """Result of position sizing calculation."""

    strategy_name: str
    recommended_size: Decimal
    max_allowed_size: Decimal
    risk_adjusted_size: Decimal
    allocation_percentage: float
    leverage_ratio: float
    confidence_score: float
    execution_time_ms: float
    timestamp: datetime
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "strategy_name": self.strategy_name,
            "recommended_size": str(self.recommended_size),
            "max_allowed_size": str(self.max_allowed_size),
            "risk_adjusted_size": str(self.risk_adjusted_size),
            "allocation_percentage": self.allocation_percentage,
            "leverage_ratio": self.leverage_ratio,
            "confidence_score": self.confidence_score,
            "execution_time_ms": self.execution_time_ms,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SizingResult":
        """Create from dictionary."""
        return cls(
            strategy_name=data["strategy_name"],
            recommended_size=Decimal(data["recommended_size"]),
            max_allowed_size=Decimal(data["max_allowed_size"]),
            risk_adjusted_size=Decimal(data["risk_adjusted_size"]),
            allocation_percentage=data["allocation_percentage"],
            leverage_ratio=data["leverage_ratio"],
            confidence_score=data["confidence_score"],
            execution_time_ms=data["execution_time_ms"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            metadata=data.get("metadata", {}),
        )


@dataclass
class ConstraintResult:
    """Result of constraint validation."""

    validator_name: str
    is_valid: bool
    violations: list[str]
    applied_adjustments: dict[str, Any]
    execution_time_ms: float
    timestamp: datetime
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "validator_name": self.validator_name,
            "is_valid": self.is_valid,
            "violations": self.violations,
            "applied_adjustments": self.applied_adjustments,
            "execution_time_ms": self.execution_time_ms,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ConstraintResult":
        """Create from dictionary."""
        return cls(
            validator_name=data["validator_name"],
            is_valid=data["is_valid"],
            violations=data["violations"],
            applied_adjustments=data["applied_adjustments"],
            execution_time_ms=data["execution_time_ms"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            metadata=data.get("metadata", {}),
        )


@dataclass
class PerformanceMetrics:
    """Performance metrics for risk manager operations."""

    total_opportunities_processed: int
    successful_processing: int
    failed_processing: int
    average_processing_time_ms: float
    max_processing_time_ms: float
    min_processing_time_ms: float
    check_success_rate: float
    sizing_accuracy_score: float
    constraint_violation_rate: float
    uptime_percentage: float
    memory_usage_mb: float
    cpu_usage_percentage: float
    timestamp: datetime

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "total_opportunities_processed": self.total_opportunities_processed,
            "successful_processing": self.successful_processing,
            "failed_processing": self.failed_processing,
            "average_processing_time_ms": self.average_processing_time_ms,
            "max_processing_time_ms": self.max_processing_time_ms,
            "min_processing_time_ms": self.min_processing_time_ms,
            "check_success_rate": self.check_success_rate,
            "sizing_accuracy_score": self.sizing_accuracy_score,
            "constraint_violation_rate": self.constraint_violation_rate,
            "uptime_percentage": self.uptime_percentage,
            "memory_usage_mb": self.memory_usage_mb,
            "cpu_usage_percentage": self.cpu_usage_percentage,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PerformanceMetrics":
        """Create from dictionary."""
        return cls(
            total_opportunities_processed=data["total_opportunities_processed"],
            successful_processing=data["successful_processing"],
            failed_processing=data["failed_processing"],
            average_processing_time_ms=data["average_processing_time_ms"],
            max_processing_time_ms=data["max_processing_time_ms"],
            min_processing_time_ms=data["min_processing_time_ms"],
            check_success_rate=data["check_success_rate"],
            sizing_accuracy_score=data["sizing_accuracy_score"],
            constraint_violation_rate=data["constraint_violation_rate"],
            uptime_percentage=data["uptime_percentage"],
            memory_usage_mb=data["memory_usage_mb"],
            cpu_usage_percentage=data["cpu_usage_percentage"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
        )


@dataclass
class RiskManagerState:
    """Complete state of the risk manager."""

    session_id: str
    configuration_hash: str
    start_time: datetime
    last_update: datetime
    status: ProcessingStatus

    # Operational state
    active_checks: dict[str, CheckResult]
    recent_check_results: list[CheckResult]
    recent_sizing_results: list[SizingResult]
    recent_constraint_results: list[ConstraintResult]

    # Performance tracking
    performance_metrics: PerformanceMetrics

    # Configuration snapshot
    configuration: dict[str, Any]

    # Error tracking
    recent_errors: list[dict[str, Any]]
    error_counts: dict[str, int] = field(default_factory=dict)

    # Circuit breaker states
    circuit_breaker_states: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "session_id": self.session_id,
            "configuration_hash": self.configuration_hash,
            "start_time": self.start_time.isoformat(),
            "last_update": self.last_update.isoformat(),
            "status": self.status.value,
            "active_checks": {k: v.to_dict() for k, v in self.active_checks.items()},
            "recent_check_results": [r.to_dict() for r in self.recent_check_results],
            "recent_sizing_results": [r.to_dict() for r in self.recent_sizing_results],
            "recent_constraint_results": [r.to_dict() for r in self.recent_constraint_results],
            "performance_metrics": self.performance_metrics.to_dict(),
            "configuration": self.configuration,
            "recent_errors": self.recent_errors,
            "error_counts": self.error_counts,
            "circuit_breaker_states": self.circuit_breaker_states,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RiskManagerState":
        """Create from dictionary."""
        return cls(
            session_id=data["session_id"],
            configuration_hash=data["configuration_hash"],
            start_time=datetime.fromisoformat(data["start_time"]),
            last_update=datetime.fromisoformat(data["last_update"]),
            status=ProcessingStatus(data["status"]),
            active_checks={k: CheckResult.from_dict(v) for k, v in data["active_checks"].items()},
            recent_check_results=[CheckResult.from_dict(r) for r in data["recent_check_results"]],
            recent_sizing_results=[
                SizingResult.from_dict(r) for r in data["recent_sizing_results"]
            ],
            recent_constraint_results=[
                ConstraintResult.from_dict(r) for r in data["recent_constraint_results"]
            ],
            performance_metrics=PerformanceMetrics.from_dict(data["performance_metrics"]),
            configuration=data["configuration"],
            recent_errors=data["recent_errors"],
            error_counts=data.get("error_counts", {}),
            circuit_breaker_states=data.get("circuit_breaker_states", {}),
        )


@dataclass
class StateSnapshot:
    """Point-in-time snapshot of risk manager state."""

    timestamp: datetime
    opportunity_id: str
    processing_stage: str
    check_results: list[CheckResult]
    sizing_result: SizingResult | None
    constraint_result: ConstraintResult | None
    final_decision: dict[str, Any]
    execution_metrics: dict[str, float]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "opportunity_id": self.opportunity_id,
            "processing_stage": self.processing_stage,
            "check_results": [r.to_dict() for r in self.check_results],
            "sizing_result": self.sizing_result.to_dict() if self.sizing_result else None,
            "constraint_result": (
                self.constraint_result.to_dict() if self.constraint_result else None
            ),
            "final_decision": self.final_decision,
            "execution_metrics": self.execution_metrics,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StateSnapshot":
        """Create from dictionary."""
        return cls(
            timestamp=datetime.fromisoformat(data["timestamp"]),
            opportunity_id=data["opportunity_id"],
            processing_stage=data["processing_stage"],
            check_results=[CheckResult.from_dict(r) for r in data["check_results"]],
            sizing_result=(
                SizingResult.from_dict(data["sizing_result"]) if data["sizing_result"] else None
            ),
            constraint_result=(
                ConstraintResult.from_dict(data["constraint_result"])
                if data["constraint_result"]
                else None
            ),
            final_decision=data["final_decision"],
            execution_metrics=data["execution_metrics"],
        )
