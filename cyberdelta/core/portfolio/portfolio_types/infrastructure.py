"""Infrastructure types for events, validation, state management.

This module consolidates all infrastructure-related types from:
- state_types.py
- exception_models.py
- validation_types.py
- resilience_types.py
- discriminated_unions.py (if exists)
- type_guards.py
- events/*.py
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, NotRequired, Protocol, TypeVar, TypedDict, cast, runtime_checkable
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator
from pydantic.dataclasses import dataclass

from cyberdelta.core.portfolio.exceptions.base import PortfolioError
from cyberdelta.core.portfolio.exceptions.state import (
    ContainerSizeLimitExceededError,
    StateValidationError,
)


# Type variables
T = TypeVar("T", bound=BaseModel)


# ==================== Event Types ====================

class EventType(Enum):
    """Portfolio event types."""
    # Trade events
    TRADE_RECEIVED = "trade.received"
    TRADE_VALIDATED = "trade.validated"
    TRADE_PROCESSED = "trade.processed"
    TRADE_REJECTED = "trade.rejected"
    
    # Balance events
    BALANCE_UPDATED = "balance.updated"
    BALANCE_RECONCILED = "balance.reconciled"
    BALANCE_ERROR = "balance.error"
    
    # Position events
    POSITION_OPENED = "position.opened"
    POSITION_UPDATED = "position.updated"
    POSITION_CLOSED = "position.closed"
    POSITION_ERROR = "position.error"
    
    # Order events
    ORDER_PLACED = "order.placed"
    ORDER_FILLED = "order.filled"
    ORDER_CANCELLED = "order.cancelled"
    ORDER_REJECTED = "order.rejected"
    
    # P&L events
    PNL_REALIZED = "pnl.realized"
    PNL_UNREALIZED_UPDATED = "pnl.unrealized.updated"
    
    # Risk events
    EXPOSURE_CALCULATED = "exposure.calculated"
    RISK_LIMIT_WARNING = "risk.limit.warning"
    RISK_LIMIT_BREACH = "risk.limit.breach"
    
    # System events
    COMPONENT_INITIALIZED = "component.initialized"
    COMPONENT_SHUTDOWN = "component.shutdown"
    STATE_SNAPSHOT_CREATED = "state.snapshot.created"
    STATE_RESTORED = "state.restored"
    
    # Error events
    ERROR_OCCURRED = "error.occurred"
    ERROR_RECOVERED = "error.recovered"


class EventPriority(Enum):
    """Event priority levels."""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4


# Type-preserving factory function for tags dict
def _str_str_dict_factory() -> dict[str, str]:
    """Factory function that preserves dict[str, str] type information."""
    return {}


@dataclass
class EventMetadata:
    """Metadata for portfolio events."""
    event_id: UUID = Field(default_factory=uuid4)
    timestamp: float = Field(default_factory=time.time)
    source_component: str = ""
    correlation_id: UUID | None = None
    exchange_id: str | None = None
    symbol: str | None = None
    priority: EventPriority = EventPriority.NORMAL
    retry_count: int = 0
    tags: dict[str, str] = Field(default_factory=_str_str_dict_factory)
    
    def to_dict(self) -> dict[str, Any]:
        """Convert metadata to dictionary."""
        return {
            "event_id": str(self.event_id),
            "timestamp": self.timestamp,
            "source_component": self.source_component,
            "correlation_id": str(self.correlation_id) if self.correlation_id else None,
            "exchange_id": self.exchange_id,
            "symbol": self.symbol,
            "priority": self.priority.name,
            "retry_count": self.retry_count,
            "tags": self.tags,
        }


@dataclass
class BasePortfolioEvent[T](ABC):
    """Base class for all portfolio events."""
    event_type: EventType
    data: T
    metadata: EventMetadata = Field(default_factory=EventMetadata)
    
    def __post_init__(self) -> None:
        """Post-initialization setup."""
        if not self.metadata.source_component:
            self.metadata.source_component = self.__class__.__name__
    
    @property
    def event_id(self) -> UUID:
        """Get event ID."""
        return self.metadata.event_id
    
    @property
    def timestamp(self) -> float:
        """Get event timestamp."""
        return self.metadata.timestamp
    
    @property
    def age(self) -> float:
        """Get event age in seconds."""
        return time.time() - self.metadata.timestamp
    
    def is_expired(self, max_age_seconds: float) -> bool:
        """Check if event has expired."""
        return self.age > max_age_seconds
    
    def to_dict(self) -> dict[str, Any]:
        """Convert event to dictionary representation."""
        return {
            "event_type": self.event_type.value,
            "data": self._serialize_data(),
            "metadata": self.metadata.to_dict(),
        }
    
    @abstractmethod
    def _serialize_data(self) -> dict[str, Any]:
        """Serialize event data."""
        ...
    
    def __str__(self) -> str:
        """String representation."""
        return (
            f"{self.__class__.__name__}("
            f"type={self.event_type.value}, "
            f"id={self.event_id}, "
            f"source={self.metadata.source_component})"
        )


class PortfolioEvent(BaseModel):
    """Portfolio event base class."""
    event_type: EventType
    exchange_id: str
    timestamp: float
    data: dict[str, Any]
    metadata: EventMetadata | None = None


# ==================== State Management Types ====================

class StateChangeType(Enum):
    """Types of state changes."""
    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"
    RESTORED = "restored"
    SNAPSHOT = "snapshot"


class StateValidationMetadata(BaseModel):
    """Typed metadata for state validation results."""
    validation_timestamp: float | None = None
    validator_version: str | None = None
    check_type: str | None = None
    data_source: str | None = None
    validation_scope: str | None = None
    performance_metrics: dict[str, float] = Field(default_factory=dict)
    related_components: list[str] = Field(default_factory=list)


class StateChange[T: BaseModel](BaseModel):
    """Represents a change in state."""
    entity_type: str
    entity_id: str
    change_type: StateChangeType
    timestamp: float
    
    # State data
    previous_state: T | None = None
    new_state: T | None = None
    
    # Change metadata
    changed_fields: list[str] = Field(default_factory=list)
    change_reason: str | None = None
    change_source: str | None = None
    
    # Validation
    is_valid: bool = True
    validation_errors: list[str] = Field(default_factory=list)


def create_business_rule_violation(
    rule_name: str,
    description: str,
    severity: str = "ERROR",
    context: dict[str, str | int | float | bool] | None = None,
) -> ValidationIssue:
    """Create a business rule violation issue.
    
    Args:
        rule_name: Name of the violated business rule
        description: Description of the violation
        severity: Severity level (ERROR, WARNING, INFO)
        context: Optional context information
        
    Returns:
        ValidationIssue representing the business rule violation
    """
    return ValidationIssue(
        issue_type="BUSINESS_RULE_VIOLATION",
        severity=severity,
        description=description,
        context=context or {},
        suggestion=f"Review and fix business rule violation: {rule_name}",
    )


def create_range_violation_issue(
    field_name: str,
    actual_value: str | int | float,
    min_value: str | int | float | None = None,
    max_value: str | int | float | None = None,
    severity: str = "ERROR",
) -> ValidationIssue:
    """Create a range violation issue.
    
    Args:
        field_name: Name of the field with range violation
        actual_value: The actual value that violates the range
        min_value: Minimum allowed value (optional)
        max_value: Maximum allowed value (optional)
        severity: Severity level (ERROR, WARNING, INFO)
        
    Returns:
        ValidationIssue representing the range violation
    """
    context = {
        "field_name": field_name,
        "actual_value": str(actual_value),
    }
    if min_value is not None:
        context["min_value"] = str(min_value)
    if max_value is not None:
        context["max_value"] = str(max_value)
    
    description = f"Value {actual_value} for field '{field_name}' is out of range"
    if min_value is not None and max_value is not None:
        description += f" (expected: {min_value} to {max_value})"
    elif min_value is not None:
        description += f" (expected: >= {min_value})"
    elif max_value is not None:
        description += f" (expected: <= {max_value})"
    
    return ValidationIssue(
        issue_type="RANGE_VIOLATION",
        severity=severity,
        description=description,
        context=context,
        suggestion=f"Ensure '{field_name}' value is within acceptable range",
    )


class StateSnapshot[T: BaseModel](BaseModel):
    """Snapshot of state at a point in time."""
    snapshot_id: str
    timestamp: float
    entity_type: str
    entity_count: int
    
    # State data
    entities: dict[str, T] = Field(default_factory=dict)
    
    # Metadata
    metadata: dict[str, str] = Field(default_factory=dict)
    checksum: str | None = None
    compressed: bool = False
    
    def get_entity(self, entity_id: str) -> T | None:
        """Get entity by ID."""
        return self.entities.get(entity_id)
    
    def has_entity(self, entity_id: str) -> bool:
        """Check if entity exists."""
        return entity_id in self.entities


class StateContainer[T: BaseModel](BaseModel):
    """Container for managing stateful entities."""
    entity_type: str
    max_size: int | None = None
    
    # Current state
    _entities: dict[str, T] = PrivateAttr(default_factory=lambda: cast(dict[str, T], {}))
    _change_history: list[StateChange[T]] = PrivateAttr(
        default_factory=lambda: cast(list[StateChange[T]], [])
    )
    _snapshots: list[StateSnapshot[T]] = PrivateAttr(
        default_factory=lambda: cast(list[StateSnapshot[T]], [])
    )
    
    # Metadata
    created_at: float = Field(default=0.0)
    last_modified: float = Field(default=0.0)
    modification_count: int = 0
    
    def add(self, entity_id: str, entity: T) -> None:
        """Add entity to container."""
        if self.max_size and len(self._entities) >= self.max_size:
            raise ContainerSizeLimitExceededError(max_size=self.max_size)
        
        previous = self._entities.get(entity_id)
        self._entities[entity_id] = entity
        
        # Record change
        change = StateChange(
            entity_type=self.entity_type,
            entity_id=entity_id,
            change_type=StateChangeType.CREATED if previous is None else StateChangeType.UPDATED,
            timestamp=time.time(),
            previous_state=previous,
            new_state=entity,
        )
        self._change_history.append(change)
        self.modification_count += 1
        self.last_modified = time.time()
    
    def remove(self, entity_id: str) -> T | None:
        """Remove entity from container."""
        entity = self._entities.pop(entity_id, None)
        if entity:
            # Record change
            change = StateChange(
                entity_type=self.entity_type,
                entity_id=entity_id,
                change_type=StateChangeType.DELETED,
                timestamp=time.time(),
                previous_state=entity,
                new_state=None,
            )
            self._change_history.append(change)
            self.modification_count += 1
            self.last_modified = time.time()
        return entity
    
    def get(self, entity_id: str) -> T | None:
        """Get entity by ID."""
        return self._entities.get(entity_id)
    
    def get_all(self) -> dict[str, T]:
        """Get all entities."""
        return self._entities.copy()
    
    def clear(self) -> None:
        """Clear all entities."""
        self._entities.clear()
        self._change_history.clear()
        self.modification_count += 1
        self.last_modified = time.time()
    
    def create_snapshot(self, snapshot_id: str) -> StateSnapshot[T]:
        """Create snapshot of current state."""
        snapshot = StateSnapshot(
            snapshot_id=snapshot_id,
            timestamp=time.time(),
            entity_type=self.entity_type,
            entity_count=len(self._entities),
            entities=self._entities.copy(),
        )
        self._snapshots.append(snapshot)
        return snapshot
    
    def restore_snapshot(self, snapshot_id: str) -> bool:
        """Restore state from snapshot."""
        snapshot: StateSnapshot[T] | None = next(
            (s for s in self._snapshots if s.snapshot_id == snapshot_id), None
        )
        if not snapshot:
            return False
        
        # Record restoration
        change: StateChange[T] = StateChange(
            entity_type=self.entity_type,
            entity_id="*",  # All entities
            change_type=StateChangeType.RESTORED,
            timestamp=time.time(),
            change_source=f"snapshot:{snapshot_id}",
        )
        self._change_history.append(change)
        
        # Restore state
        self._entities = snapshot.entities.copy()
        self.modification_count += 1
        self.last_modified = time.time()
        
        return True
    
    @property
    def size(self) -> int:
        """Get number of entities."""
        return len(self._entities)
    
    @property
    def is_empty(self) -> bool:
        """Check if container is empty."""
        return len(self._entities) == 0
    
    @property
    def change_count(self) -> int:
        """Get number of recorded changes."""
        return len(self._change_history)
    
    @property
    def snapshot_count(self) -> int:
        """Get number of snapshots."""
        return len(self._snapshots)


class StateSummary(BaseModel):
    """Summary of state across all containers."""
    timestamp: float
    
    # Entity counts
    balance_count: int = 0
    position_count: int = 0
    order_count: int = 0
    trade_count: int = 0
    
    # State health
    total_entities: int = 0
    total_changes: int = 0
    total_snapshots: int = 0
    
    # Memory usage
    estimated_memory_bytes: int = 0
    
    # Validation
    validation_errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    
    def to_dict(self) -> dict[str, object]:
        """Convert to dictionary."""
        return {
            "timestamp": self.timestamp,
            "balance_count": self.balance_count,
            "position_count": self.position_count,
            "order_count": self.order_count,
            "trade_count": self.trade_count,
            "total_entities": self.total_entities,
            "total_changes": self.total_changes,
            "total_snapshots": self.total_snapshots,
            "estimated_memory_bytes": self.estimated_memory_bytes,
            "validation_errors": self.validation_errors,
            "warnings": self.warnings,
        }


class StateUpdateResult(BaseModel):
    """Result of state update operation."""
    success: bool
    execution_time_ms: float = 0.0
    affected_entities: int = 0
    errors: list[str] = Field(default_factory=list)
    metadata: dict[str, str] = Field(default_factory=dict)


# ==================== Validation Types ====================

class ValidationSeverity(Enum):
    """Validation issue severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ValidationCategory(Enum):
    """Categories of validation issues."""
    # Data integrity
    MISSING_REQUIRED = "missing_required"
    INVALID_TYPE = "invalid_type"
    INVALID_FORMAT = "invalid_format"
    INVALID_VALUE = "invalid_value"
    OUT_OF_RANGE = "out_of_range"
    
    # Business logic
    BUSINESS_RULE = "business_rule"
    CONSISTENCY = "consistency"
    CROSS_FIELD = "cross_field"
    STATE_TRANSITION = "state_transition"
    
    # Technical
    SYSTEM_ERROR = "system_error"
    DEPENDENCY = "dependency"
    PERFORMANCE = "performance"
    
    # Security
    AUTHORIZATION = "authorization"
    SECURITY = "security"
    
    # Other
    UNKNOWN = "unknown"


class ValidationIssue(BaseModel):
    """A single validation issue."""
    model_config = ConfigDict(frozen=True)
    
    severity: ValidationSeverity
    category: ValidationCategory
    field: str | None = None
    message: str
    code: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)
    suggestion: str | None = None
    documentation_url: str | None = None
    
    def is_error(self) -> bool:
        """Check if this is an error."""
        return self.severity in {ValidationSeverity.ERROR, ValidationSeverity.CRITICAL}
    
    def is_warning(self) -> bool:
        """Check if this is a warning."""
        return self.severity == ValidationSeverity.WARNING
    
    def to_string(self) -> str:
        """Convert to human-readable string."""
        parts = [f"[{self.severity.value.upper()}]"]
        if self.field:
            parts.append(f"Field: {self.field}")
        parts.append(self.message)
        if self.code:
            parts.append(f"(Code: {self.code})")
        return " ".join(parts)


class ValidationResult[T](BaseModel):
    """Result of validation operation."""
    model_config = ConfigDict(frozen=True)
    
    # Core fields
    is_valid: bool
    validated_data: T | None = None
    issues: list[ValidationIssue] = Field(default_factory=list)
    
    # Metadata
    validation_id: str = Field(default_factory=lambda: str(uuid4()))
    timestamp: float = Field(default_factory=time.time)
    validator_name: str | None = None
    validation_duration_ms: float | None = None
    
    # Statistics
    error_count: int = 0
    warning_count: int = 0
    info_count: int = 0
    
    def has_errors(self) -> bool:
        """Check if there are any errors."""
        return self.error_count > 0
    
    def has_warnings(self) -> bool:
        """Check if there are any warnings."""
        return self.warning_count > 0
    
    def get_errors(self) -> list[ValidationIssue]:
        """Get all error issues."""
        return [issue for issue in self.issues if issue.is_error()]
    
    def get_warnings(self) -> list[ValidationIssue]:
        """Get all warning issues."""
        return [issue for issue in self.issues if issue.is_warning()]
    
    def add_issue(self, issue: ValidationIssue) -> None:
        """Add a validation issue."""
        self.issues.append(issue)
        if issue.severity == ValidationSeverity.ERROR:
            self.error_count += 1
        elif issue.severity == ValidationSeverity.WARNING:
            self.warning_count += 1
        elif issue.severity == ValidationSeverity.INFO:
            self.info_count += 1
    
    def merge(self, other: ValidationResult[Any]) -> ValidationResult[T]:
        """Merge with another validation result."""
        return ValidationResult(
            is_valid=self.is_valid and other.is_valid,
            validated_data=self.validated_data,
            issues=self.issues + other.issues,
            error_count=self.error_count + other.error_count,
            warning_count=self.warning_count + other.warning_count,
            info_count=self.info_count + other.info_count,
        )


class ValidationChain[T](BaseModel):
    """Chain of validators to run sequentially."""
    validators: list[Any] = Field(default_factory=list)
    stop_on_error: bool = True
    
    async def validate(self, data: T) -> ValidationResult[T]:
        """Run all validators in sequence."""
        result = ValidationResult[T](is_valid=True, validated_data=data)
        
        for validator in self.validators:
            validator_result = await validator.validate(data)
            result = result.merge(validator_result)
            
            if not validator_result.is_valid and self.stop_on_error:
                break
        
        return result


class StateValidationResult(BaseModel):
    """Result of state validation."""
    is_valid: bool
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    metadata: StateValidationMetadata | dict[str, object] = Field(
        default_factory=StateValidationMetadata
    )
    
    @field_validator("metadata", mode="before")
    @classmethod
    def validate_metadata(
        cls, v: dict[str, object] | StateValidationMetadata
    ) -> StateValidationMetadata:
        """Convert dict to StateValidationMetadata if needed."""
        if isinstance(v, StateValidationMetadata):
            return v
        
        # Use Pydantic's model_validate for proper type handling
        return StateValidationMetadata.model_validate(v)


# ==================== Exception Context Models ====================

class ExceptionContext(BaseModel):
    """Typed context for exceptions."""
    model_config = ConfigDict(extra="allow", frozen=True)
    
    operation_id: str = ""
    correlation_id: str = ""
    user_id: str = ""
    session_id: str = ""
    request_id: str = ""
    timestamp: float = 0.0
    component: str = ""
    version: str = ""
    environment: str = ""
    tags: dict[str, str] = Field(default_factory=dict)


class StateSnapshotException(BaseModel):
    """Typed state snapshot for exceptions."""
    model_config = ConfigDict(extra="allow", frozen=True)
    
    balances: dict[str, float] = Field(default_factory=dict)
    positions: dict[str, float] = Field(default_factory=dict)
    orders: dict[str, str] = Field(default_factory=dict)
    configuration: dict[str, str] = Field(default_factory=dict)
    connections: dict[str, bool] = Field(default_factory=dict)
    timestamp: float = 0.0
    version: str = "1.0"


class CalculationInput(BaseModel):
    """Typed input data for calculations."""
    model_config = ConfigDict(extra="allow", frozen=True)
    
    parameters: dict[str, float] = Field(default_factory=dict)
    symbols: list[str] = Field(default_factory=list)
    time_range: dict[str, float] = Field(default_factory=dict)
    configuration: dict[str, str] = Field(default_factory=dict)
    validation_rules: dict[str, bool] = Field(default_factory=dict)


class CalculationResults(BaseModel):
    """Typed intermediate calculation results."""
    model_config = ConfigDict(extra="allow", frozen=True)
    
    step_results: dict[str, float] = Field(default_factory=dict)
    intermediate_values: dict[str, float] = Field(default_factory=dict)
    validation_results: dict[str, bool] = Field(default_factory=dict)
    metadata: dict[str, str] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)


class CurrentState(BaseModel):
    """Typed current state data."""
    model_config = ConfigDict(extra="allow", frozen=True)
    
    active_connections: dict[str, bool] = Field(default_factory=dict)
    pending_operations: dict[str, str] = Field(default_factory=dict)
    cache_state: dict[str, str] = Field(default_factory=dict)
    resource_usage: dict[str, float] = Field(default_factory=dict)
    health_metrics: dict[str, float] = Field(default_factory=dict)


# ==================== Error and Exception Types ====================

class PortfolioError(Exception):
    """Base portfolio exception."""
    pass


class CalculationError(PortfolioError):
    """Calculation error."""
    pass


class StateError(PortfolioError):
    """State management error."""
    pass


class ValidationError(PortfolioError):
    """Validation error."""
    pass


# ==================== Resilience Types ====================

class ResilienceErrorType(Enum):
    """Types of resilience errors."""
    TIMEOUT = "timeout"
    CIRCUIT_BREAKER_OPEN = "circuit_breaker_open"
    RATE_LIMITED = "rate_limited"
    RETRY_EXHAUSTED = "retry_exhausted"
    FALLBACK_FAILED = "fallback_failed"
    UNKNOWN = "unknown"


class ResilienceError(BaseModel):
    """Resilience error information."""
    model_config = ConfigDict(frozen=True)
    
    error_type: ResilienceErrorType
    message: str
    component: str | None = None
    operation: str | None = None
    retry_count: int = 0
    max_retries: int | None = None
    retry_after: float | None = None
    original_error: str | None = None
    timestamp: float = Field(default_factory=time.time)
    
    def is_retryable(self) -> bool:
        """Check if error is retryable."""
        return self.error_type not in {
            ResilienceErrorType.CIRCUIT_BREAKER_OPEN,
            ResilienceErrorType.RETRY_EXHAUSTED,
        }


class ResilienceMetrics(BaseModel):
    """Metrics for resilience operations."""
    model_config = ConfigDict(frozen=True)
    
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    timeouts: int = 0
    circuit_breaker_opens: int = 0
    fallback_successes: int = 0
    fallback_failures: int = 0
    average_response_time_ms: float = 0.0
    p95_response_time_ms: float = 0.0
    p99_response_time_ms: float = 0.0


class ResilienceResult[T](BaseModel):
    """Result of a resilient operation."""
    model_config = ConfigDict(frozen=True)
    
    success: bool
    value: T | None = None
    error: ResilienceError | None = None
    metrics: ResilienceMetrics
    used_fallback: bool = False
    duration_ms: float = 0.0


# ==================== Type Guards ====================

@runtime_checkable
class Sized(Protocol):
    """Protocol for sized objects."""
    def __len__(self) -> int: ...


def is_valid_exchange_id(value: Any) -> bool:
    """Check if exchange ID is valid."""
    return isinstance(value, str) and len(value) > 0 and value.isalnum()


def is_valid_symbol(value: Any) -> bool:
    """Check if symbol is valid."""
    if not isinstance(value, str):
        return False
    # Check for common symbol formats
    return "-" in value or "/" in value or (value.isupper() and len(value) >= 3)


def is_valid_order_id(value: Any) -> bool:
    """Check if order ID is valid."""
    return isinstance(value, str) and len(value) > 0


def is_valid_timestamp(value: Any) -> bool:
    """Check if timestamp is valid."""
    if isinstance(value, (int, float)):
        # Check if it's a reasonable Unix timestamp
        return 0 < value < 2**32
    return False


def is_valid_decimal_string(value: Any) -> bool:
    """Check if value is a valid decimal string."""
    if not isinstance(value, str):
        return False
    try:
        float(value)
        return True
    except ValueError:
        return False


def is_non_empty_string(value: Any) -> bool:
    """Check if value is a non-empty string."""
    return isinstance(value, str) and len(value.strip()) > 0


def is_positive_number(value: Any) -> bool:
    """Check if value is a positive number."""
    return isinstance(value, (int, float)) and value > 0


def is_non_negative_number(value: Any) -> bool:
    """Check if value is a non-negative number."""
    return isinstance(value, (int, float)) and value >= 0


def is_valid_percentage(value: Any) -> bool:
    """Check if value is a valid percentage (0-100)."""
    return isinstance(value, (int, float)) and 0 <= value <= 100


def is_valid_currency_code(value: Any) -> bool:
    """Check if value is a valid currency code."""
    return isinstance(value, str) and len(value) == 3 and value.isupper()


def is_sized_collection(value: Any, min_size: int = 0, max_size: int | None = None) -> bool:
    """Check if value is a sized collection within bounds."""
    if not isinstance(value, Sized):
        return False
    
    size = len(value)
    if size < min_size:
        return False
    if max_size is not None and size > max_size:
        return False
    
    return True


def validate_required_fields(data: dict[str, Any], required_fields: list[str]) -> list[str]:
    """Validate required fields are present and non-null."""
    missing_fields = []
    for field in required_fields:
        if field not in data or data[field] is None:
            missing_fields.append(field)
    return missing_fields


def validate_field_types(data: dict[str, Any], field_types: dict[str, type]) -> list[str]:
    """Validate field types match expected types."""
    type_errors = []
    for field, expected_type in field_types.items():
        if field in data and data[field] is not None:
            if not isinstance(data[field], expected_type):
                type_errors.append(f"{field} must be {expected_type.__name__}")
    return type_errors


# ==================== Event Handler Types ====================

class EventHandler[T](ABC):
    """Abstract base class for event handlers."""
    
    @abstractmethod
    async def handle(self, event: BasePortfolioEvent[T]) -> None:
        """Handle an event."""
        ...
    
    @abstractmethod
    def can_handle(self, event: BasePortfolioEvent[Any]) -> bool:
        """Check if handler can handle the event."""
        ...
    
    def get_handler_name(self) -> str:
        """Get handler name for logging."""
        return self.__class__.__name__


class EventFilter(ABC):
    """Abstract base class for event filters."""
    
    @abstractmethod
    def should_process(self, event: BasePortfolioEvent[Any]) -> bool:
        """Check if event should be processed."""
        ...


# ==================== Service Exception Context Models ====================

class ServiceExceptionContext(BaseModel):
    """Pydantic model for service exception context instead of TypedDict."""
    model_config = ConfigDict(extra="allow", frozen=True)
    
    error_code: str | None = Field(default=None, description="Service error code")
    context: ExceptionContext = Field(
        default_factory=ExceptionContext, description="Additional context"
    )
    recoverable: bool = Field(default=False, description="Whether error is recoverable")
    
    # Service-specific fields
    service_name: str | None = Field(default=None, description="Name of the service")
    operation: str | None = Field(default=None, description="Operation that failed")
    retry_after: int | None = Field(default=None, description="Retry delay in seconds")
    fallback_available: bool = Field(default=False, description="Whether fallback is available")
    
    # API-specific fields
    api_name: str | None = Field(default=None, description="API name")
    endpoint: str | None = Field(default=None, description="API endpoint")
    status_code: int | None = Field(default=None, description="HTTP status code")
    response_body: str | None = Field(default=None, description="API response body")
    
    # Cache-specific fields
    cache_key: str | None = Field(default=None, description="Cache key")
    cache_backend: str | None = Field(default=None, description="Cache backend type")
    
    # Trading-specific fields
    symbol: str | None = Field(default=None, description="Trading symbol")
    exchange: str | None = Field(default=None, description="Exchange name")
    price_type: str | None = Field(default=None, description="Price type")
    
    # Timing fields
    timeout_seconds: float | None = Field(default=None, description="Timeout duration")
    limit_type: str | None = Field(default=None, description="Rate limit type")
    
    # State-specific fields
    current_state: str | None = Field(default=None, description="Current state")
    expected_state: str | None = Field(default=None, description="Expected state")
    initialization_phase: str | None = Field(default=None, description="Initialization phase")
    cleanup_phase: str | None = Field(default=None, description="Cleanup phase")
    
    # Additional cause information
    cause: str | None = Field(default=None, description="Root cause description")
    service_type: str | None = Field(default=None, description="Type of service")


class IntegrityExceptionContext(BaseModel):
    """Pydantic model for integrity exception context."""
    model_config = ConfigDict(extra="allow", frozen=True)
    
    error_code: str | None = Field(default=None, description="Integrity error code")
    context: ExceptionContext = Field(
        default_factory=ExceptionContext, description="Additional context"
    )
    recoverable: bool = Field(default=False, description="Whether error is recoverable")
    
    # Integrity-specific fields
    entity_type: str | None = Field(default=None, description="Type of entity")
    entity_id: str | None = Field(default=None, description="Entity identifier")
    integrity_rule: str | None = Field(default=None, description="Violated integrity rule")
    expected_value: str | None = Field(default=None, description="Expected value")
    actual_value: str | None = Field(default=None, description="Actual value")
    
    # Validation fields
    field_name: str | None = Field(default=None, description="Field that failed validation")
    constraint: str | None = Field(default=None, description="Constraint that was violated")
    
    # State consistency fields
    inconsistent_fields: list[str] = Field(
        default_factory=list, description="Fields that are inconsistent"
    )
    state_snapshot: StateSnapshotException = Field(
        default_factory=StateSnapshotException, description="State at time of error"
    )


class CalculationExceptionContext(BaseModel):
    """Pydantic model for calculation exception context."""
    model_config = ConfigDict(extra="allow", frozen=True)
    
    error_code: str | None = Field(default=None, description="Calculation error code")
    context: ExceptionContext = Field(
        default_factory=ExceptionContext, description="Additional context"
    )
    recoverable: bool = Field(default=False, description="Whether error is recoverable")
    
    # Calculation-specific fields
    calculation_type: str | None = Field(default=None, description="Type of calculation")
    input_data: CalculationInput = Field(default_factory=CalculationInput, description="Input data")
    intermediate_results: CalculationResults = Field(
        default_factory=CalculationResults, description="Intermediate results"
    )
    
    # Math/numerical fields
    division_by_zero: bool = Field(default=False, description="Whether division by zero occurred")
    overflow: bool = Field(default=False, description="Whether numerical overflow occurred")
    underflow: bool = Field(default=False, description="Whether numerical underflow occurred")
    precision_loss: bool = Field(default=False, description="Whether precision was lost")
    
    # Financial calculation fields
    symbol: str | None = Field(default=None, description="Trading symbol")
    price: str | None = Field(default=None, description="Price value")
    quantity: str | None = Field(default=None, description="Quantity value")
    pnl_type: str | None = Field(default=None, description="PnL calculation type")


class StateExceptionContext(BaseModel):
    """Pydantic model for state exception context."""
    model_config = ConfigDict(extra="allow", frozen=True)
    
    error_code: str | None = Field(default=None, description="State error code")
    context: ExceptionContext = Field(
        default_factory=ExceptionContext, description="Additional context"
    )
    recoverable: bool = Field(default=False, description="Whether error is recoverable")
    
    # State management fields
    state_manager: str | None = Field(default=None, description="State manager name")
    state_type: str | None = Field(default=None, description="Type of state")
    current_state: CurrentState = Field(default_factory=CurrentState, description="Current state")
    attempted_transition: str | None = Field(default=None, description="Attempted state transition")
    
    # Validation fields
    validation_errors: list[str] = Field(default_factory=list, description="Validation errors")
    invalid_fields: list[str] = Field(default_factory=list, description="Invalid fields")
    
    # Concurrency fields
    lock_timeout: bool = Field(default=False, description="Whether lock timeout occurred")
    concurrent_modification: bool = Field(
        default=False, description="Whether concurrent modification detected"
    )
    
    # Entity fields
    entity_id: str | None = Field(default=None, description="Entity identifier")
    entity_type: str | None = Field(default=None, description="Entity type")
    
    # Persistence fields
    persistence_error: bool = Field(default=False, description="Whether persistence failed")
    rollback_successful: bool | None = Field(default=None, description="Whether rollback succeeded")


# ==================== Event Metadata Types ====================

class EventMetadataContext(BaseModel):
    """Pydantic model for event metadata context."""
    model_config = ConfigDict(extra="allow", frozen=True)
    
    # Core event fields
    event_id: str | None = Field(default=None, description="Event identifier")
    event_type: str | None = Field(default=None, description="Event type")
    timestamp: float | None = Field(default=None, description="Event timestamp")
    
    # Exchange context
    exchange: str | None = Field(default=None, description="Exchange name")
    symbol: str | None = Field(default=None, description="Trading symbol")
    
    # Additional metadata
    source: str | None = Field(default=None, description="Event source")
    correlation_id: str | None = Field(default=None, description="Correlation ID")
    trace_id: str | None = Field(default=None, description="Trace ID")
    user_id: str | None = Field(default=None, description="User ID")
    
    # Performance fields
    processing_duration_ms: float | None = Field(
        default=None, description="Processing duration in ms"
    )
    retries: int = Field(default=0, description="Number of retries")
    
    # Additional context
    extra_data: dict[str, Any] = Field(default_factory=dict, description="Additional data")