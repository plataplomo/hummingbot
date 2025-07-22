"""Pydantic models for exception contexts, replacing TypedDict usage."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


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


class StateSnapshot(BaseModel):
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


class ExtraData(BaseModel):
    """Typed extra data for exceptions."""

    model_config = ConfigDict(extra="allow", frozen=True)

    debug_info: dict[str, str] = Field(default_factory=dict)
    performance_metrics: dict[str, float] = Field(default_factory=dict)
    system_info: dict[str, str] = Field(default_factory=dict)
    runtime_data: dict[str, str] = Field(default_factory=dict)
    external_references: dict[str, str] = Field(default_factory=dict)


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
    state_snapshot: StateSnapshot = Field(
        default_factory=StateSnapshot, description="State at time of error"
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
    extra_data: ExtraData = Field(default_factory=ExtraData, description="Additional data")
