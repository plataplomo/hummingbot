"""Error and system events for portfolio management."""

from __future__ import annotations

from typing import Any, Unpack

from pydantic import BaseModel, Field, field_validator
from pydantic.dataclasses import dataclass

from cyberdelta.core.portfolio.events.base.base_event import (
    BasePortfolioEvent,
    EventMetadata,
    EventMetadataKwargs,
    EventPriority,
    EventType,
)
from cyberdelta.core.portfolio.exceptions import MalformedTradeError


class ErrorContext(BaseModel):
    """Structured error context information."""

    operation: str | None = Field(default=None, description="Operation being performed")
    user_id: str | None = Field(default=None, description="User ID if applicable")
    session_id: str | None = Field(default=None, description="Session ID if applicable")
    request_id: str | None = Field(default=None, description="Request ID if applicable")
    retry_attempt: int | None = Field(default=None, ge=0, description="Current retry attempt")
    additional_info: dict[str, str] = Field(
        default_factory=dict, description="Additional string info"
    )


class ComponentMetadata(BaseModel):
    """Structured component metadata."""

    version: str | None = Field(default=None, description="Component version")
    environment: str | None = Field(default=None, description="Environment (prod/dev/test)")
    startup_time: float | None = Field(
        default=None, gt=0, description="Component startup timestamp"
    )
    uptime_seconds: float | None = Field(
        default=None, ge=0, description="Component uptime in seconds"
    )
    config_hash: str | None = Field(default=None, description="Configuration hash")
    additional_info: dict[str, str] = Field(
        default_factory=dict, description="Additional string info"
    )


@dataclass
class ErrorData:
    """Standard error data structure."""

    component: str
    error_type: str
    error_message: str
    error_code: str | None = None
    stack_trace: str | None = None
    context: ErrorContext | None = None
    recoverable: bool = True
    retry_count: int = 0
    max_retries: int = 3

    @field_validator("component", "error_type", "error_message", mode="before")
    @classmethod
    def validate_required_strings(cls, v: str) -> str:
        """Validate required string fields are non-empty."""
        if not v or not v.strip():
            raise MalformedTradeError(
                message="Required string fields cannot be empty",
                field_name="required_string",
                field_value="",
            )
        return v.strip()

    @field_validator("error_code", mode="before")
    @classmethod
    def validate_error_code(cls, v: str | None) -> str | None:
        """Validate optional error code field."""
        if v is not None and not v.strip():
            return None
        return v

    @field_validator("retry_count", mode="before")
    @classmethod
    def validate_retry_count(cls, v: int) -> int:
        """Validate retry count is non-negative."""
        if v < 0:
            raise MalformedTradeError(
                message="Retry count cannot be negative",
                field_name="retry_count",
                field_value=str(v),
            )
        return v

    @field_validator("max_retries", mode="before")
    @classmethod
    def validate_max_retries(cls, v: int) -> int:
        """Validate max retries is positive."""
        if v <= 0:
            raise MalformedTradeError(
                message="Max retries must be positive", field_name="max_retries", field_value=str(v)
            )
        return v


@dataclass
class ComponentStateData:
    """Component state change data."""

    component_name: str
    component_type: str
    new_state: str
    previous_state: str | None = None
    metadata: ComponentMetadata | None = None

    @field_validator("component_name", "component_type", "new_state", mode="before")
    @classmethod
    def validate_required_strings(cls, v: str) -> str:
        """Validate required string fields are non-empty."""
        if not v or not v.strip():
            raise MalformedTradeError(
                message="Required string fields cannot be empty",
                field_name="required_string",
                field_value="",
            )
        return v.strip()

    @field_validator("previous_state", mode="before")
    @classmethod
    def validate_previous_state(cls, v: str | None) -> str | None:
        """Validate optional previous state field."""
        if v is not None and not v.strip():
            return None
        return v


@dataclass
class StateSnapshotData:
    """State snapshot data."""

    snapshot_id: str
    component: str
    state_data: dict[str, Any]  # Keep as Any for flexibility with state data
    snapshot_type: str  # "full", "incremental", "checkpoint"
    size_bytes: int
    checksum: str | None = None

    @field_validator("snapshot_id", "component", mode="before")
    @classmethod
    def validate_required_strings(cls, v: str) -> str:
        """Validate required string fields are non-empty."""
        if not v or not v.strip():
            raise MalformedTradeError(
                message="Required string fields cannot be empty",
                field_name="required_string",
                field_value="",
            )
        return v.strip()

    @field_validator("snapshot_type", mode="before")
    @classmethod
    def validate_snapshot_type(cls, v: str) -> str:
        """Validate snapshot type is valid."""
        valid_types = {"full", "incremental", "checkpoint"}
        if v.lower() not in valid_types:
            raise MalformedTradeError(
                message="Invalid snapshot type", field_name="snapshot_type", field_value=v
            )
        return v.lower()

    @field_validator("size_bytes", mode="before")
    @classmethod
    def validate_size_bytes(cls, v: int) -> int:
        """Validate size bytes is non-negative."""
        if v < 0:
            raise MalformedTradeError(
                message="Size bytes cannot be negative", field_name="size_bytes", field_value=str(v)
            )
        return v

    @field_validator("checksum", mode="before")
    @classmethod
    def validate_checksum(cls, v: str | None) -> str | None:
        """Validate optional checksum field."""
        if v is not None and not v.strip():
            return None
        return v


@dataclass
class ErrorOccurredEvent(BasePortfolioEvent[ErrorData]):
    """Event fired when an error occurs."""

    @classmethod
    def create(
        cls,
        error: ErrorData,
        severity: EventPriority = EventPriority.HIGH,
        **kwargs: Unpack[EventMetadataKwargs],
    ) -> ErrorOccurredEvent:
        """Create an error occurred event with proper initialization.

        Args:
            error: Error details
            severity: Error severity level
            **kwargs: Additional metadata fields
        """
        # Build metadata with explicit fields first
        metadata = EventMetadata(priority=severity, source_component=error.component)

        # Apply additional fields from kwargs
        if "correlation_id" in kwargs:
            metadata.correlation_id = kwargs["correlation_id"]
        if "exchange_id" in kwargs:
            metadata.exchange_id = kwargs["exchange_id"]
        if "symbol" in kwargs:
            metadata.symbol = kwargs["symbol"]
        if "retry_count" in kwargs:
            metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            metadata.tags.update(kwargs["tags"])

        # Set standard tags
        metadata.tags["error_type"] = error.error_type
        metadata.tags["recoverable"] = str(error.recoverable)

        if error.error_code:
            metadata.tags["error_code"] = error.error_code

        return cls(event_type=EventType.ERROR_OCCURRED, data=error, metadata=metadata)

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize error data."""
        return {
            "component": self.data.component,
            "error_type": self.data.error_type,
            "error_message": self.data.error_message,
            "error_code": self.data.error_code,
            "stack_trace": self.data.stack_trace,
            "context": self.data.context.model_dump() if self.data.context else {},
            "recoverable": self.data.recoverable,
            "retry_count": self.data.retry_count,
            "max_retries": self.data.max_retries,
        }


@dataclass
class ErrorRecoveredEvent(BasePortfolioEvent[ErrorData]):
    """Event fired when an error is recovered from."""

    @classmethod
    def create(
        cls,
        error: ErrorData,
        recovery_method: str,
        recovery_duration_ms: float | None = None,
        **kwargs: Unpack[EventMetadataKwargs],
    ) -> ErrorRecoveredEvent:
        """Create an error recovered event with proper initialization.

        Args:
            error: Original error that was recovered
            recovery_method: How the error was recovered
            recovery_duration_ms: Time taken to recover
            **kwargs: Additional metadata fields
        """
        # Build metadata with explicit fields first
        metadata = EventMetadata(source_component=error.component)

        # Apply additional fields from kwargs
        if "correlation_id" in kwargs:
            metadata.correlation_id = kwargs["correlation_id"]
        if "exchange_id" in kwargs:
            metadata.exchange_id = kwargs["exchange_id"]
        if "symbol" in kwargs:
            metadata.symbol = kwargs["symbol"]
        if "priority" in kwargs:
            metadata.priority = kwargs["priority"]
        if "retry_count" in kwargs:
            metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            metadata.tags.update(kwargs["tags"])

        # Set standard tags
        metadata.tags["error_type"] = error.error_type
        metadata.tags["recovery_method"] = recovery_method

        if recovery_duration_ms is not None:
            metadata.tags["recovery_duration_ms"] = str(recovery_duration_ms)

        return cls(event_type=EventType.ERROR_RECOVERED, data=error, metadata=metadata)

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize error data."""
        return {
            "component": self.data.component,
            "error_type": self.data.error_type,
            "error_message": self.data.error_message,
            "error_code": self.data.error_code,
            "retry_count": self.data.retry_count,
        }


@dataclass
class ComponentInitializedEvent(BasePortfolioEvent[ComponentStateData]):
    """Event fired when a component is initialized."""

    @classmethod
    def create(
        cls, component_state: ComponentStateData, **kwargs: Unpack[EventMetadataKwargs]
    ) -> ComponentInitializedEvent:
        """Create a component initialized event with proper initialization.

        Args:
            component_state: Component state data
            **kwargs: Additional metadata fields
        """
        # Build metadata with explicit fields first
        metadata = EventMetadata(source_component=component_state.component_name)

        # Apply additional fields from kwargs
        if "correlation_id" in kwargs:
            metadata.correlation_id = kwargs["correlation_id"]
        if "exchange_id" in kwargs:
            metadata.exchange_id = kwargs["exchange_id"]
        if "symbol" in kwargs:
            metadata.symbol = kwargs["symbol"]
        if "priority" in kwargs:
            metadata.priority = kwargs["priority"]
        if "retry_count" in kwargs:
            metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            metadata.tags.update(kwargs["tags"])

        # Set standard tags
        metadata.tags["component_type"] = component_state.component_type

        return cls(
            event_type=EventType.COMPONENT_INITIALIZED, data=component_state, metadata=metadata
        )

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize component state data."""
        return {
            "component_name": self.data.component_name,
            "component_type": self.data.component_type,
            "previous_state": self.data.previous_state,
            "new_state": self.data.new_state,
            "metadata": self.data.metadata.model_dump() if self.data.metadata else {},
        }


@dataclass
class ComponentShutdownEvent(BasePortfolioEvent[ComponentStateData]):
    """Event fired when a component is shut down."""

    @classmethod
    def create(
        cls,
        component_state: ComponentStateData,
        shutdown_reason: str | None = None,
        **kwargs: Unpack[EventMetadataKwargs],
    ) -> ComponentShutdownEvent:
        """Create a component shutdown event with proper initialization.

        Args:
            component_state: Component state data
            shutdown_reason: Reason for shutdown
            **kwargs: Additional metadata fields
        """
        # Build metadata with explicit fields first
        metadata = EventMetadata(source_component=component_state.component_name)

        # Apply additional fields from kwargs
        if "correlation_id" in kwargs:
            metadata.correlation_id = kwargs["correlation_id"]
        if "exchange_id" in kwargs:
            metadata.exchange_id = kwargs["exchange_id"]
        if "symbol" in kwargs:
            metadata.symbol = kwargs["symbol"]
        if "priority" in kwargs:
            metadata.priority = kwargs["priority"]
        if "retry_count" in kwargs:
            metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            metadata.tags.update(kwargs["tags"])

        # Set standard tags
        metadata.tags["component_type"] = component_state.component_type

        if shutdown_reason:
            metadata.tags["shutdown_reason"] = shutdown_reason

        return cls(event_type=EventType.COMPONENT_SHUTDOWN, data=component_state, metadata=metadata)

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize component state data."""
        return {
            "component_name": self.data.component_name,
            "component_type": self.data.component_type,
            "previous_state": self.data.previous_state,
            "new_state": self.data.new_state,
            "metadata": self.data.metadata.model_dump() if self.data.metadata else {},
        }


@dataclass
class StateSnapshotCreatedEvent(BasePortfolioEvent[StateSnapshotData]):
    """Event fired when a state snapshot is created."""

    @classmethod
    def create(
        cls, snapshot: StateSnapshotData, **kwargs: Unpack[EventMetadataKwargs]
    ) -> StateSnapshotCreatedEvent:
        """Create a state snapshot created event with proper initialization.

        Args:
            snapshot: Snapshot data
            **kwargs: Additional metadata fields
        """
        # Build metadata with explicit fields first
        metadata = EventMetadata(source_component=snapshot.component)

        # Apply additional fields from kwargs
        if "correlation_id" in kwargs:
            metadata.correlation_id = kwargs["correlation_id"]
        if "exchange_id" in kwargs:
            metadata.exchange_id = kwargs["exchange_id"]
        if "symbol" in kwargs:
            metadata.symbol = kwargs["symbol"]
        if "priority" in kwargs:
            metadata.priority = kwargs["priority"]
        if "retry_count" in kwargs:
            metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            metadata.tags.update(kwargs["tags"])

        # Set standard tags
        metadata.tags["snapshot_id"] = snapshot.snapshot_id
        metadata.tags["snapshot_type"] = snapshot.snapshot_type
        metadata.tags["size_bytes"] = str(snapshot.size_bytes)

        return cls(event_type=EventType.STATE_SNAPSHOT_CREATED, data=snapshot, metadata=metadata)

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize snapshot data."""
        return {
            "snapshot_id": self.data.snapshot_id,
            "component": self.data.component,
            "snapshot_type": self.data.snapshot_type,
            "size_bytes": self.data.size_bytes,
            "checksum": self.data.checksum,
            # Don't serialize full state data to avoid bloat
            "state_keys": list(self.data.state_data.keys()),
        }


@dataclass
class StateRestoredEvent(BasePortfolioEvent[StateSnapshotData]):
    """Event fired when state is restored from snapshot."""

    @classmethod
    def create(
        cls,
        snapshot: StateSnapshotData,
        restore_duration_ms: float | None = None,
        **kwargs: Unpack[EventMetadataKwargs],
    ) -> StateRestoredEvent:
        """Create a state restored event with proper initialization.

        Args:
            snapshot: Snapshot that was restored
            restore_duration_ms: Time taken to restore
            **kwargs: Additional metadata fields
        """
        # Build metadata with explicit fields first
        metadata = EventMetadata(source_component=snapshot.component)

        # Apply additional fields from kwargs
        if "correlation_id" in kwargs:
            metadata.correlation_id = kwargs["correlation_id"]
        if "exchange_id" in kwargs:
            metadata.exchange_id = kwargs["exchange_id"]
        if "symbol" in kwargs:
            metadata.symbol = kwargs["symbol"]
        if "priority" in kwargs:
            metadata.priority = kwargs["priority"]
        if "retry_count" in kwargs:
            metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            metadata.tags.update(kwargs["tags"])

        # Set standard tags
        metadata.tags["snapshot_id"] = snapshot.snapshot_id
        metadata.tags["snapshot_type"] = snapshot.snapshot_type

        if restore_duration_ms is not None:
            metadata.tags["restore_duration_ms"] = str(restore_duration_ms)

        return cls(event_type=EventType.STATE_RESTORED, data=snapshot, metadata=metadata)

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize snapshot data."""
        return {
            "snapshot_id": self.data.snapshot_id,
            "component": self.data.component,
            "snapshot_type": self.data.snapshot_type,
            "size_bytes": self.data.size_bytes,
            "checksum": self.data.checksum,
        }
