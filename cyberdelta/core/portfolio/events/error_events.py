"""Error and system events for portfolio management."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Unpack

from cyberdelta.core.portfolio.events.base.base_event import (
    BasePortfolioEvent,
    EventMetadataKwargs,
    EventPriority,
    EventType,
)


@dataclass
class ErrorData:
    """Standard error data structure."""

    component: str
    error_type: str
    error_message: str
    error_code: str | None = None
    stack_trace: str | None = None
    context: dict[str, Any] | None = None
    recoverable: bool = True
    retry_count: int = 0
    max_retries: int = 3


@dataclass
class ComponentStateData:
    """Component state change data."""

    component_name: str
    component_type: str
    previous_state: str | None
    new_state: str
    metadata: dict[str, Any] | None = None


@dataclass
class StateSnapshotData:
    """State snapshot data."""

    snapshot_id: str
    component: str
    state_data: dict[str, Any]
    snapshot_type: str  # "full", "incremental", "checkpoint"
    size_bytes: int
    checksum: str | None = None


@dataclass
class ErrorOccurredEvent(BasePortfolioEvent[ErrorData]):
    """Event fired when an error occurs."""

    def __init__(
        self,
        error: ErrorData,
        severity: EventPriority = EventPriority.HIGH,
        **kwargs: Unpack[EventMetadataKwargs],
    ) -> None:
        """Initialize error occurred event.

        Args:
            error: Error details
            severity: Error severity level
            **kwargs: Additional metadata fields
        """
        super().__init__(
            event_type=EventType.ERROR_OCCURRED,
            data=error,
        )

        self.metadata.priority = severity
        self.metadata.source_component = error.component
        self.metadata.tags["error_type"] = error.error_type
        self.metadata.tags["recoverable"] = str(error.recoverable)

        if error.error_code:
            self.metadata.tags["error_code"] = error.error_code

        # Apply any additional metadata using typed fields
        if "source_component" in kwargs:
            self.metadata.source_component = kwargs["source_component"]
        if "correlation_id" in kwargs:
            self.metadata.correlation_id = kwargs["correlation_id"]
        if "exchange_id" in kwargs:
            self.metadata.exchange_id = kwargs["exchange_id"]
        if "symbol" in kwargs:
            self.metadata.symbol = kwargs["symbol"]
        if "priority" in kwargs:
            self.metadata.priority = kwargs["priority"]
        if "retry_count" in kwargs:
            self.metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            self.metadata.tags.update(kwargs["tags"])

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize error data."""
        return {
            "component": self.data.component,
            "error_type": self.data.error_type,
            "error_message": self.data.error_message,
            "error_code": self.data.error_code,
            "stack_trace": self.data.stack_trace,
            "context": self.data.context or {},
            "recoverable": self.data.recoverable,
            "retry_count": self.data.retry_count,
            "max_retries": self.data.max_retries,
        }


@dataclass
class ErrorRecoveredEvent(BasePortfolioEvent[ErrorData]):
    """Event fired when an error is recovered from."""

    def __init__(
        self,
        error: ErrorData,
        recovery_method: str,
        recovery_duration_ms: float | None = None,
        **kwargs: Unpack[EventMetadataKwargs],
    ) -> None:
        """Initialize error recovered event.

        Args:
            error: Original error that was recovered
            recovery_method: How the error was recovered
            recovery_duration_ms: Time taken to recover
            **kwargs: Additional metadata fields
        """
        super().__init__(
            event_type=EventType.ERROR_RECOVERED,
            data=error,
        )

        self.metadata.source_component = error.component
        self.metadata.tags["error_type"] = error.error_type
        self.metadata.tags["recovery_method"] = recovery_method

        if recovery_duration_ms is not None:
            self.metadata.tags["recovery_duration_ms"] = str(recovery_duration_ms)

        # Apply any additional metadata using typed fields
        if "source_component" in kwargs:
            self.metadata.source_component = kwargs["source_component"]
        if "correlation_id" in kwargs:
            self.metadata.correlation_id = kwargs["correlation_id"]
        if "exchange_id" in kwargs:
            self.metadata.exchange_id = kwargs["exchange_id"]
        if "symbol" in kwargs:
            self.metadata.symbol = kwargs["symbol"]
        if "priority" in kwargs:
            self.metadata.priority = kwargs["priority"]
        if "retry_count" in kwargs:
            self.metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            self.metadata.tags.update(kwargs["tags"])

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

    def __init__(
        self, component_state: ComponentStateData, **kwargs: Unpack[EventMetadataKwargs]
    ) -> None:
        """Initialize component initialized event.

        Args:
            component_state: Component state data
            **kwargs: Additional metadata fields
        """
        super().__init__(
            event_type=EventType.COMPONENT_INITIALIZED,
            data=component_state,
        )

        self.metadata.source_component = component_state.component_name
        self.metadata.tags["component_type"] = component_state.component_type

        # Apply any additional metadata using typed fields
        if "source_component" in kwargs:
            self.metadata.source_component = kwargs["source_component"]
        if "correlation_id" in kwargs:
            self.metadata.correlation_id = kwargs["correlation_id"]
        if "exchange_id" in kwargs:
            self.metadata.exchange_id = kwargs["exchange_id"]
        if "symbol" in kwargs:
            self.metadata.symbol = kwargs["symbol"]
        if "priority" in kwargs:
            self.metadata.priority = kwargs["priority"]
        if "retry_count" in kwargs:
            self.metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            self.metadata.tags.update(kwargs["tags"])

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize component state data."""
        return {
            "component_name": self.data.component_name,
            "component_type": self.data.component_type,
            "previous_state": self.data.previous_state,
            "new_state": self.data.new_state,
            "metadata": self.data.metadata or {},
        }


@dataclass
class ComponentShutdownEvent(BasePortfolioEvent[ComponentStateData]):
    """Event fired when a component is shut down."""

    def __init__(
        self,
        component_state: ComponentStateData,
        shutdown_reason: str | None = None,
        **kwargs: Unpack[EventMetadataKwargs],
    ) -> None:
        """Initialize component shutdown event.

        Args:
            component_state: Component state data
            shutdown_reason: Reason for shutdown
            **kwargs: Additional metadata fields
        """
        super().__init__(
            event_type=EventType.COMPONENT_SHUTDOWN,
            data=component_state,
        )

        self.metadata.source_component = component_state.component_name
        self.metadata.tags["component_type"] = component_state.component_type

        if shutdown_reason:
            self.metadata.tags["shutdown_reason"] = shutdown_reason

        # Apply any additional metadata using typed fields
        if "source_component" in kwargs:
            self.metadata.source_component = kwargs["source_component"]
        if "correlation_id" in kwargs:
            self.metadata.correlation_id = kwargs["correlation_id"]
        if "exchange_id" in kwargs:
            self.metadata.exchange_id = kwargs["exchange_id"]
        if "symbol" in kwargs:
            self.metadata.symbol = kwargs["symbol"]
        if "priority" in kwargs:
            self.metadata.priority = kwargs["priority"]
        if "retry_count" in kwargs:
            self.metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            self.metadata.tags.update(kwargs["tags"])

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize component state data."""
        return {
            "component_name": self.data.component_name,
            "component_type": self.data.component_type,
            "previous_state": self.data.previous_state,
            "new_state": self.data.new_state,
            "metadata": self.data.metadata or {},
        }


@dataclass
class StateSnapshotCreatedEvent(BasePortfolioEvent[StateSnapshotData]):
    """Event fired when a state snapshot is created."""

    def __init__(self, snapshot: StateSnapshotData, **kwargs: Unpack[EventMetadataKwargs]) -> None:
        """Initialize state snapshot created event.

        Args:
            snapshot: Snapshot data
            **kwargs: Additional metadata fields
        """
        super().__init__(
            event_type=EventType.STATE_SNAPSHOT_CREATED,
            data=snapshot,
        )

        self.metadata.source_component = snapshot.component
        self.metadata.tags["snapshot_id"] = snapshot.snapshot_id
        self.metadata.tags["snapshot_type"] = snapshot.snapshot_type
        self.metadata.tags["size_bytes"] = str(snapshot.size_bytes)

        # Apply any additional metadata using typed fields
        if "source_component" in kwargs:
            self.metadata.source_component = kwargs["source_component"]
        if "correlation_id" in kwargs:
            self.metadata.correlation_id = kwargs["correlation_id"]
        if "exchange_id" in kwargs:
            self.metadata.exchange_id = kwargs["exchange_id"]
        if "symbol" in kwargs:
            self.metadata.symbol = kwargs["symbol"]
        if "priority" in kwargs:
            self.metadata.priority = kwargs["priority"]
        if "retry_count" in kwargs:
            self.metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            self.metadata.tags.update(kwargs["tags"])

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

    def __init__(
        self,
        snapshot: StateSnapshotData,
        restore_duration_ms: float | None = None,
        **kwargs: Unpack[EventMetadataKwargs],
    ) -> None:
        """Initialize state restored event.

        Args:
            snapshot: Snapshot that was restored
            restore_duration_ms: Time taken to restore
            **kwargs: Additional metadata fields
        """
        super().__init__(
            event_type=EventType.STATE_RESTORED,
            data=snapshot,
        )

        self.metadata.source_component = snapshot.component
        self.metadata.tags["snapshot_id"] = snapshot.snapshot_id
        self.metadata.tags["snapshot_type"] = snapshot.snapshot_type

        if restore_duration_ms is not None:
            self.metadata.tags["restore_duration_ms"] = str(restore_duration_ms)

        # Apply any additional metadata using typed fields
        if "source_component" in kwargs:
            self.metadata.source_component = kwargs["source_component"]
        if "correlation_id" in kwargs:
            self.metadata.correlation_id = kwargs["correlation_id"]
        if "exchange_id" in kwargs:
            self.metadata.exchange_id = kwargs["exchange_id"]
        if "symbol" in kwargs:
            self.metadata.symbol = kwargs["symbol"]
        if "priority" in kwargs:
            self.metadata.priority = kwargs["priority"]
        if "retry_count" in kwargs:
            self.metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            self.metadata.tags.update(kwargs["tags"])

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize snapshot data."""
        return {
            "snapshot_id": self.data.snapshot_id,
            "component": self.data.component,
            "snapshot_type": self.data.snapshot_type,
            "size_bytes": self.data.size_bytes,
            "checksum": self.data.checksum,
        }
