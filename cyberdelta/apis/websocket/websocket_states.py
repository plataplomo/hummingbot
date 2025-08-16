"""WebSocket state enums for the API layer.

This module contains enums that define various states and results
for WebSocket connections and message processing.
"""

from enum import Enum

from cyberdelta.apis.enums.websocket import DataPresenceState, FieldPresenceState


class CancellationState(Enum):
    """WebSocket cancellation state for connection management.

    Replaces boolean `was_cancelled` and `is_cancelled` parameters with explicit states.
    """

    ACTIVE = "active"
    """Connection is active and running (was was_cancelled=False)."""

    CANCELLED = "cancelled"
    """Connection was cancelled (was was_cancelled=True)."""

    TERMINATED = "terminated"
    """Connection terminated normally without cancellation."""

    FAILED = "failed"
    """Connection failed due to error."""

    @property
    def allows_reconnection(self) -> bool:
        """Check if this state allows reconnection attempts."""
        return self in {CancellationState.TERMINATED, CancellationState.FAILED}

    @property
    def is_cancelled(self) -> bool:
        """Check if this represents a cancelled state."""
        return self == CancellationState.CANCELLED


class MessageProcessingResult(Enum):
    """Message processing result for WebSocket metrics.

    Replaces the boolean `success` parameter.
    """

    SUCCESS = "success"
    """Message processing succeeded (was success=True)."""

    FAILURE = "failure"
    """Message processing failed (was success=False)."""

    PARTIAL = "partial"
    """Message processing partially succeeded."""

    TIMEOUT = "timeout"
    """Message processing timed out."""

    @property
    def is_successful(self) -> bool:
        """Check if processing was successful."""
        return self == MessageProcessingResult.SUCCESS


class OperationResult(Enum):
    """General operation result for various operations.

    Replaces the boolean `success` parameter in performance tracking and metrics.
    """

    SUCCESS = "success"
    """Operation completed successfully (was success=True)."""

    FAILURE = "failure"
    """Operation failed (was success=False)."""

    PARTIAL = "partial"
    """Operation partially succeeded."""

    TIMEOUT = "timeout"
    """Operation timed out."""

    SKIPPED = "skipped"
    """Operation was skipped."""

    @property
    def is_successful(self) -> bool:
        """Check if operation was successful."""
        return self == OperationResult.SUCCESS

    @property
    def is_failure(self) -> bool:
        """Check if operation failed."""
        return self in {OperationResult.FAILURE, OperationResult.TIMEOUT}


# DataPresenceState and FieldPresenceState moved to cyberdelta.enums.data_states
# to avoid circular imports with base validation contexts


__all__ = [
    "CancellationState",
    "DataPresenceState",
    "FieldPresenceState",
    "MessageProcessingResult",
    "OperationResult",
]
