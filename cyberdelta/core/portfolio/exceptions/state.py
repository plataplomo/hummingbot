"""State management exceptions for portfolio management."""

from __future__ import annotations

from typing import Any, TypedDict, TypeVar, Unpack

from cyberdelta.core.portfolio.exceptions.base import PortfolioError


# Type variables for generic error/value types
T = TypeVar("T", bound=object)
E = TypeVar("E", bound=object)


class StateExceptionKwargs(TypedDict, total=False):
    """Type definition for state exception keyword arguments."""

    error_code: str | None
    context: dict[str, Any] | None
    recoverable: bool


class StateError(PortfolioError):
    """Base exception for state management errors."""

    def _get_default_error_code(self) -> str:
        """Get default error code for state exceptions."""
        return f"STATE_{self.__class__.__name__.upper()}"


class StateNotInitializedError(StateError):
    """Raised when accessing uninitialized state."""

    def __init__(
        self,
        message: str,
        component: str | None = None,
        required_state: str | None = None,
        **kwargs: Unpack[StateExceptionKwargs],
    ) -> None:
        """Initialize state not initialized exception.

        Args:
            message: Error message
            component: Component name
            required_state: Required state that's missing
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "component": component,
            "required_state": required_state,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "STATE_NOT_INITIALIZED"
        super().__init__(message, **kwargs)


class StateSynchronizationError(StateError):
    """Raised when state synchronization fails."""

    def __init__(
        self,
        message: str,
        source: str | None = None,
        target: str | None = None,
        sync_type: str | None = None,
        **kwargs: Unpack[StateExceptionKwargs],
    ) -> None:
        """Initialize state synchronization exception.

        Args:
            message: Error message
            source: Source of synchronization
            target: Target of synchronization
            sync_type: Type of synchronization
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "source": source,
            "target": target,
            "sync_type": sync_type,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "STATE_SYNC_FAILED"
        super().__init__(message, **kwargs)


class StateCorruptionError(StateError):
    """Raised when state corruption is detected."""

    def __init__(
        self,
        message: str,
        component: str | None = None,
        corruption_type: str | None = None,
        recovery_possible: bool = False,
        **kwargs: Unpack[StateExceptionKwargs],
    ) -> None:
        """Initialize state corruption exception.

        Args:
            message: Error message
            component: Component with corrupted state
            corruption_type: Type of corruption
            recovery_possible: Whether recovery is possible
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "component": component,
            "corruption_type": corruption_type,
            "recovery_possible": recovery_possible,
        })
        kwargs["context"] = context
        kwargs["recoverable"] = recovery_possible
        kwargs["error_code"] = "STATE_CORRUPTED"
        super().__init__(message, **kwargs)


class StatePersistenceError(StateError):
    """Raised when state persistence fails."""

    def __init__(
        self,
        message: str,
        operation: str | None = None,
        state_id: str | None = None,
        storage_backend: str | None = None,
        **kwargs: Unpack[StateExceptionKwargs],
    ) -> None:
        """Initialize state persistence exception.

        Args:
            message: Error message
            operation: Persistence operation (save/load/delete)
            state_id: State identifier
            storage_backend: Storage backend type
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "operation": operation,
            "state_id": state_id,
            "storage_backend": storage_backend,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "STATE_PERSISTENCE_FAILED"
        super().__init__(message, **kwargs)


class ConcurrencyError(StateError):
    """Raised when concurrency issues occur."""

    def __init__(
        self,
        message: str,
        lock_type: str | None = None,
        resource: str | None = None,
        timeout: float | None = None,
        **kwargs: Unpack[StateExceptionKwargs],
    ) -> None:
        """Initialize concurrency exception.

        Args:
            message: Error message
            lock_type: Type of lock
            resource: Resource being locked
            timeout: Lock timeout if applicable
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "lock_type": lock_type,
            "resource": resource,
            "timeout": timeout,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "STATE_CONCURRENCY_ERROR"
        super().__init__(message, **kwargs)


class DeadlockError(ConcurrencyError):
    """Raised when deadlock is detected."""

    def __init__(
        self,
        message: str = "Deadlock detected",
        resources: list[str] | None = None,
        threads: list[str] | None = None,
        **kwargs: Unpack[StateExceptionKwargs],
    ) -> None:
        """Initialize deadlock exception.

        Args:
            message: Error message
            resources: Resources involved in deadlock
            threads: Thread/task identifiers
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "resources": resources or [],
            "threads": threads or [],
        })
        kwargs["context"] = context
        kwargs["recoverable"] = False  # Deadlocks are critical
        kwargs["error_code"] = "STATE_DEADLOCK"
        super().__init__(message, **kwargs)


class StateTransitionError(StateError):
    """Raised when state transition is invalid."""

    def __init__(
        self,
        message: str,
        current_state: str | None = None,
        target_state: str | None = None,
        allowed_transitions: list[str] | None = None,
        **kwargs: Unpack[StateExceptionKwargs],
    ) -> None:
        """Initialize state transition exception.

        Args:
            message: Error message
            current_state: Current state
            target_state: Target state attempted
            allowed_transitions: List of allowed transitions
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "current_state": current_state,
            "target_state": target_state,
            "allowed_transitions": allowed_transitions or [],
        })
        kwargs["context"] = context
        kwargs["error_code"] = "STATE_TRANSITION_INVALID"
        super().__init__(message, **kwargs)


class StateValidationError(StateError):
    """Raised when state validation fails."""

    def __init__(
        self,
        message: str,
        validation_errors: list[str] | None = None,
        component: str | None = None,
        **kwargs: Unpack[StateExceptionKwargs],
    ) -> None:
        """Initialize state validation exception.

        Args:
            message: Error message
            validation_errors: List of validation errors
            component: Component with validation failure
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "validation_errors": validation_errors or [],
            "component": component,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "STATE_VALIDATION_FAILED"
        super().__init__(message, **kwargs)


class StateManagerNotInitializedError(StateValidationError):
    """Raised when portfolio state manager is not initialized."""

    def __init__(self, **kwargs: Unpack[StateExceptionKwargs]) -> None:
        """Initialize state manager not initialized exception."""
        super().__init__(
            "Portfolio state manager not initialized", component="PortfolioStateManager", **kwargs
        )


class StateManagerInitializationFailedError(StateValidationError):
    """Raised when portfolio state manager initialization fails."""

    def __init__(self, **kwargs: Unpack[StateExceptionKwargs]) -> None:
        """Initialize state manager initialization exception."""
        super().__init__("Initialization failed", component="PortfolioStateManager", **kwargs)


class StateOperationFailedError(StateValidationError):
    """Raised when state operations fail."""

    def __init__(self, operation: str, **kwargs: Unpack[StateExceptionKwargs]) -> None:
        """Initialize state operation exception."""
        super().__init__(
            f"Operation failed: {operation}", component="PortfolioStateManager", **kwargs
        )


class ScreenerNotInitializedError(StateNotInitializedError):
    """Raised when screener is not initialized."""

    def __init__(
        self, screener_name: str | None = None, **kwargs: Unpack[StateExceptionKwargs]
    ) -> None:
        """Initialize screener not initialized exception."""
        component = f"Screener[{screener_name}]" if screener_name else "Screener"
        super().__init__(
            "Screener not initialized", component=component, required_state="initialized", **kwargs
        )


class ServiceNotRunningError(StateError):
    """Raised when a service is not running."""

    def __init__(
        self, service_name: str | None = None, **kwargs: Unpack[StateExceptionKwargs]
    ) -> None:
        """Initialize service not running exception."""
        service = service_name or "Service"
        super().__init__(f"{service} not running", **kwargs)


# Result type validation exceptions to comply with TRY003
class ResultSuccessWithoutValueError(StateValidationError):
    """Raised when a success result has no value."""

    def __init__(self, **kwargs: Unpack[StateExceptionKwargs]) -> None:
        """Initialize result success without value exception."""
        super().__init__(
            "Success result must have a value",
            validation_errors=["Result marked as success but has no value"],
            component="Result",
            **kwargs,
        )


class ResultErrorWithoutErrorError(StateValidationError):
    """Raised when an error result has no error."""

    def __init__(self, **kwargs: Unpack[StateExceptionKwargs]) -> None:
        """Initialize result error without error exception."""
        super().__init__(
            "Error result must have an error",
            validation_errors=["Result marked as error but has no error value"],
            component="Result",
            **kwargs,
        )


class ResultSuccessWithErrorError(StateValidationError):
    """Raised when a success result has an error."""

    def __init__(self, **kwargs: Unpack[StateExceptionKwargs]) -> None:
        """Initialize result success with error exception."""
        super().__init__(
            "Success result cannot have an error",
            validation_errors=["Result marked as success but has error value"],
            component="Result",
            **kwargs,
        )


class ResultErrorWithValueError(StateValidationError):
    """Raised when an error result has a value."""

    def __init__(self, **kwargs: Unpack[StateExceptionKwargs]) -> None:
        """Initialize result error with value exception."""
        super().__init__(
            "Error result cannot have a value",
            validation_errors=["Result marked as error but has success value"],
            component="Result",
            **kwargs,
        )


class ResultUnwrapOnErrorError(StateValidationError):
    """Raised when unwrap is called on error result."""

    def __init__(self, error: object, **kwargs: Unpack[StateExceptionKwargs]) -> None:
        """Initialize result unwrap on error exception."""
        super().__init__(
            f"Called unwrap on error result: {error}",
            validation_errors=["Cannot unwrap error result"],
            component="Result",
            **kwargs,
        )


class ResultUnwrapNoneValueError(StateValidationError):
    """Raised when unwrapping None value."""

    def __init__(self, **kwargs: Unpack[StateExceptionKwargs]) -> None:
        """Initialize result unwrap none value exception."""
        super().__init__(
            "Success result has None value",
            validation_errors=["Cannot unwrap None value"],
            component="Result",
            **kwargs,
        )


class ResultUnwrapErrorOnSuccessError(StateValidationError):
    """Raised when unwrap_error is called on success result."""

    def __init__(self, value: object, **kwargs: Unpack[StateExceptionKwargs]) -> None:
        """Initialize result unwrap error on success exception."""
        super().__init__(
            f"Called unwrap_error on success result: {value}",
            validation_errors=["Cannot unwrap_error on success result"],
            component="Result",
            **kwargs,
        )


class ResultUnwrapNoneErrorError(StateValidationError):
    """Raised when unwrapping None error."""

    def __init__(self, **kwargs: Unpack[StateExceptionKwargs]) -> None:
        """Initialize result unwrap none error exception."""
        super().__init__(
            "Error result has None error",
            validation_errors=["Cannot unwrap None error"],
            component="Result",
            **kwargs,
        )


class ResultMapNoneValueError(StateValidationError):
    """Raised when mapping None value."""

    def __init__(self, **kwargs: Unpack[StateExceptionKwargs]) -> None:
        """Initialize result map none value exception."""
        super().__init__(
            "Success result has None value",
            validation_errors=["Cannot map None value"],
            component="Result",
            **kwargs,
        )


class ResultMapNoneErrorError(StateValidationError):
    """Raised when mapping with None error."""

    def __init__(self, **kwargs: Unpack[StateExceptionKwargs]) -> None:
        """Initialize result map none error exception."""
        super().__init__(
            "Error result has None error",
            validation_errors=["Cannot propagate None error"],
            component="Result",
            **kwargs,
        )


class ResultMapErrorNoneErrorError(StateValidationError):
    """Raised when map_error has None error."""

    def __init__(self, **kwargs: Unpack[StateExceptionKwargs]) -> None:
        """Initialize result map error none error exception."""
        super().__init__(
            "Error result has None error",
            validation_errors=["Cannot map_error on None error"],
            component="Result",
            **kwargs,
        )


class ResultMapErrorNoneValueError(StateValidationError):
    """Raised when map_error has None value."""

    def __init__(self, **kwargs: Unpack[StateExceptionKwargs]) -> None:
        """Initialize result map error none value exception."""
        super().__init__(
            "Success result has None value",
            validation_errors=["Cannot propagate None value"],
            component="Result",
            **kwargs,
        )


class ResultAndThenNoneValueError(StateValidationError):
    """Raised when and_then has None value."""

    def __init__(self, **kwargs: Unpack[StateExceptionKwargs]) -> None:
        """Initialize result and then none value exception."""
        super().__init__(
            "Success result has None value",
            validation_errors=["Cannot chain on None value"],
            component="Result",
            **kwargs,
        )


class ResultAndThenNoneErrorError(StateValidationError):
    """Raised when and_then has None error."""

    def __init__(self, **kwargs: Unpack[StateExceptionKwargs]) -> None:
        """Initialize result and then none error exception."""
        super().__init__(
            "Error result has None error",
            validation_errors=["Cannot propagate None error in chain"],
            component="Result",
            **kwargs,
        )


class ResultOrElseNoneErrorError(StateValidationError):
    """Raised when or_else has None error."""

    def __init__(self, **kwargs: Unpack[StateExceptionKwargs]) -> None:
        """Initialize result or else none error exception."""
        super().__init__(
            "Error result has None error",
            validation_errors=["Cannot apply or_else on None error"],
            component="Result",
            **kwargs,
        )


class ResultOrElseNoneValueError(StateValidationError):
    """Raised when or_else has None value."""

    def __init__(self, **kwargs: Unpack[StateExceptionKwargs]) -> None:
        """Initialize result or else none value exception."""
        super().__init__(
            "Success result has None value",
            validation_errors=["Cannot propagate None value in or_else"],
            component="Result",
            **kwargs,
        )
