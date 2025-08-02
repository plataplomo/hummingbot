"""Typed state manager base class with direct AppSettings access.

Following the risk module pattern of direct AppSettings access without abstraction layers.
"""

from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from dataclasses import field
from datetime import datetime
from typing import TYPE_CHECKING, Any, TypeVar

from pydantic import BaseModel, Field
from pydantic.dataclasses import dataclass

from cyberdelta.config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.exceptions.state import (
    StateTransitionError,
    StateValidationError,
)
from cyberdelta.core.portfolio.portfolio_types.infrastructure import StateValidationResult


# Type-preserving factory functions for dataclass fields
def _str_list_factory() -> list[str]:
    """Factory function that preserves list[str] type information.

    Returns:
        Empty list of strings.
    """
    return []


class StateManagerMetadata(BaseModel):
    """Generic metadata for state manager operations."""

    operation_type: str | None = Field(default=None, description="Type of operation performed")
    source: str | None = Field(default=None, description="Source of the operation")
    timestamp: float | None = Field(default=None, description="Operation timestamp")
    version: int | None = Field(default=None, description="State version")
    user_id: str | None = Field(default=None, description="User who performed operation")
    session_id: str | None = Field(default=None, description="Session ID")
    correlation_id: str | None = Field(default=None, description="Correlation ID for tracing")
    additional_data: dict[str, str] = Field(
        default_factory=dict, description="Additional string data"
    )


if TYPE_CHECKING:
    from cyberdelta.core.portfolio.models.base import BaseStateModel
    from cyberdelta.core.portfolio.portfolio_types.protocols import StateContainerProtocol

# Type variable for state data
T = TypeVar("T", bound=object)


@dataclass
class StateUpdate[T]:
    """Represents a state update with metadata."""

    data: T
    timestamp: datetime
    source: str
    version: int = 0
    metadata: StateManagerMetadata | None = None


@dataclass(frozen=True)
class StateManagerResult:
    """Result from state management operations."""

    success: bool
    message: str
    data: Any | None = None
    errors: list[str] = field(default_factory=_str_list_factory)
    metadata: StateManagerMetadata | None = None
    execution_time_ms: float = 0.0

    @classmethod
    def success_result(
        cls,
        message: str = "Operation successful",
        data: object | None = None,
        metadata: StateManagerMetadata | None = None,
        execution_time_ms: float = 0.0,
    ) -> StateManagerResult:
        """Create a successful result.

        Returns:
            StateManagerResult with success=True.
        """
        return cls(
            success=True,
            message=message,
            data=data,
            metadata=metadata,
            execution_time_ms=execution_time_ms,
        )

    @classmethod
    def failure_result(
        cls,
        message: str,
        errors: list[str] | None = None,
        metadata: StateManagerMetadata | None = None,
        execution_time_ms: float = 0.0,
    ) -> StateManagerResult:
        """Create a failure result.

        Returns:
            StateManagerResult with success=False.
        """
        return cls(
            success=False,
            message=message,
            errors=errors or [message],
            metadata=metadata,
            execution_time_ms=execution_time_ms,
        )


class TypedStateManager[T](ABC):
    """Base class for typed state managers with direct AppSettings access.

    Follows risk module pattern:
    - Direct AppSettings access (no abstraction)
    - Dataclasses for results
    - Strong typing with generics
    - No service locators or dependency injection
    """

    def __init__(
        self,
        app_settings: AppSettings,
        state_container: StateContainerProtocol[BaseStateModel],
        manager_name: str,
    ) -> None:
        """Initialize the typed state manager.

        Args:
            app_settings: Application settings with portfolio configuration
            state_container: State container for persistence
            manager_name: Name of this manager for logging
        """
        self.app_settings = app_settings
        self.state_container = state_container
        self.manager_name = manager_name
        self.logger = get_logger(f"{self.__class__.__module__}.{self.__class__.__name__}")

        # State management with versioning
        self._state_lock = asyncio.Lock()
        self._state_version = 0
        self._last_update: datetime | None = None
        self._state_history: list[StateUpdate[T]] = []
        self._max_history_size = app_settings.state.max_state_history_size

        # Configuration from AppSettings
        self.atomic_updates = app_settings.state.atomic_updates
        self.update_timeout = app_settings.state.update_timeout
        self.validate_on_update = app_settings.state.validate_on_update

        # Performance tracking
        self.update_count = 0
        self.validation_count = 0
        self.error_count = 0

    @abstractmethod
    async def get_state(self) -> T:
        """Get the current state.

        Returns:
            Current state data
        """
        ...

    @abstractmethod
    async def update_state(self, update: StateUpdate[T]) -> StateManagerResult:
        """Update the state with new data.

        Args:
            update: State update with data and metadata

        Returns:
            Result of the update operation
        """
        ...

    @abstractmethod
    async def validate_state(self, state: T) -> StateValidationResult:
        """Validate state data.

        Args:
            state: State to validate

        Returns:
            Validation result
        """
        ...

    async def update_with_validation(self, update: StateUpdate[T]) -> StateManagerResult:
        """Update state with validation.

        Args:
            update: State update to apply

        Returns:
            Result of the update operation
        """
        if self.atomic_updates:
            async with self._state_lock:
                return await self._perform_update_with_validation(update)
        else:
            return await self._perform_update_with_validation(update)

    async def _perform_update_with_validation(self, update: StateUpdate[T]) -> StateManagerResult:
        """Perform state update with optional validation.

        Args:
            update: State update to apply

        Returns:
            Result of the update operation
        """
        start_time = time.time()

        try:
            # Validate if enabled
            if self.validate_on_update:
                validation_result = await self.validate_state(update.data)
                if not validation_result.is_valid:
                    self.error_count += 1
                    return StateManagerResult.failure_result(
                        message="State validation failed",
                        errors=validation_result.errors,
                        metadata=None,  # Can't serialize StateValidationResult to string dict
                        execution_time_ms=(time.time() - start_time) * 1000,
                    )
                self.validation_count += 1

            # Set version for the update
            update.version = self._state_version + 1

            # Apply update
            result = await self.update_state(update)

            # Calculate execution time
            execution_time = (time.time() - start_time) * 1000

            if result.success:
                self.update_count += 1
                self._state_version = update.version
                self._last_update = update.timestamp

                # Add to history
                self._add_to_history(update)

                # Create new result with execution time
                return StateManagerResult.success_result(
                    message=result.message,
                    data=result.data,
                    metadata=result.metadata,
                    execution_time_ms=execution_time,
                )
            self.error_count += 1

            # Create new result with execution time
            return StateManagerResult.failure_result(
                message=result.message,
                errors=result.errors,
                metadata=result.metadata,
                execution_time_ms=execution_time,
            )

        except TimeoutError:
            self.error_count += 1
            return StateManagerResult.failure_result(
                message=f"Update timeout after {self.update_timeout}s",
                errors=[f"Operation timed out after {self.update_timeout} seconds"],
                execution_time_ms=(time.time() - start_time) * 1000,
            )
        except (StateTransitionError, StateValidationError) as e:
            self.error_count += 1
            self.logger.exception(
                "State update error",
                error=str(e),
                error_type=type(e).__name__,
                manager=self.manager_name,
            )
            return StateManagerResult.failure_result(
                message=f"State error: {e}",
                errors=[str(e)],
                execution_time_ms=(time.time() - start_time) * 1000,
            )
        except Exception as e:
            self.error_count += 1
            self.logger.exception(
                "Unexpected error during state update",
                error=str(e),
                manager=self.manager_name,
            )
            return StateManagerResult.failure_result(
                message=f"Unexpected error: {e}",
                errors=[str(e)],
                execution_time_ms=(time.time() - start_time) * 1000,
            )

    async def get_state_with_timeout(self, *, timeout_seconds: float | None = None) -> T | None:
        """Get state with timeout.

        Args:
            timeout_seconds: Timeout in seconds (uses config default if None)

        Returns:
            State data or None if timeout
        """
        timeout_value = timeout_seconds or self.update_timeout
        try:
            return await asyncio.wait_for(self.get_state(), timeout=timeout_value)
        except TimeoutError:
            self.logger.warning(
                "State retrieval timeout",
                timeout=timeout_value,
                manager=self.manager_name,
            )
            return None

    async def bulk_update(self, updates: list[StateUpdate[T]]) -> list[StateManagerResult]:
        """Apply multiple updates.

        Args:
            updates: List of updates to apply

        Returns:
            List of results for each update
        """
        results: list[StateManagerResult] = []
        for update in updates:
            result = await self.update_with_validation(update)
            results.append(result)
            if not result.success and self.atomic_updates:
                # Stop on first failure in atomic mode
                break
        return results

    def _add_to_history(self, update: StateUpdate[T]) -> None:
        """Add update to state history, maintaining size limit."""
        self._state_history.append(update)

        # Trim history if it exceeds maximum size
        if len(self._state_history) > self._max_history_size:
            self._state_history = self._state_history[-self._max_history_size :]

    def get_state_history(self, limit: int | None = None) -> list[StateUpdate[T]]:
        """Get state update history.

        Args:
            limit: Maximum number of updates to return (None for all)

        Returns:
            List of state updates, most recent first
        """
        history = list(reversed(self._state_history))
        if limit is not None:
            return history[:limit]
        return history

    def get_state_at_version(self, version: int) -> StateUpdate[T] | None:
        """Get state update for a specific version.

        Args:
            version: State version to retrieve

        Returns:
            State update if found, None otherwise
        """
        for update in self._state_history:
            if update.version == version:
                return update
        return None

    def get_metrics(self) -> dict[str, Any]:
        """Get performance metrics.

        Returns:
            Dictionary of metrics
        """
        return {
            "manager_name": self.manager_name,
            "state_version": self._state_version,
            "last_update": self._last_update.isoformat() if self._last_update else None,
            "update_count": self.update_count,
            "validation_count": self.validation_count,
            "error_count": self.error_count,
            "error_rate": self.error_count / max(self.update_count, 1),
            "atomic_updates": self.atomic_updates,
            "validate_on_update": self.validate_on_update,
            "history_size": len(self._state_history),
            "max_history_size": self._max_history_size,
        }

    def reset_metrics(self) -> None:
        """Reset performance metrics."""
        self.update_count = 0
        self.validation_count = 0
        self.error_count = 0

    def __str__(self) -> str:
        """String representation.

        Returns:
            String representation of the state manager.
        """
        return (
            f"{self.__class__.__name__}("
            f"manager={self.manager_name}, "
            f"version={self._state_version}, "
            f"updates={self.update_count})"
        )
