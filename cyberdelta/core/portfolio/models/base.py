"""Base Pydantic models for portfolio module."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import TypeVar

from pydantic import BaseModel, Field


def utc_now() -> datetime:
    """Get current UTC datetime."""
    return datetime.now(UTC)


class BaseStateModel(BaseModel):
    """Base Pydantic model for all state objects.

    This model provides common fields and functionality for all
    state objects in the portfolio system, replacing untyped
    dict[str, object] patterns.
    """

    state_id: str = Field(..., description="Unique identifier for this state object")
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Timestamp when this state was created",
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Timestamp when this state was last updated",
    )

    def update_timestamp(self) -> None:
        """Update the updated_at timestamp to current time."""
        self.updated_at = datetime.now(UTC)

    async def validate_state(self) -> ValidationResult:
        """Validate this state object.

        Returns:
            ValidationResult with validation status
        """
        result = ValidationResult(valid=True)

        # Basic validation
        if not self.state_id:
            result.add_error("State ID cannot be empty")

        if self.created_at > self.updated_at:
            result.add_error("Created time cannot be after updated time")

        return result


class BaseEventModel(BaseModel):
    """Base Pydantic model for all event objects.

    This model provides common fields for event-driven architecture
    with proper type safety and validation.
    """

    event_id: str = Field(..., description="Unique identifier for this event")
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC), description="Timestamp when this event occurred"
    )
    event_type: str = Field(..., description="Type of event for discriminated unions")


class ValidationResult(BaseModel):
    """Pydantic model for validation results.

    Replaces untyped dict[str, Any] validation result patterns
    with strongly typed, validated structures.
    """

    valid: bool = Field(..., description="Whether validation passed")
    errors: list[str] = Field(default_factory=list, description="List of validation error messages")
    warnings: list[str] = Field(
        default_factory=list, description="List of validation warning messages"
    )

    def add_error(self, error: str) -> None:
        """Add an error message and mark validation as invalid."""
        self.errors.append(error)
        self.valid = False

    def add_warning(self, warning: str) -> None:
        """Add a warning message."""
        self.warnings.append(warning)

    @property
    def has_errors(self) -> bool:
        """Check if there are any validation errors."""
        return len(self.errors) > 0

    @property
    def has_warnings(self) -> bool:
        """Check if there are any validation warnings."""
        return len(self.warnings) > 0

    @property
    def is_valid(self) -> bool:
        """Alias for valid property for compatibility."""
        return self.valid


# Generic type variable for StateWrapper
T = TypeVar("T", bound=BaseModel)


class StateWrapper[T: BaseModel](BaseModel):
    """Generic wrapper for type-safe state persistence.

    This model wraps any BaseModel subclass for serialization,
    providing metadata and ensuring type safety throughout
    the persistence layer.
    """

    state_id: str = Field(..., description="Unique identifier for the wrapped state")
    timestamp: float = Field(
        default_factory=time.time, description="Unix timestamp when this wrapper was created"
    )
    datetime_iso: str = Field(
        default_factory=lambda: datetime.now(UTC).isoformat(),
        description="ISO format datetime for human readability",
    )
    metadata: dict[str, str | int | float | bool] = Field(
        default_factory=dict, description="Additional metadata for the wrapped state"
    )
    data: T = Field(..., description="The actual state data being wrapped")

    @property
    def wrapped_datetime(self) -> datetime:
        """Get the wrapped timestamp as a datetime object."""
        return datetime.fromtimestamp(self.timestamp, UTC)


class StateSnapshot[T: BaseModel](BaseModel):
    """Type-safe state snapshot model.

    Clean break: No dict[str, Any] - explicit Pydantic model.
    """

    snapshot_id: str = Field(..., description="Unique snapshot identifier")
    timestamp: datetime = Field(default_factory=utc_now)
    version: int = Field(..., description="Snapshot version")
    states: dict[str, T] = Field(default_factory=dict, description="Snapshot of states")
    metadata: dict[str, str | int | float | bool] = Field(
        default_factory=dict, description="Snapshot metadata"
    )
