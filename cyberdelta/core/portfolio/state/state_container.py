"""Type-safe state container for portfolio state management."""

from __future__ import annotations

from collections.abc import Iterator

from pydantic import Field

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.models.base import BaseStateModel, StateSnapshot, ValidationResult


logger = get_logger(__name__)


class StateContainer[T: BaseStateModel](BaseStateModel):
    """Type-safe container for portfolio state with snapshot management.

    COMPLETE REPLACEMENT of the old dict[str, object] implementation.

    This class provides a unified interface for managing portfolio state
    with full type safety, eliminating all dict[str, object] patterns
    that caused type errors.

    Features:
    - Generic[T] for type preservation
    - Pydantic validation throughout
    - Protocol compliance for StateStorable, Snapshotable, Validatable
    - No dict[str, Any] or dict[str, object] anywhere
    """

    # Typed state storage - NO dict[str, object]
    states: dict[str, T] = Field(
        default_factory=dict, description="Type-safe state storage with Generic[T]"
    )

    # Version tracking
    version: int = Field(default=1, description="State container version")

    # Snapshot management
    # NOTE: pyright shows "list[Unknown]" due to generic type inference limitation
    # This is a known issue with pyright and Pydantic generics. The type is correct at runtime.
    snapshots: list[StateSnapshot[T]] = Field(  # pyright: ignore[reportUnknownVariableType]
        default_factory=list, description="State snapshots"
    )
    max_snapshots: int = Field(default=100, description="Maximum number of snapshots to retain")

    # Validation state
    is_valid: bool = Field(default=True, description="Whether the container state is valid")
    validation_errors: list[str] = Field(
        default_factory=list, description="List of validation errors"
    )

    # Strongly typed metadata - NO dict[str, Any]
    metadata: dict[str, str | int | float | bool] = Field(
        default_factory=dict, description="Container metadata with strict typing"
    )

    def model_post_init(self, context: dict[str, object] | None, /) -> None:
        """Post-initialization setup."""
        logger.info(
            "state_container_created",
            state_id=self.state_id,
            version=self.version,
            created_at=self.created_at,
        )

    def add_state(self, key: str, state: T) -> None:
        """Add a state with type safety.

        Args:
            key: State key
            state: State object that must be BaseModel subclass
        """
        # Type-safe storage - T is preserved
        self.states[key] = state
        self.version += 1
        self.update_timestamp()

        logger.debug(
            "state_added",
            state_id=self.state_id,
            key=key,
            state_type=type(state).__name__,
            version=self.version,
        )

    def get_state(self, key: str) -> T | None:
        """Get a state with preserved type information.

        Args:
            key: State key to retrieve

        Returns:
            State object of type T or None if not found
        """
        return self.states.get(key)

    def update_state(self, key: str, state: T) -> None:
        """Update an existing state.

        Args:
            key: State key to update
            state: New state object
        """
        old_state = self.states.get(key)
        self.states[key] = state
        self.version += 1
        self.update_timestamp()

        logger.debug(
            "state_updated",
            state_id=self.state_id,
            key=key,
            old_state_type=type(old_state).__name__ if old_state else None,
            new_state_type=type(state).__name__,
            version=self.version,
        )

    def remove_state(self, key: str) -> T | None:
        """Remove a state and return it.

        Args:
            key: State key to remove

        Returns:
            Removed state object or None if not found
        """
        removed_state = self.states.pop(key, None)
        if removed_state is not None:
            self.version += 1
            self.update_timestamp()

            logger.debug(
                "state_removed",
                state_id=self.state_id,
                key=key,
                removed_state_type=type(removed_state).__name__,
                version=self.version,
            )

        return removed_state

    def clear_states(self) -> None:
        """Clear all states."""
        old_count = len(self.states)
        self.states.clear()
        self.version += 1
        self.update_timestamp()

        logger.info(
            "states_cleared",
            state_id=self.state_id,
            old_count=old_count,
            version=self.version,
        )

    def iter_states(self) -> Iterator[tuple[str, T]]:
        """Iterate over states with full type preservation.

        This completely eliminates the dict[str, object] type errors
        where pyright inferred k: Unknown, v: Unknown.

        Yields:
            Tuples of (key: str, value: T) with full type information
        """
        yield from self.states.items()

    def get_state_count(self) -> int:
        """Get the number of stored states.
        
        Returns:
            Number of states currently stored in the container
        """
        return len(self.states)

    def get_state_keys(self) -> list[str]:
        """Get all state keys.
        
        Returns:
            List of all state keys in the container
        """
        return list(self.states.keys())

    def has_state(self, key: str) -> bool:
        """Check if a state key exists.
        
        Args:
            key: State key to check
            
        Returns:
            True if the key exists in the container, False otherwise
        """
        return key in self.states

    def create_snapshot(self) -> StateSnapshot[T]:
        """Create a snapshot of current state.

        Returns:
            StateSnapshot model with current state
        """
        snapshot = StateSnapshot[T](
            snapshot_id=f"{self.state_id}_snapshot_{len(self.snapshots)}",
            timestamp=self.updated_at,
            version=self.version,
            states=self.states.copy(),
            metadata=self.metadata.copy(),
        )

        self.snapshots.append(snapshot)

        # Manage snapshot limits
        if len(self.snapshots) > self.max_snapshots:
            removed_snapshot = self.snapshots.pop(0)
            logger.debug(
                "snapshot_removed_for_limit",
                state_id=self.state_id,
                removed_snapshot_id=removed_snapshot.snapshot_id,
                max_snapshots=self.max_snapshots,
            )

        logger.info(
            "snapshot_created",
            state_id=self.state_id,
            snapshot_id=snapshot.snapshot_id,
            version=self.version,
            snapshot_count=len(self.snapshots),
        )

        return snapshot

    def restore_from_snapshot(self, snapshot: StateSnapshot[T]) -> None:
        """Restore state from a snapshot.

        Args:
            snapshot: StateSnapshot model to restore from
        """
        self.states = snapshot.states.copy()
        self.version = snapshot.version + 1  # Increment version after restore
        self.metadata = snapshot.metadata.copy()
        self.update_timestamp()

        logger.info(
            "state_restored_from_snapshot",
            state_id=self.state_id,
            snapshot_id=snapshot.snapshot_id,
            restored_version=snapshot.version,
            new_version=self.version,
        )

    async def validate_state(self) -> ValidationResult:
        """Validate the container and all contained states.

        Returns:
            ValidationResult with comprehensive validation status
        """
        result = ValidationResult(valid=True)

        # Validate container state
        if not self.state_id:
            result.add_error("State ID cannot be empty")

        if self.version < 1:
            result.add_error("Version must be >= 1")

        if self.created_at > self.updated_at:
            result.add_error("Created time cannot be after updated time")

        # Validate each stored state - T is bound to BaseStateModel which implements validate_state
        for key, state in self.states.items():
            state_result = await state.validate_state()
            if not state_result.valid:
                result.valid = False
                for error in state_result.errors:
                    result.add_error(f"State '{key}': {error}")
                for warning in state_result.warnings:
                    result.add_warning(f"State '{key}': {warning}")

        self.is_valid = result.valid
        self.validation_errors = result.errors.copy()

        if not self.is_valid:
            logger.warning(
                "state_container_validation_failed",
                state_id=self.state_id,
                errors=self.validation_errors,
            )

        return result

    @property
    def state_key(self) -> str:
        """State key for StateStorable protocol compliance."""
        return f"container_{self.state_id}"
