"""Type-safe state snapshot using Pydantic + StateStorable protocol.

COMPLETE REPLACEMENT - Eliminates all dict[str, object] patterns and manual validation.
"""

from __future__ import annotations

import hashlib
import sys
from collections.abc import Generator
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.models.base import BaseStateModel, ValidationResult


logger = get_logger(__name__)


class SnapshotMetadata(BaseModel):
    """Metadata for state snapshots with strict typing."""

    source_component: str = Field(..., description="Component that created the snapshot")
    snapshot_type: str = Field(
        default="manual", description="Type of snapshot (manual, automatic, etc.)"
    )
    reason: str | None = Field(default=None, description="Reason for creating the snapshot")
    tags: dict[str, str | int | float | bool] = Field(
        default_factory=dict, description="Custom tags for snapshot categorization"
    )
    retention_days: int | None = Field(
        default=None, description="Number of days to retain this snapshot"
    )

    @field_validator("tags")
    @classmethod
    def validate_tags(
        cls, v: dict[str, str | int | float | bool]
    ) -> dict[str, str | int | float | bool]:
        """Validate tags contain only allowed types.
        
        Returns:
            dict[str, str | int | float | bool]: The validated tags dictionary.
        """
        # All values are already validated by type hints
        return v


class StateDataComparison(BaseModel):
    """Type-safe state data comparison details."""

    common_keys: list[str] = Field(..., description="Keys present in both states")
    added_keys: list[str] = Field(..., description="Keys added in self vs other")
    removed_keys: list[str] = Field(..., description="Keys removed in self vs other")
    changed_keys: list[str] = Field(..., description="Keys with different values")
    total_keys: dict[str, int] = Field(..., description="Total key counts")


class SnapshotComparison(BaseModel):
    """Type-safe comparison result between two snapshots."""

    snapshot_ids: dict[str, str] = Field(..., description="Snapshot IDs being compared")
    state_ids: dict[str, Any] = Field(..., description="State ID comparison")
    versions: dict[str, int] = Field(..., description="Version comparison")
    timestamps: dict[str, Any] = Field(..., description="Timestamp comparison")
    checksums: dict[str, Any] = Field(..., description="Checksum comparison")
    state_data: dict[str, Any] = Field(..., description="State data differences")


class ChangeDetail(BaseModel):
    """Type-safe change detail for snapshot diffs."""

    field_name: str = Field(..., description="Name of the changed field")
    old_value: str | int | float | bool | None = Field(..., description="Previous value")
    new_value: str | int | float | bool | None = Field(..., description="New value")
    change_type: str = Field(..., description="Type of change: added/removed/modified")


class DiffSummary(BaseModel):
    """Type-safe summary statistics for snapshot diffs."""

    total_changes: int = Field(..., description="Total number of changes")
    added_count: int = Field(..., description="Number of added fields")
    removed_count: int = Field(..., description="Number of removed fields")
    modified_count: int = Field(..., description="Number of modified fields")
    has_changes: bool = Field(..., description="Whether there are any changes")


class SnapshotDiff(BaseModel):
    """Type-safe detailed diff between two snapshots."""

    comparison: SnapshotComparison = Field(..., description="Basic comparison info")
    detailed_changes: dict[str, ChangeDetail] = Field(
        default_factory=dict, description="Detailed changes by field name"
    )
    added_values: dict[str, str | int | float | bool] = Field(
        default_factory=dict, description="Values that were added"
    )
    removed_values: dict[str, str | int | float | bool] = Field(
        default_factory=dict, description="Values that were removed"
    )
    summary: DiffSummary = Field(..., description="Summary statistics")


class SnapshotSummary(BaseModel):
    """Type-safe snapshot summary data.

    Clean break: No dict[str, Any] - explicit Pydantic model.
    """

    snapshot_id: str = Field(..., description="Snapshot identifier")
    target_state_id: str = Field(..., description="Target state identifier")
    state_id: str = Field(..., description="State identifier")
    version: int = Field(..., description="Snapshot version")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Update timestamp")
    state_size: int = Field(..., description="Size of state data")
    state_fields: list[str] = Field(..., description="List of state fields")
    field_count: int = Field(..., description="Number of fields")
    checksum: str | None = Field(..., description="Data checksum")
    is_valid: bool = Field(..., description="Validation status")
    validation_errors: list[str] = Field(..., description="Validation errors")
    snapshot_metadata: SnapshotMetadata = Field(..., description="Snapshot metadata")
    state_data_type: str = Field(..., description="Type name of state data")


class StateSnapshot[T: BaseModel](BaseStateModel):
    """Type-safe immutable snapshot of portfolio state.

    COMPLETE REPLACEMENT of the old dataclass implementation that caused type errors
    due to dict[str, object] usage and manual validation.

    Features:
    - Pure Pydantic model with automatic validation
    - Generic type preservation for state data
    - StateStorable protocol compliance
    - Validatable protocol compliance
    - Zero dict[str, object] usage
    """

    # Core snapshot identification
    snapshot_id: str = Field(..., description="Unique identifier for this snapshot")
    target_state_id: str = Field(..., description="ID of the state being snapshotted")

    # Type-safe state data using Generic[T]
    state_data: T = Field(..., description="Strongly-typed state data")

    # Versioning
    version: int = Field(default=1, ge=1, description="Snapshot version number")

    # Snapshot metadata with strict typing
    snapshot_metadata: SnapshotMetadata = Field(
        default_factory=lambda: SnapshotMetadata(source_component="portfolio_tracker"),
        description="Snapshot metadata",
    )

    # Validation state
    is_valid: bool = Field(default=True, description="Whether snapshot passed validation")
    validation_errors: list[str] = Field(
        default_factory=list, description="List of validation errors"
    )

    # Integrity verification
    checksum: str | None = Field(default=None, description="Data integrity checksum")

    def model_post_init(self, __context: dict[str, object] | None = None, /) -> None:
        """Post-initialization setup with automatic checksum calculation."""
        super().model_post_init(__context)

        if self.checksum is None:
            self.checksum = self.calculate_checksum()

        logger.debug(
            "state_snapshot_created",
            snapshot_id=self.snapshot_id,
            target_state_id=self.target_state_id,
            state_id=self.state_id,
            version=self.version,
            created_at=self.created_at,
            checksum=self.checksum,
            state_data_type=type(self.state_data).__name__,
        )

    @property
    def state_key(self) -> str:
        """State key for StateStorable protocol compliance."""
        return f"snapshot_{self.snapshot_id}"

    def calculate_checksum(self) -> str:
        """Calculate checksum of the state data using Pydantic serialization.

        Returns:
            Checksum string
        """
        try:
            # Use Pydantic's model_dump_json for consistent serialization
            state_json = self.state_data.model_dump_json()
            return hashlib.sha256(state_json.encode()).hexdigest()
        except (TypeError, ValueError, AttributeError) as e:
            logger.warning(
                "checksum_calculation_failed",
                snapshot_id=self.snapshot_id,
                error=str(e),
                error_type=type(e).__name__,
            )
            return "unknown"

    def verify_integrity(self) -> bool:
        """Verify the integrity of the snapshot.

        Returns:
            True if integrity is verified
        """
        if self.checksum is None:
            return False

        current_checksum = self.calculate_checksum()
        is_valid = current_checksum == self.checksum

        if not is_valid:
            logger.error(
                "snapshot_integrity_check_failed",
                snapshot_id=self.snapshot_id,
                expected_checksum=self.checksum,
                actual_checksum=current_checksum,
            )

        return is_valid

    def get_state_value(
        self, key: str, default: str | float | bool | None = None
    ) -> str | int | float | bool | None:
        """Get a value from the snapshot state using Pydantic field access.

        Args:
            key: State field name to retrieve
            default: Default value if field not found

        Returns:
            State field value or default
        """
        # Check if field exists in model fields
        if key in self.state_data.__class__.model_fields:
            # Access the attribute directly - will exist if in model_fields
            return self.state_data.__dict__.get(key, default)
        return default

    def has_state_field(self, field_name: str) -> bool:
        """Check if a state field exists in the snapshot.

        Args:
            field_name: State field name to check

        Returns:
            True if field exists
        """
        # Use Pydantic's model_fields to check field existence
        return field_name in self.state_data.__class__.model_fields

    def get_state_fields(self) -> list[str]:
        """Get all state field names in the snapshot.

        Returns:
            List of state field names
        """
        return list(self.state_data.__class__.model_fields.keys())

    def get_state_size(self) -> int:
        """Get the approximate size of the snapshot data.

        Returns:
            Approximate size in bytes
        """
        return sys.getsizeof(self.state_data.model_dump_json())

    def compare_with(self, other: StateSnapshot[T]) -> SnapshotComparison:
        """Compare this snapshot with another snapshot.

        Args:
            other: Other snapshot to compare with

        Returns:
            Structured comparison results
        """
        # Compare basic properties
        comparison_data = {
            "snapshot_ids": {
                "self": self.snapshot_id,
                "other": other.snapshot_id,
            },
            "state_ids": {
                "self": self.target_state_id,
                "other": other.target_state_id,
                "same": self.target_state_id == other.target_state_id,
            },
            "versions": {
                "self": self.version,
                "other": other.version,
                "diff": self.version - other.version,
            },
            "timestamps": {
                "self": self.created_at,
                "other": other.created_at,
                "diff_seconds": (self.created_at - other.created_at).total_seconds(),
            },
            "checksums": {
                "self": self.checksum,
                "other": other.checksum,
                "same": self.checksum == other.checksum,
            },
        }

        # Compare state data using Pydantic field comparison
        self_state_dict = self.state_data.model_dump()
        other_state_dict = other.state_data.model_dump()

        self_keys = set(self_state_dict.keys())
        other_keys = set(other_state_dict.keys())

        common_keys = self_keys & other_keys
        added_keys = self_keys - other_keys
        removed_keys = other_keys - self_keys

        changed_keys = [key for key in common_keys if self_state_dict[key] != other_state_dict[key]]

        # Create type-safe state data comparison
        state_data_comparison = StateDataComparison(
            common_keys=list(common_keys),
            added_keys=list(added_keys),
            removed_keys=list(removed_keys),
            changed_keys=changed_keys,
            total_keys={
                "self": len(self_keys),
                "other": len(other_keys),
            },
        )
        comparison_data["state_data"] = state_data_comparison.model_dump()

        return SnapshotComparison.model_validate(comparison_data)

    def create_diff(self, other: StateSnapshot[T]) -> SnapshotDiff:
        """Create a detailed diff with another snapshot.

        Args:
            other: Other snapshot to diff with

        Returns:
            Structured detailed differences
        """
        comparison = self.compare_with(other)

        self_state_dict = self.state_data.model_dump()
        other_state_dict = other.state_data.model_dump()

        # Create detailed diff for changed keys
        detailed_changes: dict[str, ChangeDetail] = {}
        for key in comparison.state_data["changed_keys"]:
            detailed_changes[key] = ChangeDetail(
                field_name=key,
                old_value=self._convert_value(other_state_dict[key]),
                new_value=self._convert_value(self_state_dict[key]),
                change_type="modified",
            )

        # Create detailed diff for added keys
        added_values: dict[str, str | int | float | bool] = {}
        for key in comparison.state_data["added_keys"]:
            added_values[key] = self._convert_value(self_state_dict[key])
            detailed_changes[key] = ChangeDetail(
                field_name=key,
                old_value=None,
                new_value=self._convert_value(self_state_dict[key]),
                change_type="added",
            )

        # Create detailed diff for removed keys
        removed_values: dict[str, str | int | float | bool] = {}
        for key in comparison.state_data["removed_keys"]:
            removed_values[key] = self._convert_value(other_state_dict[key])
            detailed_changes[key] = ChangeDetail(
                field_name=key,
                old_value=self._convert_value(other_state_dict[key]),
                new_value=None,
                change_type="removed",
            )

        summary = DiffSummary(
            total_changes=len(detailed_changes),
            added_count=len(added_values),
            removed_count=len(removed_values),
            modified_count=len(comparison.state_data["changed_keys"]),
            has_changes=(len(detailed_changes) > 0),
        )

        return SnapshotDiff(
            comparison=comparison,
            detailed_changes=detailed_changes,
            added_values=added_values,
            removed_values=removed_values,
            summary=summary,
        )

    def _convert_value(self, value: str | float | bool | object) -> str | int | float | bool:
        """Convert value to allowed types for strict typing.
        
        Returns:
            str | int | float | bool: Converted value or 'None' string.
        """
        if isinstance(value, (str, int, float, bool)):
            return value
        return str(value)

    async def validate_state(self) -> ValidationResult:
        """Validate the snapshot using Pydantic + custom business logic.

        Returns:
            ValidationResult with validation status
        """
        result = ValidationResult(valid=True)

        # Basic validation (Pydantic handles most of this automatically)
        if not self.snapshot_id:
            result.add_error("Snapshot ID cannot be empty")

        if not self.target_state_id:
            result.add_error("Target state ID cannot be empty")

        if self.version < 1:
            result.add_error("Version must be >= 1")

        # Validate state data using Pydantic
        try:
            # Re-validate the state data - BaseModel guaranteed by type system
            self.state_data.model_validate(self.state_data.model_dump())
        except (TypeError, ValueError, AttributeError) as e:
            result.add_error(f"State data validation failed: {e!s}")

        # Verify integrity
        if not self.verify_integrity():
            result.add_error("Snapshot integrity check failed")

        # Update validation state
        self.is_valid = result.valid
        self.validation_errors = result.errors.copy()

        if not result.valid:
            logger.warning(
                "snapshot_validation_failed",
                snapshot_id=self.snapshot_id,
                errors=result.errors,
                warnings=result.warnings,
            )

        return result

    def get_summary(self) -> SnapshotSummary:
        """Get a summary of the snapshot.

        Returns:
            Type-safe SnapshotSummary model
        """
        return SnapshotSummary(
            snapshot_id=self.snapshot_id,
            target_state_id=self.target_state_id,
            state_id=self.state_id,
            version=self.version,
            created_at=self.created_at,
            updated_at=self.updated_at,
            state_size=self.get_state_size(),
            state_fields=self.get_state_fields(),
            field_count=len(self.get_state_fields()),
            checksum=self.checksum,
            is_valid=self.is_valid,
            validation_errors=self.validation_errors,
            snapshot_metadata=self.snapshot_metadata,
            state_data_type=type(self.state_data).__name__,
        )

    def is_newer_than(self, other: StateSnapshot[T]) -> bool:
        """Check if this snapshot is newer than another.

        Args:
            other: Other snapshot to compare with

        Returns:
            True if this snapshot is newer
        """
        return self.created_at > other.created_at

    def is_same_state(self, other: StateSnapshot[T]) -> bool:
        """Check if this snapshot is from the same state.

        Args:
            other: Other snapshot to compare with

        Returns:
            True if from same state
        """
        return self.target_state_id == other.target_state_id

    def get_age_seconds(self) -> float:
        """Get the age of the snapshot in seconds.

        Returns:
            Age in seconds
        """
        return (datetime.now(UTC) - self.created_at).total_seconds()

    def get_age_minutes(self) -> float:
        """Get the age of the snapshot in minutes.

        Returns:
            Age in minutes
        """
        return self.get_age_seconds() / 60

    def get_age_hours(self) -> float:
        """Get the age of the snapshot in hours.

        Returns:
            Age in hours
        """
        return self.get_age_minutes() / 60

    def is_expired(self, max_age_seconds: float) -> bool:
        """Check if the snapshot is expired.

        Args:
            max_age_seconds: Maximum age in seconds

        Returns:
            True if expired
        """
        return self.get_age_seconds() > max_age_seconds

    def create_copy(self, new_snapshot_id: str | None = None) -> StateSnapshot[T]:
        """Create a copy of this snapshot with type safety.

        Args:
            new_snapshot_id: Optional new snapshot ID

        Returns:
            New StateSnapshot instance
        """
        copy_id = new_snapshot_id or f"{self.snapshot_id}_copy"

        # Use Pydantic's model_copy for deep copying
        copied_state_data = self.state_data.model_copy(deep=True)
        copied_metadata = self.snapshot_metadata.model_copy(deep=True)

        return StateSnapshot[T](
            state_id=f"{self.state_id}_copy",
            snapshot_id=copy_id,
            target_state_id=self.target_state_id,
            state_data=copied_state_data,
            version=self.version,
            created_at=datetime.now(UTC),  # New timestamp for copy
            snapshot_metadata=copied_metadata,
            is_valid=self.is_valid,
            validation_errors=self.validation_errors.copy(),
            checksum=None,  # Will be recalculated in post_init
        )

    def __len__(self) -> int:
        """Get the number of state fields.
        
        Returns:
            int: Number of state fields in the snapshot.
        """
        return len(self.get_state_fields())

    def __contains__(self, field_name: str) -> bool:
        """Check if a field exists in the snapshot state.
        
        Returns:
            bool: True if field exists, False otherwise.
        """
        return self.has_state_field(field_name)

    def __getitem__(self, field_name: str) -> str | int | float | bool | None:
        """Get a state field value by name.
        
        Returns:
            str | int | float | bool | None: Field value or None if not found.
        """
        return self.get_state_value(field_name)

    def __iter__(self) -> Generator[tuple[str, str | int | float | bool | None]]:
        """Iterate over state field names.
        
        Yields:
            tuple[str, str | int | float | bool | None]: Field name and value pairs.
        """
        for field in self.get_state_fields():
            yield field, self.get_state_value(field)

    def __eq__(self, other: object) -> bool:
        """Check equality with another snapshot.
        
        Returns:
            bool: True if snapshots are equal, False otherwise.
        """
        if not isinstance(other, StateSnapshot):
            return False

        return (
            self.snapshot_id == other.snapshot_id
            and self.target_state_id == other.target_state_id
            and self.checksum == other.checksum
        )

    def __hash__(self) -> int:
        """Get hash of the snapshot.
        
        Returns:
            int: Hash value based on snapshot ID, target state ID, and checksum.
        """
        return hash((self.snapshot_id, self.target_state_id, self.checksum))

    def __repr__(self) -> str:
        """String representation.
        
        Returns:
            str: String representation of the StateSnapshot object.
        """
        return (
            f"StateSnapshot(snapshot_id='{self.snapshot_id}', "
            f"target_state_id='{self.target_state_id}', "
            f"version={self.version}, "
            f"created_at={self.created_at}, "
            f"state_fields={len(self)})"
        )


# Type-safe factory functions for common snapshot types


def create_portfolio_snapshot[T: BaseModel](
    snapshot_id: str,
    target_state_id: str,
    state_data: T,
    source_component: str = "portfolio_tracker",
    snapshot_type: str = "manual",
    reason: str | None = None,
    **metadata_kwargs: str | float | bool,
) -> StateSnapshot[T]:
    """Create a portfolio state snapshot with proper typing.

    Args:
        snapshot_id: Unique identifier for the snapshot
        target_state_id: ID of the state being snapshotted
        state_data: Strongly-typed state data
        source_component: Component creating the snapshot
        snapshot_type: Type of snapshot
        reason: Reason for creating the snapshot
        **metadata_kwargs: Additional metadata

    Returns:
        Properly typed StateSnapshot
    """
    metadata = SnapshotMetadata(
        source_component=source_component,
        snapshot_type=snapshot_type,
        reason=reason,
        tags=metadata_kwargs,
    )

    return StateSnapshot[T](
        state_id=f"snapshot_{snapshot_id}",
        snapshot_id=snapshot_id,
        target_state_id=target_state_id,
        state_data=state_data,
        snapshot_metadata=metadata,
    )


def create_automatic_snapshot[T: BaseModel](
    target_state_id: str,
    state_data: T,
    trigger_reason: str,
    source_component: str = "auto_snapshot_service",
) -> StateSnapshot[T]:
    """Create an automatic snapshot with proper typing.

    Args:
        target_state_id: ID of the state being snapshotted
        state_data: Strongly-typed state data
        trigger_reason: Reason that triggered the automatic snapshot
        source_component: Component creating the snapshot

    Returns:
        Properly typed StateSnapshot
    """
    snapshot_id = f"auto_{target_state_id}_{int(datetime.now(UTC).timestamp())}"

    return create_portfolio_snapshot(
        snapshot_id=snapshot_id,
        target_state_id=target_state_id,
        state_data=state_data,
        source_component=source_component,
        snapshot_type="automatic",
        reason=trigger_reason,
        auto_generated=True,
    )
