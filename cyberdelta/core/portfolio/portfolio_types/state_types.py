"""State management type definitions."""

from __future__ import annotations

from enum import Enum
from typing import TypeVar, cast

from pydantic import BaseModel, Field, PrivateAttr, field_validator

from cyberdelta.core.portfolio.exceptions import ContainerSizeLimitExceededError


T = TypeVar("T", bound=BaseModel)  # Properly bounded to BaseModel


class StateValidationMetadata(BaseModel):
    """Typed metadata for state validation results."""

    validation_timestamp: float | None = None
    validator_version: str | None = None
    check_type: str | None = None
    data_source: str | None = None
    validation_scope: str | None = None
    performance_metrics: dict[str, float] = Field(default_factory=dict)
    related_components: list[str] = Field(default_factory=list)


class StateChangeType(Enum):
    """Types of state changes."""

    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"
    RESTORED = "restored"
    SNAPSHOT = "snapshot"


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
            timestamp=0.0,  # Should be set by caller
            previous_state=previous,
            new_state=entity,
        )
        self._change_history.append(change)
        self.modification_count += 1

    def remove(self, entity_id: str) -> T | None:
        """Remove entity from container."""
        entity = self._entities.pop(entity_id, None)
        if entity:
            # Record change
            change = StateChange(
                entity_type=self.entity_type,
                entity_id=entity_id,
                change_type=StateChangeType.DELETED,
                timestamp=0.0,  # Should be set by caller
                previous_state=entity,
                new_state=None,
            )
            self._change_history.append(change)
            self.modification_count += 1
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

    def create_snapshot(self, snapshot_id: str) -> StateSnapshot[T]:
        """Create snapshot of current state."""
        snapshot = StateSnapshot(
            snapshot_id=snapshot_id,
            timestamp=0.0,  # Should be set by caller
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
            timestamp=0.0,  # Should be set by caller
            change_source=f"snapshot:{snapshot_id}",
        )
        self._change_history.append(change)

        # Restore state
        self._entities = snapshot.entities.copy()
        self.modification_count += 1

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


class StateUpdateResult(BaseModel):
    """Result of state update operation."""

    success: bool
    execution_time_ms: float = 0.0
    affected_entities: int = 0
    errors: list[str] = Field(default_factory=list)
    metadata: dict[str, str] = Field(default_factory=dict)


class PortfolioSnapshot(BaseModel):
    """Snapshot of portfolio state."""

    timestamp: float
    exchange: str
    balances: dict[str, str] = Field(default_factory=dict)
    positions: dict[str, str] = Field(default_factory=dict)
    orders: list[str] = Field(default_factory=list)
    metadata: dict[str, str] = Field(default_factory=dict)


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
