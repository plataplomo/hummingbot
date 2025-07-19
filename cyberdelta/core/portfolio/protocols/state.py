"""State management protocols for portfolio module."""

from __future__ import annotations

from typing import Protocol, Self, runtime_checkable

from pydantic import BaseModel


@runtime_checkable
class StateStorable(Protocol):
    """Protocol for objects that can be stored in state containers.
    
    This protocol defines the interface for objects that can be:
    - Stored in typed state containers
    - Serialized using Pydantic's built-in methods
    - Identified by a unique state key
    
    Clean break approach: No dict[str, Any] - use Pydantic serialization.
    """
    
    @property
    def state_key(self) -> str:
        """Unique identifier for this state object.
        
        Returns:
            Unique string identifier for state storage and retrieval
        """
        ...
    
    def model_dump(self) -> dict[str, object]:
        """Pydantic's built-in serialization method.
        
        Returns:
            Dictionary representation using Pydantic
        """
        ...
    
    @classmethod
    def model_validate(cls, data: dict[str, object]) -> Self:
        """Pydantic's built-in deserialization method.
        
        Args:
            data: Dictionary representation of state data
            
        Returns:
            New instance of the state object
        """
        ...


@runtime_checkable
class Snapshotable(Protocol):
    """Protocol for objects that support snapshotting.
    
    This protocol defines the interface for objects that can:
    - Create snapshots of their current state
    - Restore state from previous snapshots
    
    Clean break approach: Use Pydantic models for snapshots.
    """
    
    def create_snapshot(self) -> BaseModel:
        """Create a snapshot of the current state.
        
        Returns:
            Pydantic model representing current state snapshot
        """
        ...
    
    def restore_from_snapshot(self, snapshot: BaseModel) -> None:
        """Restore state from a snapshot.
        
        Args:
            snapshot: Pydantic model representing state to restore
        """
        ...