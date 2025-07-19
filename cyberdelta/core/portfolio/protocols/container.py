"""State container protocols for type-safe state management."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, TypeVar, runtime_checkable

from pydantic import BaseModel, Field


if TYPE_CHECKING:
    from cyberdelta.core.models import DerivativePosition, Order, SpotBalance, Trade


T = TypeVar("T", bound=BaseModel)


class ContainerOperationResult(BaseModel):
    """Result of a container operation."""
    success: bool = Field(description="Whether the operation succeeded")
    execution_time_ms: float = Field(default=0.0, description="Operation execution time")


@runtime_checkable
class StateContainerProtocol(Protocol[T]):
    """Protocol for state containers that manage typed state.
    
    This protocol defines the interface for containers that can store,
    retrieve, and manage state objects with full type safety.
    """
    
    def add_state(self, key: str, state: T) -> None:
        """Add a state to the container.
        
        Args:
            key: Unique identifier for the state
            state: State object to store
        """
        ...
    
    def get_state(self, key: str) -> T | None:
        """Get a state from the container.
        
        Args:
            key: State identifier
            
        Returns:
            State object or None if not found
        """
        ...
    
    def update_state(self, key: str, state: T) -> None:
        """Update an existing state.
        
        Args:
            key: State identifier
            state: New state object
        """
        ...
    
    def remove_state(self, key: str) -> T | None:
        """Remove a state from the container.
        
        Args:
            key: State identifier
            
        Returns:
            Removed state object or None if not found
        """
        ...
    
    def has_state(self, key: str) -> bool:
        """Check if a state exists in the container.
        
        Args:
            key: State identifier
            
        Returns:
            True if state exists
        """
        ...
    
    def get_state_count(self) -> int:
        """Get the number of states in the container.
        
        Returns:
            Number of stored states
        """
        ...
    
    def get_state_keys(self) -> list[str]:
        """Get all state keys.
        
        Returns:
            List of state identifiers
        """
        ...
    
    def clear_states(self) -> None:
        """Clear all states from the container."""
        ...
    
    # Portfolio-specific methods - matching actual usage patterns
    def get_balances(self, exchange: str) -> dict[str, SpotBalance]:
        """Get balance information from container for specific exchange."""
        ...
    
    def get_positions(self, exchange: str) -> dict[str, DerivativePosition]:
        """Get position information from container for specific exchange."""
        ...
    
    def get_orders(self, exchange: str) -> dict[str, Order]:
        """Get order information from container for specific exchange.""" 
        ...
    
    def add_trade(self, exchange: str, trade: Trade) -> ContainerOperationResult:
        """Add a trade to the container, returns operation result."""
        ...
    
    def update_balances(
        self, exchange: str, balances: dict[str, SpotBalance]
    ) -> ContainerOperationResult:
        """Update balance information in container, returns operation result."""
        ...
    
    def update_positions(
        self, exchange: str, positions: dict[str, DerivativePosition]
    ) -> ContainerOperationResult:
        """Update position information in container, returns operation result."""
        ...