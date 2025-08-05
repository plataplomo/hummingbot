"""Storage protocols for data persistence.

This module defines the storage interfaces that implementations must follow
for portfolio state and other persistent data.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from cyberdelta.models.portfolio.state import PortfolioState


class PortfolioStorageProtocol(ABC):
    """Protocol for portfolio state persistence.

    Defines the contract that all portfolio storage implementations must follow.

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO default implementations - pure interface
    - Explicit error handling in implementations
    - NO assumptions about storage backend
    """

    @abstractmethod
    async def save_state(self, state: PortfolioState) -> None:
        """Save portfolio state to persistent storage.

        Args:
            state: Portfolio state to persist

        Raises:
            StorageError: If save operation fails
        """
        pass

    @abstractmethod
    async def load_state(self) -> Optional[PortfolioState]:
        """Load portfolio state from persistent storage.

        Returns:
            PortfolioState if found, None if no state exists

        Raises:
            StorageError: If load operation fails
        """
        pass

    @abstractmethod
    async def save_snapshot(self, state: PortfolioState, snapshot_name: str) -> None:
        """Save a named snapshot of portfolio state.

        Args:
            state: Portfolio state to snapshot
            snapshot_name: Name/identifier for the snapshot

        Raises:
            StorageError: If snapshot operation fails
        """
        pass

    @abstractmethod
    async def list_snapshots(self) -> list[str]:
        """List all available snapshots.

        Returns:
            List of snapshot names/identifiers

        Raises:
            StorageError: If listing operation fails
        """
        pass

    @abstractmethod
    async def delete_snapshot(self, snapshot_name: str) -> None:
        """Delete a named snapshot.

        Args:
            snapshot_name: Name/identifier of snapshot to delete

        Raises:
            StorageError: If delete operation fails
        """
        pass


class StorageError(Exception):
    """Exception raised for storage operation failures.

    IMPORTANT: Following CODING_STANDARDS.md:
    - Explicit error messages with context
    - NO silent failures
    """

    def __init__(self, message: str, operation: str, original_error: Optional[Exception] = None):
        """Initialize storage error.

        Args:
            message: Human-readable error message
            operation: Storage operation that failed (save, load, etc.)
            original_error: Original exception that caused the failure
        """
        super().__init__(message)
        self.operation = operation
        self.original_error = original_error
