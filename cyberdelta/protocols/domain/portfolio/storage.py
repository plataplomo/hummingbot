"""Storage protocols for portfolio persistence."""

from __future__ import annotations

from typing import Protocol

from cyberdelta.models.portfolio.state import PortfolioState


class PortfolioStorageProtocol(Protocol):
    """Protocol for portfolio state persistence.

    Defines the interface for persisting and loading portfolio state
    from various storage backends (files, databases, etc.).

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO default implementations - pure interface
    - Explicit error handling in implementations
    - NO assumptions about storage backend
    """

    async def load_state(self) -> PortfolioState | None:
        """Load portfolio state from storage.

        Returns:
            PortfolioState if found, None otherwise

        Raises:
            StorageError: If load operation fails
        """
        ...

    async def save_state(self, state: PortfolioState) -> None:
        """Save portfolio state to storage.

        Args:
            state: Portfolio state to save

        Raises:
            StorageError: If save operation fails
        """
        ...

    async def save_snapshot(self, state: PortfolioState, snapshot_name: str) -> None:
        """Save a named snapshot of portfolio state.

        Args:
            state: Portfolio state to snapshot
            snapshot_name: Name/identifier for the snapshot


        Raises:
            StorageError: If snapshot operation fails
        """
        ...

    async def list_snapshots(self) -> list[str]:
        """List all available snapshots.

        Returns:
            List of snapshot names/identifiers


        Raises:
            StorageError: If listing operation fails
        """
        ...

    async def delete_snapshot(self, snapshot_name: str) -> None:
        """Delete a named snapshot.

        Args:
            snapshot_name: Name/identifier of snapshot to delete


        Raises:
            StorageError: If delete operation fails
        """
        ...


class StorageError(Exception):
    """Exception raised for storage operation failures.

    IMPORTANT: Following CODING_STANDARDS.md:
    - Explicit error messages with context
    - NO silent failures
    """

    def __init__(
        self, message: str, operation: str, original_error: Exception | None = None
    ) -> None:
        """Initialize storage error.

        Args:
            message: Human-readable error message
            operation: Storage operation that failed (save, load, etc.)
            original_error: Original exception that caused the failure
        """
        super().__init__(message)
        self.operation = operation
        self.original_error = original_error
