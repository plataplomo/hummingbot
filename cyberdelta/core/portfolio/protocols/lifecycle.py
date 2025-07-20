"""Lifecycle management protocols for portfolio components."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class Initializable(Protocol):
    """Protocol for objects that can be initialized.

    This protocol defines the interface for objects that require
    explicit initialization before use.
    """

    async def initialize(self) -> None:
        """Initialize the object.

        This method should set up any required resources,
        connections, or state needed for operation.
        """
        ...


@runtime_checkable
class Shutdownable(Protocol):
    """Protocol for objects that can be shut down.

    This protocol defines the interface for objects that need
    explicit cleanup when no longer needed.
    """

    async def shutdown(self) -> None:
        """Shutdown the object.

        This method should clean up resources, close connections,
        and perform any necessary cleanup operations.
        """
        ...


@runtime_checkable
class HealthCheckable(Protocol):
    """Protocol for objects that support health checks."""

    async def health_check(self) -> bool:
        """Check the health status of the object.

        Returns:
            True if the object is healthy and operational
        """
        ...
