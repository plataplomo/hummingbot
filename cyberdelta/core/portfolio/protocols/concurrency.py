"""Concurrency protocols for portfolio module."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class AsyncLockProtocol(Protocol):
    """Protocol for async lock objects.

    This protocol defines the interface for objects that provide
    asynchronous locking capabilities with proper type safety.
    Eliminates "unknown member" errors for lock.release() calls.
    """

    async def acquire(self) -> None:
        """Acquire the lock.

        This method blocks until the lock is available.
        """
        ...

    def release(self) -> None:
        """Release the lock.

        This method releases the lock, making it available for other tasks.
        """
        ...

    def locked(self) -> bool:
        """Check if the lock is currently held.

        Returns:
            True if the lock is currently held, False otherwise
        """
        ...

    async def __aenter__(self) -> None:
        """Async context manager entry.

        Acquires the lock when entering the context.
        """
        ...

    async def __aexit__(self, *args: object) -> None:
        """Async context manager exit.

        Releases the lock when exiting the context.

        Args:
            *args: Exception information if an exception occurred
        """
        ...


@runtime_checkable
class ConcurrencyAware(Protocol):
    """Protocol for objects that are aware of concurrency requirements.

    This protocol defines the interface for objects that can specify
    their concurrency requirements and priorities.
    """

    @property
    def lock_key(self) -> str:
        """Unique key for lock acquisition ordering.

        Returns:
            String key used to ensure consistent lock ordering
        """
        ...

    @property
    def lock_priority(self) -> int:
        """Priority for lock acquisition.

        Returns:
            Integer priority (lower numbers = higher priority)
        """
        ...
