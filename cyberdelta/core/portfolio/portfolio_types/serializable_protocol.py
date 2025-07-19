"""Protocols for serializable objects in portfolio system."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class SerializableProtocol(Protocol):
    """Protocol for objects that can be serialized to dict."""

    def to_dict(self) -> dict[str, Any]:
        """Convert object to dictionary representation."""
        ...


@runtime_checkable
class TimestampedProtocol(Protocol):
    """Protocol for objects with timestamp information."""

    @property
    def timestamp(self) -> float:
        """Get timestamp of the object."""
        ...


@runtime_checkable
class CreatedAtProtocol(Protocol):
    """Protocol for objects with creation time."""

    @property
    def created_at(self) -> float:
        """Get creation timestamp of the object."""
        ...


@runtime_checkable
class AwaitableProtocol(Protocol):
    """Protocol for awaitable objects."""

    def __await__(self) -> object:
        """Make object awaitable."""
        ...


@runtime_checkable
class IsRunningProtocol(Protocol):
    """Protocol for objects with running state."""

    @property
    def is_running(self) -> bool:
        """Check if object is currently running."""
        ...


@runtime_checkable
class ChangesProtocol(Protocol):
    """Protocol for objects with changes tracking."""

    @property
    def changes(self) -> list[Any]:
        """Get list of changes."""
        ...
