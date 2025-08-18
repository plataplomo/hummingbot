"""Protocols for WebSocket event handling system.

This module defines the protocols for event handlers and filters used by
the WebSocket error event publishing system.
"""

from typing import Protocol

from pydantic import BaseModel


class EventHandlerProtocol(Protocol):
    """Protocol for event handlers."""

    async def handle_event(self, event: BaseModel) -> None:
        """Handle a published event.

        Args:
            event: The event to handle
        """
        ...


class EventFilterProtocol(Protocol):
    """Protocol for event filters."""

    def should_publish(self, event: BaseModel) -> bool:
        """Determine if an event should be published.

        Args:
            event: The event to check

        Returns:
            True if the event should be published
        """
        ...
