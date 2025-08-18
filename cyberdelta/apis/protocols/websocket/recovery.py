"""Recovery protocols for WebSocket error handling.

This module contains protocol definitions for recovery handlers,
connection managers, subscription managers, and state managers
in the WebSocket recovery system.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import Protocol

from cyberdelta.apis.models.websocket.recovery import RecoveryResult
from cyberdelta.apis.websocket.exceptions import WebSocketStreamError
from cyberdelta.enums import ExchangeName


class RecoveryHandler(Protocol):
    """Protocol for recovery handlers."""

    @abstractmethod
    async def can_handle(self, error: WebSocketStreamError) -> bool:
        """Check if this handler can handle the error.

        Args:
            error: The WebSocket stream error

        Returns:
            True if this handler can handle the error
        """
        ...

    @abstractmethod
    async def handle(
        self, error: WebSocketStreamError, recovery: object | None = None
    ) -> RecoveryResult:
        """Handle the recovery for the error.

        Args:
            error: The WebSocket stream error
            recovery: Optional recovery system reference

        Returns:
            Result of the recovery attempt
        """
        ...


class ConnectionManagerProtocol(Protocol):
    """Protocol for connection management in recovery system."""

    async def reconnect(
        self,
        connection_id: str,
        exchange: ExchangeName,
        force: bool = False,
    ) -> bool:
        """Reconnect to the WebSocket.

        Args:
            connection_id: Connection identifier
            exchange: Exchange name enum
            force: Force reconnection even if connected

        Returns:
            True if reconnection successful
        """
        ...

    async def reset_connection(
        self,
        connection_id: str,
        exchange: ExchangeName,
    ) -> bool:
        """Reset the connection state.

        Args:
            connection_id: Connection identifier
            exchange: Exchange name enum

        Returns:
            True if reset successful
        """
        ...

    async def get_connection_state(
        self,
        connection_id: str,
        exchange: ExchangeName,
    ) -> str:
        """Get current connection state.

        Args:
            connection_id: Connection identifier
            exchange: Exchange name enum

        Returns:
            Connection state string
        """
        ...


class SubscriptionManagerProtocol(Protocol):
    """Protocol for subscription management in recovery system."""

    async def resubscribe(
        self,
        connection_id: str,
        exchange: ExchangeName,
        channel: str | None = None,
        topic: str | None = None,
    ) -> bool:
        """Resubscribe to channels.

        Args:
            connection_id: Connection identifier
            exchange: Exchange name enum
            channel: Optional specific channel
            topic: Optional specific topic

        Returns:
            True if resubscription successful
        """
        ...

    async def resubscribe_all(
        self,
        connection_id: str,
        exchange: ExchangeName,
    ) -> bool:
        """Resubscribe to all channels.

        Args:
            connection_id: Connection identifier
            exchange: Exchange name enum

        Returns:
            True if all resubscriptions successful
        """
        ...

    async def get_active_subscriptions(
        self,
        connection_id: str,
        exchange: ExchangeName,
    ) -> list[tuple[str, str | None]]:
        """Get list of active subscriptions.

        Args:
            connection_id: Connection identifier
            exchange: Exchange name enum

        Returns:
            List of (channel, topic) tuples
        """
        ...


class StateManagerProtocol(Protocol):
    """Protocol for state management in recovery system."""

    async def save_state(
        self,
        connection_id: str,
        exchange: ExchangeName,
    ) -> bool:
        """Save current state.

        Args:
            connection_id: Connection identifier
            exchange: Exchange name enum

        Returns:
            True if state saved successfully
        """
        ...

    async def restore_state(
        self,
        connection_id: str,
        exchange: ExchangeName,
    ) -> bool:
        """Restore saved state.

        Args:
            connection_id: Connection identifier
            exchange: Exchange name enum

        Returns:
            True if state restored successfully
        """
        ...

    async def clear_state(
        self,
        connection_id: str,
        exchange: ExchangeName,
    ) -> bool:
        """Clear saved state.

        Args:
            connection_id: Connection identifier
            exchange: Exchange name enum

        Returns:
            True if state cleared successfully
        """
        ...


class MessageBufferProtocol(Protocol):
    """Protocol for message buffering during recovery."""

    def buffer_message(
        self,
        connection_id: str,
        message: object,
    ) -> None:
        """Buffer a message during recovery.

        Args:
            connection_id: Connection identifier
            message: Message to buffer
        """
        ...

    def get_buffered_messages(
        self,
        connection_id: str,
    ) -> list[object]:
        """Get buffered messages.

        Args:
            connection_id: Connection identifier

        Returns:
            List of buffered messages
        """
        ...

    async def clear_buffer(
        self,
        connection_id: str,
        exchange: ExchangeName,
    ) -> None:
        """Clear message buffer.

        Args:
            connection_id: Connection identifier
            exchange: Exchange name enum
        """
        ...

    async def replay_messages(
        self,
        connection_id: str,
        exchange: ExchangeName,
        since_sequence: int | None = None,
    ) -> None:
        """Replay buffered messages.

        Args:
            connection_id: Connection identifier
            exchange: Exchange name enum
            since_sequence: Replay messages since this sequence number
        """
        ...
