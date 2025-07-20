"""WebSocket connection adapter for error recovery integration.

This module provides adapters to integrate existing WebSocket connections
with the error recovery system.
"""

from __future__ import annotations

from typing import Any, Protocol

from cyberdelta.config.structlog_config import get_logger


class WebSocketConnection(Protocol):
    """Protocol for WebSocket connections."""

    async def connect(self) -> None:
        """Connect to the WebSocket."""
        ...

    async def disconnect(self) -> None:
        """Disconnect from the WebSocket."""
        ...

    async def send(self, message: str | bytes | dict[str, Any]) -> None:
        """Send a message through the WebSocket."""
        ...

    @property
    def closed(self) -> bool:
        """Check if connection is closed."""
        ...


class WebSocketConnectionAdapter:
    """Adapter to make WebSocket connections compatible with error recovery."""

    def __init__(self, websocket: WebSocketConnection, connection_id: str) -> None:
        """Initialize the adapter.

        Args:
            websocket: WebSocket connection instance
            connection_id: Unique connection identifier
        """
        self.websocket = websocket
        self.connection_id = connection_id
        self.logger = get_logger(f"WSAdapter.{connection_id}")
        self._last_health_check = 0.0

    async def connect(self) -> bool:
        """Establish connection.

        Returns:
            True if connection successful
        """
        try:
            await self.websocket.connect()
        except (ConnectionError, OSError, TimeoutError, ValueError) as e:
            self.logger.warning(
                "connection_failed",
                connection_id=self.connection_id,
                error=str(e),
            )
            return False
        else:
            self.logger.info("connection_established", connection_id=self.connection_id)
            return True

    async def disconnect(self) -> None:
        """Close connection."""
        try:
            await self.websocket.disconnect()
            self.logger.info("connection_closed", connection_id=self.connection_id)
        except (ConnectionError, OSError, TimeoutError, ValueError) as e:
            self.logger.warning(
                "disconnect_error",
                connection_id=self.connection_id,
                error=str(e),
            )

    async def is_healthy(self) -> bool:
        """Check if connection is healthy.

        Returns:
            True if connection is healthy
        """
        try:
            # Basic check: connection should not be closed
            # Could add ping/pong check here if WebSocket supports it
            # For now, just check if connection is open
            return not self.websocket.closed

        except (ConnectionError, OSError, TimeoutError, ValueError) as e:
            self.logger.debug(
                "health_check_error",
                connection_id=self.connection_id,
                error=str(e),
            )
            return False

    async def send_message(self, message: dict[str, Any]) -> bool:
        """Send message through connection.

        Args:
            message: Message to send

        Returns:
            True if message sent successfully
        """
        try:
            await self.websocket.send(message)
        except (ConnectionError, OSError, TimeoutError, ValueError) as e:
            self.logger.warning(
                "message_send_failed",
                connection_id=self.connection_id,
                error=str(e),
            )
            return False
        else:
            return True
