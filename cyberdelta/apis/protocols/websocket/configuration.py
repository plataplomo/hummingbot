"""Configuration protocols for WebSocket system.

This module contains protocol definitions for application configuration
interfaces in the WebSocket system.
"""

from __future__ import annotations

from typing import Protocol

from cyberdelta.config.models.websocket_error_config import WebSocketErrorConfig


class AppConfigProtocol(Protocol):
    """Protocol for accessing application configuration.

    This protocol defines the interface that configuration objects must
    implement to provide WebSocket error configuration access.
    """

    @property
    def websocket_error(self) -> WebSocketErrorConfig:
        """Get WebSocket error configuration.

        Returns:
            WebSocket error configuration object
        """
        ...

    def get_exchange_config(self, exchange_name: str) -> object:
        """Get exchange-specific configuration.

        Args:
            exchange_name: Name of the exchange

        Returns:
            Exchange configuration object
        """
        ...

    def is_production_environment(self) -> bool:
        """Check if running in production environment.

        Returns:
            True if production environment
        """
        ...
