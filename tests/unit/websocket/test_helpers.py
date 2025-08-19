"""Test helpers for WebSocket unit tests.

This module provides mock implementations and adapters for testing
the refactored WebSocket error handling system.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cyberdelta.apis.websocket.error_context.error_handler import WebSocketErrorHandler
from cyberdelta.apis.websocket.error_context.error_handler_factory import (
    WebSocketErrorHandlerFactory,
)
from cyberdelta.config.models.websocket_error_config import WebSocketErrorConfig
from cyberdelta.enums import ExchangeName


if TYPE_CHECKING:
    from typing import Any


class MockWebSocketErrorHandlerRegistry:
    """Mock registry adapter for test compatibility.

    This adapter provides a registry-like interface to the factory-based
    error handler system for backwards compatibility with existing tests.
    """

    def __init__(self) -> None:
        """Initialize the mock registry."""
        self._handlers: dict[str, WebSocketErrorHandler] = {}
        self._configs: dict[str, WebSocketErrorConfig] = {}
        self._created_count = 0
        self._cache_hits = 0

    def get_handler(
        self,
        exchange: ExchangeName,
        environment: str = "test",
    ) -> WebSocketErrorHandler:
        """Get or create a handler for the specified exchange.

        Args:
            exchange: Exchange to get handler for
            environment: Environment (test, production, etc.)

        Returns:
            WebSocketErrorHandler for the exchange
        """
        key = f"{exchange.value}_{environment}"

        if key in self._handlers:
            self._cache_hits += 1
            return self._handlers[key]

        # Create new handler using factory
        config = WebSocketErrorHandlerFactory.create_default_config(
            exchange=exchange,
            environment=environment,
        )
        self._configs[key] = config

        handler = WebSocketErrorHandlerFactory.create_handler(
            exchange=exchange,
            config=config,
        )

        self._handlers[key] = handler
        self._created_count += 1

        return handler

    def remove_handler(self, exchange: ExchangeName, environment: str = "test") -> bool:
        """Remove a handler from the registry.

        Args:
            exchange: Exchange to remove handler for
            environment: Environment

        Returns:
            True if handler was removed, False if not found
        """
        key = f"{exchange.value}_{environment}"

        if key in self._handlers:
            del self._handlers[key]
            if key in self._configs:
                del self._configs[key]
            return True

        return False

    def list_active_handlers(self) -> list[str]:
        """List all active handler keys.

        Returns:
            List of handler keys
        """
        return list(self._handlers.keys())

    def get_registry_statistics(self) -> dict[str, Any]:
        """Get registry statistics.

        Returns:
            Dictionary of statistics
        """
        return {
            "total_created": self._created_count,
            "active_handlers": len(self._handlers),
            "cache_hits": self._cache_hits,
        }

    def health_check(self) -> dict[str, Any]:
        """Perform health check on the registry.

        Returns:
            Health check results
        """
        issues = []

        if len(self._handlers) == 0:
            issues.append("No active handlers registered")

        return {
            "registry_healthy": True,
            "active_handlers": len(self._handlers),
            "issues": issues,
        }
