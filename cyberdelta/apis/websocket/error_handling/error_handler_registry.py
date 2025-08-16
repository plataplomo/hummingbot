"""Registry for managing WebSocket error handlers per exchange.

This registry provides centralized management of error handlers,
allowing for efficient lookup and lifecycle management of handlers
across different exchanges and environments.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from weakref import WeakValueDictionary

from cyberdelta.apis.websocket.error_handling.error_handler import (
    WebSocketErrorHandler,
)
from cyberdelta.apis.websocket.error_handling.error_handler_factory import (
    WebSocketErrorHandlerFactory,
)
from cyberdelta.apis.websocket.error_handling.recovery import (
    ConnectionManagerProtocol,
    StateManagerProtocol,
    SubscriptionManagerProtocol,
)
from cyberdelta.apis.websocket.metrics.error_metrics import WebSocketErrorMetrics
from cyberdelta.config.models.websocket_error_config import WebSocketErrorConfig
from cyberdelta.enums import ExchangeName


if TYPE_CHECKING:
    from logging import Logger


# ============================================================================
# Registry Implementation
# ============================================================================


class WebSocketErrorHandlerRegistry:
    """Registry for managing WebSocket error handlers.

    This registry provides centralized management of error handlers,
    ensuring proper lifecycle management and efficient handler lookup.
    Features:
    - Per-exchange handler management
    - Automatic cleanup with weak references
    - Environment-specific configuration
    - Thread-safe operations
    """

    def __init__(self, logger: Logger | None = None) -> None:
        """Initialize the error handler registry.

        Args:
            logger: Optional logger instance for registry operations
        """
        self._logger = logger or logging.getLogger(__name__)

        # Use weak references to allow automatic cleanup
        self._handlers: WeakValueDictionary[str, WebSocketErrorHandler] = WeakValueDictionary()

        # Track handler configurations for recreation
        self._handler_configs: dict[str, WebSocketErrorConfig] = {}
        self._handler_params: dict[str, dict[str, object]] = {}

        # Registry statistics
        self._creation_count = 0
        self._lookup_count = 0
        self._cache_hits = 0

        self._logger.info("WebSocket error handler registry initialized")

    def get_handler(
        self,
        exchange: ExchangeName,
        config: WebSocketErrorConfig | None = None,
        environment: str = "production",
        connection_manager: ConnectionManagerProtocol | None = None,
        subscription_manager: SubscriptionManagerProtocol | None = None,
        state_manager: StateManagerProtocol | None = None,
        metrics_collector: WebSocketErrorMetrics | None = None,
    ) -> WebSocketErrorHandler:
        """Get or create an error handler for the specified exchange.

        Args:
            exchange: Exchange name (e.g., 'hyperliquid', 'backpack')
            config: Optional error configuration (uses default if not provided)
            environment: Environment name for configuration
            connection_manager: Optional connection manager
            subscription_manager: Optional subscription manager
            state_manager: Optional state manager
            metrics_collector: Optional metrics collector

        Returns:
            WebSocketErrorHandler: Error handler for the exchange

        """
        self._lookup_count += 1

        # Create registry key
        registry_key = self._create_registry_key(exchange, environment)

        # Check if handler exists and is still valid
        if registry_key in self._handlers:
            handler = self._handlers[registry_key]
            if self._is_handler_valid(handler, config):
                self._cache_hits += 1
                self._logger.debug("Retrieved cached error handler for %s", registry_key)
                return handler
            self._logger.debug("Cached handler invalid, recreating for %s", registry_key)
            del self._handlers[registry_key]

        # Create new handler
        handler = self._create_handler(
            exchange=exchange,
            config=config,
            environment=environment,
            connection_manager=connection_manager,
            subscription_manager=subscription_manager,
            state_manager=state_manager,
            metrics_collector=metrics_collector,
        )

        # Store in registry
        self._handlers[registry_key] = handler
        self._creation_count += 1

        self._logger.info("Created and registered error handler for %s", registry_key)
        return handler

    def remove_handler(self, exchange: ExchangeName, environment: str = "production") -> bool:
        """Remove a handler from the registry.

        Args:
            exchange: Exchange name
            environment: Environment name

        Returns:
            bool: True if handler was removed, False if not found
        """
        registry_key = self._create_registry_key(exchange, environment)

        if registry_key in self._handlers:
            del self._handlers[registry_key]
            if registry_key in self._handler_configs:
                del self._handler_configs[registry_key]
            if registry_key in self._handler_params:
                del self._handler_params[registry_key]

            self._logger.info("Removed error handler for %s", registry_key)
            return True

        self._logger.debug("No error handler found to remove for %s", registry_key)
        return False

    def clear_handlers(self) -> None:
        """Clear all handlers from the registry."""
        handler_count = len(self._handlers)

        self._handlers.clear()
        self._handler_configs.clear()
        self._handler_params.clear()

        self._logger.info("Cleared %d error handlers from registry", handler_count)

    def list_active_handlers(self) -> list[str]:
        """Get list of active handler keys.

        Returns:
            List of active handler registry keys
        """
        return list(self._handlers.keys())

    def get_registry_statistics(self) -> dict[str, int]:
        """Get registry usage statistics.

        Returns:
            Dictionary with registry statistics
        """
        return {
            "active_handlers": len(self._handlers),
            "total_created": self._creation_count,
            "total_lookups": self._lookup_count,
            "cache_hits": self._cache_hits,
            "cache_hit_rate_percent": (
                int((self._cache_hits / self._lookup_count) * 100) if self._lookup_count > 0 else 0
            ),
        }

    def health_check(self) -> dict[str, object]:
        """Perform health check on the registry.

        Returns:
            Dictionary with health check results
        """
        issues: list[str] = []
        health_status: dict[str, object] = {
            "registry_healthy": True,
            "active_handlers": len(self._handlers),
            "issues": issues,
        }

        # Check for any obvious issues
        if len(self._handlers) == 0:
            issues.append("No active handlers registered")

        # Check handler validity
        invalid_handlers: list[str] = []
        for key, handler in self._handlers.items():
            try:
                # Basic validation - ensure handler has required attributes
                if not hasattr(handler, "config") or not hasattr(handler, "logger"):
                    invalid_handlers.append(key)
            except (AttributeError, TypeError) as e:
                invalid_handlers.append(f"{key}: {e}")

        if invalid_handlers:
            issues.extend([f"Invalid handler: {handler}" for handler in invalid_handlers])
            health_status["registry_healthy"] = False

        return health_status

    # ========================================================================
    # Private Methods
    # ========================================================================

    def _create_registry_key(self, exchange: ExchangeName, environment: str) -> str:
        """Create a unique registry key for an exchange and environment.

        Args:
            exchange: Exchange name
            environment: Environment name

        Returns:
            Unique registry key
        """
        return f"{exchange.value.lower().strip()}_{environment.lower().strip()}"

    def _create_handler(
        self,
        exchange: ExchangeName,
        config: WebSocketErrorConfig | None,
        environment: str,
        connection_manager: ConnectionManagerProtocol | None,
        subscription_manager: SubscriptionManagerProtocol | None,
        state_manager: StateManagerProtocol | None,
        metrics_collector: WebSocketErrorMetrics | None,
    ) -> WebSocketErrorHandler:
        """Create a new error handler.

        Args:
            exchange: Exchange name
            config: Error configuration
            environment: Environment name
            connection_manager: Connection manager
            subscription_manager: Subscription manager
            state_manager: State manager
            metrics_collector: Metrics collector

        Returns:
            New error handler instance
        """
        # Use provided config or create default
        if config is None:
            config = WebSocketErrorHandlerFactory.create_default_config(
                exchange=exchange,
                environment=environment,
            )

        # Create exchange-specific logger
        handler_logger = logging.getLogger(f"websocket.{exchange.value}.error_handler")

        # Store configuration for validation
        registry_key = self._create_registry_key(exchange, environment)
        self._handler_configs[registry_key] = config
        self._handler_params[registry_key] = {
            "exchange": exchange,
            "environment": environment,
            "has_connection_manager": connection_manager is not None,
            "has_subscription_manager": subscription_manager is not None,
            "has_state_manager": state_manager is not None,
            "has_metrics_collector": metrics_collector is not None,
        }

        # Create handler using factory
        return WebSocketErrorHandlerFactory.create_handler(
            exchange=exchange,
            config=config,
            logger=handler_logger,
            metrics_collector=metrics_collector,
            connection_manager=connection_manager,
            subscription_manager=subscription_manager,
            state_manager=state_manager,
        )

    def _is_handler_valid(
        self,
        handler: WebSocketErrorHandler,
        new_config: WebSocketErrorConfig | None,
    ) -> bool:
        """Check if a cached handler is still valid.

        Args:
            handler: Existing handler
            new_config: New configuration to compare against

        Returns:
            True if handler is valid for reuse
        """
        # Basic validation
        if not hasattr(handler, "config"):
            return False

        # If no new config provided, handler is valid
        if new_config is None:
            return True

        # Compare configurations (simple implementation)
        # In production, you might want more sophisticated comparison
        try:
            current_config_dict = handler.config.model_dump()
            new_config_dict = new_config.model_dump()
        except (AttributeError, TypeError, ValueError):
            # If comparison fails, assume invalid
            return False
        else:
            return current_config_dict == new_config_dict


# ============================================================================
# Note: Global registry pattern removed for better dependency injection.
# Use WebSocketErrorHandlerFactory.create_handler() directly instead.
# The registry class above can still be used for local caching if needed.
# ============================================================================
