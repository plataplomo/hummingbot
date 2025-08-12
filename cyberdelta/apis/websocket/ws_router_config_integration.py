"""Router configuration integration for typed WebSocket error handling.

This module provides utilities to integrate WebSocket router configuration
with the new typed error system, enabling seamless configuration of router
error handling behavior.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar

from pydantic import BaseModel

from cyberdelta.apis.websocket.ws_router_error_bridge import RouterErrorBridge
from cyberdelta.apis.websocket.ws_stream_error_handler import WebSocketStreamErrorHandler
from cyberdelta.config.models.websocket_error_config import (
    WebSocketErrorConfig,
)
from cyberdelta.config.structlog_config import get_logger


if TYPE_CHECKING:
    from cyberdelta.apis.websocket.ws_error_handler import BaseErrorHandler
    from cyberdelta.apis.websocket.ws_router import BaseWebSocketRouter

# Type variable for any BaseModel envelope type
EnvelopeT = TypeVar("EnvelopeT", bound=BaseModel)


class RouterConfigurationError(Exception):
    """Raised when router configuration is invalid or incomplete."""

    def __init__(self, message: str, config_field: str | None = None) -> None:
        """Initialize configuration error.

        Args:
            message: Error description
            config_field: Field name that caused the error
        """
        super().__init__(message)
        self.config_field = config_field


class WebSocketRouterConfigurator[EnvelopeT: BaseModel]:
    """Configures WebSocket routers with typed error handling.

    This configurator provides methods to integrate router configuration
    with the new typed error system, creating appropriate error handlers,
    bridges, and validation based on configuration settings.
    """

    def __init__(self, config: WebSocketErrorConfig) -> None:
        """Initialize router configurator.

        Args:
            config: WebSocket error configuration
        """
        self.config = config
        self.router_config = config.router
        self.logger = get_logger("RouterConfigurator")

    def configure_router_error_handling(
        self,
        router: BaseWebSocketRouter[EnvelopeT],
        legacy_error_handler: BaseErrorHandler,
    ) -> RouterErrorBridge[EnvelopeT] | None:
        """Configure router with typed error handling.

        Args:
            router: Router to configure
            legacy_error_handler: Legacy error handler for fallback

        Returns:
            Configured error bridge or None if disabled

        Raises:
            RouterConfigurationError: If configuration is invalid
        """
        try:
            # Check if router error handling is enabled
            if not self.router_config.enable_error_bridge:
                self.logger.info(
                    "router_error_bridge_disabled",
                    exchange=router.exchange_name,
                )
                return None

            # Get or create stream error handler
            stream_error_handler = self._get_stream_error_handler(router)
            if not stream_error_handler:
                if self.router_config.bridge_fallback_to_legacy:
                    self.logger.warning(
                        "no_stream_error_handler_fallback_enabled",
                        exchange=router.exchange_name,
                    )
                    return None
                raise RouterConfigurationError(
                    "Stream error handler required but not available", "stream_error_handler"
                )

            # Create router error bridge
            error_bridge = RouterErrorBridge(
                router=router,
                stream_error_handler=stream_error_handler,
                legacy_error_handler=legacy_error_handler,
            )

            # Validate bridge configuration
            self._validate_bridge_configuration(error_bridge)

            self.logger.info(
                "router_error_bridge_configured",
                exchange=router.exchange_name,
                bridge_available=error_bridge.is_available(),
                fallback_enabled=self.router_config.bridge_fallback_to_legacy,
            )

            return error_bridge

        except Exception as e:
            error_msg = f"Failed to configure router error handling: {e}"
            self.logger.error(
                "router_configuration_failed",
                exchange=router.exchange_name,
                error=str(e),
                error_type=type(e).__name__,
            )

            if isinstance(e, RouterConfigurationError):
                raise

            raise RouterConfigurationError(error_msg) from e

    def _get_stream_error_handler(
        self, router: BaseWebSocketRouter[EnvelopeT]
    ) -> WebSocketStreamErrorHandler | None:
        """Get stream error handler from router or create one.

        Args:
            router: Router instance

        Returns:
            Stream error handler or None if not available
        """
        # Use router's existing stream error handler if available
        if hasattr(router, "stream_error_handler") and router.stream_error_handler:
            return router.stream_error_handler

        # Could create a new one here based on configuration
        # For now, return None to indicate unavailable
        return None

    def _validate_bridge_configuration(
        self,
        error_bridge: RouterErrorBridge[EnvelopeT],
    ) -> None:
        """Validate error bridge configuration.

        Args:
            error_bridge: Error bridge to validate

        Raises:
            RouterConfigurationError: If configuration is invalid
        """
        if not error_bridge.is_available():
            raise RouterConfigurationError(
                "Error bridge is not available after configuration", "error_bridge_availability"
            )

        bridge_info = error_bridge.get_bridge_info()

        # Validate required components
        if not bridge_info.get("stream_handler_available", False):
            raise RouterConfigurationError(
                "Stream error handler not available in bridge", "stream_handler_available"
            )

        if not bridge_info.get("legacy_handler_available", False):
            raise RouterConfigurationError(
                "Legacy error handler not available in bridge", "legacy_handler_available"
            )

    def get_envelope_validation_config(self) -> dict[str, Any]:
        """Get envelope validation configuration.

        Returns:
            Dictionary with validation configuration
        """
        return {
            "strict_validation": self.router_config.strict_envelope_validation,
            "log_failures": self.router_config.log_envelope_validation_failures,
            "timeout_ms": self.router_config.envelope_validation_timeout_ms,
        }

    def get_routing_key_config(self) -> dict[str, Any]:
        """Get routing key configuration.

        Returns:
            Dictionary with routing key configuration
        """
        return {
            "allow_empty": self.router_config.allow_empty_routing_keys,
            "max_length": self.router_config.routing_key_max_length,
            "log_missing": self.router_config.log_missing_routing_keys,
        }

    def get_processor_config(self) -> dict[str, Any]:
        """Get processor configuration.

        Returns:
            Dictionary with processor configuration
        """
        return {
            "log_missing_processors": self.router_config.log_missing_processors,
            "log_missing_handlers": self.router_config.log_missing_handlers,
            "processor_timeout_ms": self.router_config.processor_lookup_timeout_ms,
            "handler_timeout_ms": self.router_config.handler_lookup_timeout_ms,
        }

    def get_message_send_config(self) -> dict[str, Any]:
        """Get message send configuration.

        Returns:
            Dictionary with message send configuration
        """
        return {
            "enable_tracking": self.router_config.enable_message_send_error_tracking,
            "timeout_ms": self.router_config.message_send_timeout_ms,
            "retry_attempts": self.router_config.message_send_retry_attempts,
        }

    def get_performance_config(self) -> dict[str, Any]:
        """Get performance configuration.

        Returns:
            Dictionary with performance configuration
        """
        return {
            "enable_tracking": self.router_config.enable_routing_performance_tracking,
            "warning_threshold_ms": self.router_config.routing_performance_warning_threshold_ms,
            "max_concurrent": self.router_config.max_concurrent_routing_operations,
        }

    def get_context_config(self) -> dict[str, Any]:
        """Get context creation configuration.

        Returns:
            Dictionary with context configuration
        """
        return {
            "enhanced_contexts": self.router_config.enable_enhanced_error_contexts,
            "include_metadata": self.router_config.include_message_metadata_in_contexts,
            "timeout_ms": self.router_config.context_creation_timeout_ms,
            "max_extra_data_bytes": self.router_config.max_context_extra_data_size_bytes,
        }

    def should_use_typed_error_handling(self) -> bool:
        """Check if typed error handling should be used.

        Returns:
            True if typed error handling is enabled
        """
        return self.config.enabled and self.router_config.enable_error_bridge

    def should_fallback_to_legacy(self) -> bool:
        """Check if fallback to legacy error handling is enabled.

        Returns:
            True if fallback is enabled
        """
        return self.router_config.bridge_fallback_to_legacy

    def get_bridge_timeout_ms(self) -> int:
        """Get bridge operation timeout in milliseconds.

        Returns:
            Timeout value in milliseconds
        """
        return self.router_config.bridge_error_timeout_ms

    def create_router_config_summary(
        self, router: BaseWebSocketRouter[EnvelopeT]
    ) -> dict[str, Any]:
        """Create configuration summary for router.

        Args:
            router: Router to create summary for

        Returns:
            Dictionary with configuration summary
        """
        return {
            "exchange": router.exchange_name,
            "router_type": type(router).__name__,
            "typed_error_handling_enabled": self.should_use_typed_error_handling(),
            "bridge_fallback_enabled": self.should_fallback_to_legacy(),
            "envelope_validation": self.get_envelope_validation_config(),
            "routing_key": self.get_routing_key_config(),
            "processor": self.get_processor_config(),
            "message_send": self.get_message_send_config(),
            "performance": self.get_performance_config(),
            "context": self.get_context_config(),
            "bridge_timeout_ms": self.get_bridge_timeout_ms(),
        }


class RouterConfigurationValidator:
    """Validates router configuration for consistency and completeness."""

    @staticmethod
    def validate_config(config: WebSocketErrorConfig) -> list[str]:
        """Validate router configuration.

        Args:
            config: Configuration to validate

        Returns:
            List of validation errors (empty if valid)
        """
        errors: list[str] = []
        router_config = config.router

        # Check timeout consistency
        if router_config.bridge_error_timeout_ms < router_config.envelope_validation_timeout_ms:
            errors.append("Bridge timeout must be >= envelope validation timeout")

        if router_config.context_creation_timeout_ms > router_config.bridge_error_timeout_ms:
            errors.append("Context creation timeout must be <= bridge timeout")

        # Check performance settings
        if router_config.max_concurrent_routing_operations < 1:
            errors.append("Max concurrent routing operations must be >= 1")

        if router_config.routing_performance_warning_threshold_ms < 1:
            errors.append("Routing performance warning threshold must be >= 1ms")

        # Check size limits
        if router_config.max_context_extra_data_size_bytes < 1024:
            errors.append("Max context extra data size must be >= 1024 bytes")

        if router_config.routing_key_max_length < 1:
            errors.append("Routing key max length must be >= 1")

        # Check retry settings
        if router_config.message_send_retry_attempts < 0:
            errors.append("Message send retry attempts must be >= 0")

        return errors

    @staticmethod
    def validate_router_compatibility(
        router: BaseWebSocketRouter[Any], config: WebSocketErrorConfig
    ) -> list[str]:
        """Validate router compatibility with configuration.

        Args:
            router: Router to check
            config: Configuration to validate against

        Returns:
            List of compatibility issues (empty if compatible)
        """
        issues: list[str] = []
        router_config = config.router

        # Check if router supports required features
        if router_config.enable_error_bridge and not hasattr(router, "stream_error_handler"):
            issues.append(
                "Router does not support stream error handler but error bridge is enabled"
            )

        if router_config.strict_envelope_validation and not hasattr(router, "envelope_validator"):
            issues.append(
                "Router does not support envelope validator but strict validation is enabled"
            )

        if router_config.enable_routing_performance_tracking and not hasattr(
            router, "metrics_collector"
        ):
            issues.append(
                "Router does not support metrics collector but performance tracking is enabled"
            )

        return issues
