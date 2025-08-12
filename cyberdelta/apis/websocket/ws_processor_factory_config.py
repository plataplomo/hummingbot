"""Enhanced processor factory with configuration support.

This module provides an enhanced factory for creating WebSocket processors
with full configuration support including the new typed error system.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from pydantic import BaseModel

from cyberdelta.apis.websocket.ws_error_handler_factory import WebSocketErrorHandlerFactory
from cyberdelta.apis.websocket.ws_processor import (
    MessageTransformer,
    PydanticWebSocketProcessor,
    SimpleDictTransformer,
)
from cyberdelta.config.models.websocket_error_config import WebSocketErrorConfig
from cyberdelta.config.models.websocket_processor_config import WebSocketProcessorConfig
from cyberdelta.config.structlog_config import get_logger


if TYPE_CHECKING:
    from cyberdelta.apis.websocket.ws_metrics import WebSocketMetricsCollector
    from cyberdelta.config.models.app_config import AppSettings

from cyberdelta.enums import ExchangeName


# Type variables for processor factory
T = TypeVar("T", bound=BaseModel)
U = TypeVar("U", bound=BaseModel)


class ConfiguredProcessorFactory:
    """Factory for creating configured WebSocket processors.

    This factory integrates with the application configuration system
    to create properly configured processors with the new error system.
    """

    def __init__(self, app_config: AppSettings) -> None:
        """Initialize factory with application configuration.

        Args:
            app_config: Application configuration
        """
        self.app_config = app_config
        self.websocket_error_config = app_config.websocket_error
        self.processor_config = app_config.websocket_processor
        self.logger = get_logger(__name__)

        # Create error handler factory
        self.error_handler_factory = WebSocketErrorHandlerFactory()

    def create_simple_processor(
        self,
        raw_model: type[T],
        exchange: ExchangeName,
        processor_name: str | None = None,
        metrics_collector: WebSocketMetricsCollector | None = None,
    ) -> PydanticWebSocketProcessor[T, T]:
        """Create a processor with no transformation and full configuration.

        Args:
            raw_model: The Pydantic model for validation
            exchange: Exchange name for configuration
            processor_name: Optional processor name
            metrics_collector: Optional metrics collector

        Returns:
            Configured processor instance with pure WebSocket error system
        """
        # Get exchange-specific configurations
        error_config = self.websocket_error_config.get_exchange_config(exchange)

        # Always create stream error handler (required for backwards removal architecture)
        stream_error_handler = self.error_handler_factory.create_handler(
            exchange=exchange,
            config=error_config,
        )
        self.logger.info(
            "Created typed error handler for processor",
            processor=processor_name or raw_model.__name__,
            exchange=exchange,
        )
        
        return PydanticWebSocketProcessor(
            raw_model=raw_model,
            transformer=SimpleDictTransformer[T](),
            processor_name=processor_name,
            metrics_collector=metrics_collector,
            stream_error_handler=stream_error_handler,
        )

    def create_processor(
        self,
        raw_model: type[T],
        transformer: MessageTransformer[T, U | list[U] | None],
        exchange: ExchangeName,
        processor_name: str | None = None,
        metrics_collector: WebSocketMetricsCollector | None = None,
    ) -> PydanticWebSocketProcessor[T, U]:
        """Create a processor with custom transformation and full configuration.

        Args:
            raw_model: The Pydantic model for validation
            transformer: Custom transformer instance
            exchange: Exchange name for configuration
            processor_name: Optional processor name
            metrics_collector: Optional metrics collector

        Returns:
            Configured processor instance with pure WebSocket error system
        """
        # Get exchange-specific configurations
        error_config = self.websocket_error_config.get_exchange_config(exchange)

        # Always create stream error handler (required for backwards removal architecture)
        stream_error_handler = self.error_handler_factory.create_handler(
            exchange=exchange,
            config=error_config,
        )
        self.logger.info(
            "Created typed error handler for processor",
            processor=processor_name or raw_model.__name__,
            exchange=exchange,
            transformer=type(transformer).__name__,
        )
        
        return PydanticWebSocketProcessor(
            raw_model=raw_model,
            transformer=transformer,
            processor_name=processor_name,
            metrics_collector=metrics_collector,
            stream_error_handler=stream_error_handler,
        )

    def get_processor_config(self, exchange: ExchangeName) -> WebSocketProcessorConfig:
        """Get processor configuration for an exchange.

        Args:
            exchange: Exchange name

        Returns:
            Processor configuration with exchange overrides applied
        """
        return self.processor_config.get_exchange_config(exchange)

    def get_error_config(self, exchange: ExchangeName) -> WebSocketErrorConfig:
        """Get error configuration for an exchange.

        Args:
            exchange: Exchange name

        Returns:
            Error configuration with exchange overrides applied
        """
        return self.websocket_error_config.get_exchange_config(exchange)
