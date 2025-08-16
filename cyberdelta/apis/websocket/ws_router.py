"""Base WebSocket Router with Type-Safe Message Handling.

This module provides abstract base classes for WebSocket message routing
with support for generic type parameters and centralized error handling.
"""

from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

from pydantic import BaseModel, ValidationError

from cyberdelta.apis.base.infrastructure_config_domain import (
    MemoryOptimizationMode,
)
from cyberdelta.apis.enums.websocket import WebSocketErrorCode

# Old error_recovery imports removed - using unified recovery system
from cyberdelta.apis.websocket.error_handling.stream_error_handler import (
    WebSocketStreamErrorHandler,
)
from cyberdelta.apis.websocket.exceptions import (
    EnvelopeValidatorNotSetError,
    WebSocketValidationError,
)
from cyberdelta.apis.websocket.memory.memory_optimized import (
    MemoryOptimizedMessageContext,
    MemoryPool,
)
from cyberdelta.apis.websocket.metrics.general_metrics import WebSocketMetricsCollector
from cyberdelta.apis.websocket.security.validators import WebSocketPayloadValidators
from cyberdelta.apis.websocket.ws_context import WebSocketMessageContext
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.apis.websocket.ws_router_error_context import RouterErrorContextBuilder
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import ExchangeName


if TYPE_CHECKING:
    from cyberdelta.apis.websocket.ws_typed_processor import TypeSafeWebSocketProcessor


# Type variable for context types
ContextType = TypeVar("ContextType", bound="WebSocketMessageContext[BaseModel]")

# Message handler type - takes typed context
MessageHandler = Callable[[WebSocketContextProtocol], Awaitable[None]]


class MessageProcessor(Protocol):
    """Protocol for message processors."""

    async def process(
        self,
        payload: dict[str, Any] | list[Any],
        handler: MessageHandler,
        context: WebSocketContextProtocol,
    ) -> None:
        """Process a message payload with typed context."""
        ...


# BaseErrorHandler import removed - deprecated and not used


class BaseWebSocketRouter[EnvelopeType: BaseModel](ABC):
    """Abstract base class for WebSocket message routing with type safety.

    This class provides a framework for routing WebSocket messages with:
    - Type-safe message processing using TypeSafeWebSocketProcessor
    - Centralized error handling
    - Exchange-agnostic abstractions
    - Comprehensive logging
    - Automatic typed context creation based on message format
    """

    def __init__(
        self,
        exchange_name: ExchangeName,
        typed_processor: TypeSafeWebSocketProcessor,
        stream_error_handler: WebSocketStreamErrorHandler,
        envelope_validator: Callable[[dict[str, Any]], EnvelopeType] | None = None,
        payload_validator: WebSocketPayloadValidators | None = None,
        metrics_collector: WebSocketMetricsCollector | None = None,
        memory_optimization_mode: MemoryOptimizationMode = MemoryOptimizationMode.DISABLED,
        memory_pool_size: int = 1000,
    ) -> None:
        """Initialize the WebSocket router.

        Args:
            exchange_name: ExchangeName enum for the exchange.
            typed_processor: Required typed processor (use WebSocketRegistryFactory to create).
            envelope_validator: Optional envelope validator for type-safe message validation.
            payload_validator: Optional payload validator (default instance created if None).
            metrics_collector: Optional metrics collector for monitoring.
            stream_error_handler: Optional typed WebSocket stream error handler.
            memory_optimization_mode: Mode for memory optimization in
                high-frequency scenarios.
            memory_pool_size: Size of the memory pool for object reuse.

        """
        self.exchange_name = exchange_name  # Store enum for proper typing
        self.envelope_validator = envelope_validator
        self.payload_validator = payload_validator or WebSocketPayloadValidators()
        self.metrics_collector = metrics_collector or WebSocketMetricsCollector(exchange_name)
        self.stream_error_handler = stream_error_handler

        # Store the required typed processor
        self.typed_processor = typed_processor
        self.logger = get_logger(f"WebSocketRouter.{exchange_name.value}")
        self._connection_id = str(uuid.uuid4())[:8]  # Short connection ID for context

        # Error recovery is handled by stream_error_handler using unified recovery system

        # Memory optimization system
        self.memory_optimization_mode = memory_optimization_mode
        self.memory_pool: MemoryPool | None
        if memory_optimization_mode.is_enabled:
            self.memory_pool = MemoryPool(pool_size=memory_pool_size)
        else:
            self.memory_pool = None

        # Message processors registry
        self.processors: dict[str, Any] = {}

        # Setup exchange-specific processors
        self._setup_processors()

        self.logger.info(
            "websocket_router_initialized",
            exchange=exchange_name,
            processors=list(self.processors.keys()),
            memory_optimization_mode=memory_optimization_mode.value,
            memory_pool_size=memory_pool_size if memory_optimization_mode.is_enabled else None,
        )

    @property
    def connection_id(self) -> str:
        """Get the connection ID.

        Returns:
            The connection ID string.
        """
        return self._connection_id

    @abstractmethod
    def _setup_processors(self) -> None:
        """Setup exchange-specific message processors.

        This method should populate self.processors with routing_key -> processor
        mappings for the specific exchange implementation.
        """

    @abstractmethod
    def _extract_routing_key_from_envelope(self, envelope: EnvelopeType) -> str | None:
        """Extract routing key from validated envelope for processor lookup.

        Args:
            envelope: The validated WebSocket envelope to extract routing key from.

        Returns:
            Routing key string or None if no valid key found.

        """

    @abstractmethod
    def _extract_payload_from_envelope(self, envelope: EnvelopeType) -> dict[str, Any] | list[Any]:
        """Extract the payload data from the validated envelope.

        Args:
            envelope: The validated WebSocket envelope to extract payload from.

        Returns:
            The payload data (dict, list, or other type).

        """

    def _validate_message_structure(self, message: dict[str, Any]) -> dict[str, Any]:
        """Validate basic message structure.

        Args:
            message: The message to validate.

        Returns:
            The validated message.
        """
        # Basic structure validation
        validated = self.payload_validator.validate_dict_payload(message, "WebSocket message")

        # Log message info for debugging
        self.logger.debug(
            "processing_websocket_message",
            exchange=self.exchange_name.value,
            message_keys=list(validated.keys()),
            message_size=len(str(validated)),
        )

        return validated

    def _create_typed_context(
        self,
        envelope: EnvelopeType,
        routing_key: str,
        message_id: str,
    ) -> WebSocketContextProtocol:
        """Create typed context using TypeSafeWebSocketProcessor.

        This method uses the centralized typed processor to create properly
        typed contexts based on the envelope format, eliminating the need
        for each exchange to implement its own context creation logic.

        Args:
            envelope: Validated envelope
            routing_key: Extracted routing key
            message_id: Message ID

        Returns:
            Properly typed context (BackpackMessageContext or HyperliquidMessageContext)
        """
        # Convert envelope to dict for typed processor
        raw_data = envelope.model_dump(mode="python")

        # Create typed context using the centralized processor
        return self.typed_processor.create_typed_context(
            raw_data=raw_data,
            connection_id=self._connection_id,
            message_id=message_id,
        )

    def _create_memory_optimized_context(
        self,
        envelope_type: str,
        routing_key: str,
        message_id: str,
        symbol: str | None = None,
    ) -> MemoryOptimizedMessageContext:
        """Create memory-optimized context for high-frequency scenarios.

        Args:
            envelope_type: Type of the envelope
            routing_key: Extracted routing key
            message_id: Message ID
            symbol: Extracted symbol/coin if available

        Returns:
            Memory-optimized context with minimal overhead

        Raises:
            RuntimeError: If memory pool is not available but required for optimization.
        """
        if self.memory_pool is None:
            raise RuntimeError("Pool")

        return self.memory_pool.get_context(
            envelope_type=envelope_type,
            routing_key=routing_key,
            message_id=message_id,
            connection_id=self._connection_id,
            symbol=symbol,
        )

    async def _enhance_typed_context(
        self,
        context: WebSocketContextProtocol,
        routing_key: str,
    ) -> WebSocketContextProtocol:
        """Allow exchanges to enhance typed context with exchange-specific data.

        By default, returns the context as-is since typed contexts already
        include computed fields for symbol/coin extraction.

        Args:
            context: Typed context
            routing_key: Message routing key

        Returns:
            Enhanced typed context
        """
        return context

    async def _handle_envelope_validation_error(
        self,
        error: Exception,
        message: dict[str, Any],
    ) -> None:
        """Standardized envelope validation error handling."""
        # Pure typed error system - stream error handler required
        # Create typed error context using RouterErrorContextBuilder
        error_context = RouterErrorContextBuilder.from_envelope_validation_error(
            router=self,
            message=message,
            validation_error=error,
            envelope_type=type(error).__name__,
        )

        # Create typed WebSocket error
        ws_error = WebSocketValidationError(
            message=f"Invalid message envelope format: {error}",
            context=error_context,
            code=WebSocketErrorCode.VALIDATION_FAILED,
            cause=error,
            field="envelope",
            value=message,
        )

        # Handle with typed error system
        await self.stream_error_handler.handle_stream_error(ws_error)

    async def _handle_missing_routing_key(
        self,
        message: dict[str, Any],
        envelope: EnvelopeType,
    ) -> None:
        """Handle case where routing key cannot be extracted."""
        # Pure typed error system - stream error handler required
        # Create typed error context using RouterErrorContextBuilder
        error_context = RouterErrorContextBuilder.from_missing_routing_key_error(
            router=self,
            message=message,
            envelope=envelope,
        )

        # Create typed WebSocket error
        ws_error = WebSocketValidationError(
            message="Unable to extract routing key from validated envelope",
            context=error_context,
            code=WebSocketErrorCode.ROUTER_ERROR,
            cause=ValueError("No routing key found in envelope"),
            field="routing_key",
            value=envelope,
        )

        # Handle with typed error system
        await self.stream_error_handler.handle_stream_error(ws_error)

    async def _handle_missing_handler(
        self,
        message: dict[str, Any],
        routing_key: str,
        handlers: dict[str, MessageHandler],
    ) -> None:
        """Handle case where no handler is registered."""
        # Pure typed error system - stream error handler required
        # Stream error handler is always available (required parameter)

        # Create typed error context using RouterErrorContextBuilder
        error_context = RouterErrorContextBuilder.from_missing_handler_error(
            router=self,
            routing_key=routing_key,
            message=message,
            available_handlers=list(handlers.keys()),
        )

        # Create typed WebSocket error
        ws_error = WebSocketValidationError(
            message=f"No handler found for routing key: {routing_key}",
            context=error_context,
            code=WebSocketErrorCode.HANDLER_ERROR,
            cause=ValueError(f"No handler registered for routing key: {routing_key}"),
            field="routing_key",
            value=routing_key,
        )

        # Handle with typed error system
        await self.stream_error_handler.handle_stream_error(ws_error)

    async def _handle_missing_processor(
        self,
        routing_key: str,
        payload: dict[str, Any] | list[Any],
        context: WebSocketContextProtocol,
    ) -> None:
        """Handle case where no processor is found."""
        # Pure typed error system - stream error handler required
        # Create typed error context using RouterErrorContextBuilder
        error_context = RouterErrorContextBuilder.from_missing_processor_error(
            router=self,
            routing_key=routing_key,
            payload=payload,
            context=context,
        )

        # Create typed WebSocket error
        error = WebSocketValidationError(
            message=f"No processor found for routing key: {routing_key}",
            context=error_context,
            code=WebSocketErrorCode.PROCESSOR_ERROR,
            cause=ValueError(f"No processor found for routing key: {routing_key}"),
            field="routing_key",
            value=routing_key,
        )

        # Handle with typed error system
        await self.stream_error_handler.handle_stream_error(error)

    async def route_message(
        self,
        message: dict[str, Any],
        handlers: dict[str, MessageHandler],
    ) -> None:
        """Route WebSocket message to appropriate processor and handler.

        Args:
            message: The WebSocket message to route.
            handlers: Dictionary mapping routing keys to handler functions.

        Raises:
            ValueError: If envelope validator is not set
        """
        if self.envelope_validator is None:
            msg = "Envelope validator is required"
            raise ValueError(msg)

        try:
            # Direct envelope-based routing only
            await self._route_with_envelope_validation(message, handlers)

        except (ValueError, TypeError, AttributeError, KeyError, RuntimeError) as e:
            # Pure typed error system - stream error handler required
            # Create typed error context using RouterErrorContextBuilder
            error_context = RouterErrorContextBuilder.from_routing_error(
                router=self,
                error=e,
                message=message,
                routing_stage="message_routing",
            )

            # Create typed WebSocket error
            ws_error = WebSocketValidationError(
                message=f"WebSocket routing error: {e!s}",
                context=error_context,
                code=WebSocketErrorCode.ROUTER_ERROR,
                cause=e,
                field="message_routing",
                value=message,
            )

            # Handle with typed error system
            await self.stream_error_handler.handle_stream_error(ws_error)

            # Error recovery is handled by stream_error_handler with unified recovery system

    async def _route_with_envelope_validation(
        self,
        message: dict[str, Any],
        handlers: dict[str, MessageHandler],
    ) -> None:
        """Enhanced route_message with built-in envelope validation.

        This method consolidates the envelope validation pattern used by both
        Backpack and Hyperliquid, eliminating code duplication.

        Raises:
            EnvelopeValidatorNotSetError: If envelope validator is not set
        """
        # Step 1: Validate envelope structure first - eliminates type safety issues
        try:
            # envelope_validator is guaranteed to be not None when this method is called
            if self.envelope_validator is None:
                raise EnvelopeValidatorNotSetError
            validated_envelope = self.envelope_validator(message)
        except (ValidationError, ValueError) as e:
            await self._handle_envelope_validation_error(e, message)
            return

        # Step 2: Extract routing key from validated envelope (type-safe!)
        routing_key = self._extract_routing_key_from_envelope(validated_envelope)

        if not routing_key:
            await self._handle_missing_routing_key(message, validated_envelope)
            return

        # Step 3: Get the appropriate handler
        handler = handlers.get(routing_key)
        if not handler:
            await self._handle_missing_handler(message, routing_key, handlers)
            return

        # Step 4: Extract payload from validated envelope (type-safe!)
        payload = self._extract_payload_from_envelope(validated_envelope)

        # Step 5: Create typed context with validated envelope
        message_id = str(uuid.uuid4())
        typed_context = self._create_typed_context(validated_envelope, routing_key, message_id)

        # Step 6: Allow exchanges to enhance typed context
        typed_context = await self._enhance_typed_context(typed_context, routing_key)

        # Step 7: Get processor and process the message with typed context
        processor = self.processors.get(routing_key)
        if processor:
            await processor.process(payload, handler, typed_context)
            # Success handling is managed by stream_error_handler unified recovery system
        else:
            await self._handle_missing_processor(routing_key, payload, typed_context)

    def get_processor_info(self) -> dict[str, Any]:
        """Get information about registered processors.

        Returns:
            Dictionary with processor information.

        """
        return {
            "exchange": self.exchange_name.value,
            "processors": {
                key: {
                    "type": type(processor).__name__,
                    "raw_model": (
                        raw_model.__name__
                        if (raw_model := getattr(processor, "raw_model", None)) is not None
                        else None
                    ),
                }
                for key, processor in self.processors.items()
            },
            "total_processors": len(self.processors),
        }

    def register_processor(self, routing_key: str, processor: MessageProcessor) -> None:
        """Register a message processor for a routing key.

        Args:
            routing_key: The key to route messages by.
            processor: The processor instance to handle messages.

        """
        self.processors[routing_key] = processor
        self.logger.debug(
            "processor_registered",
            exchange=self.exchange_name.value,
            routing_key=routing_key,
            processor_type=type(processor).__name__,
        )

    def unregister_processor(self, routing_key: str) -> bool:
        """Unregister a message processor.

        Args:
            routing_key: The routing key to unregister.

        Returns:
            True if processor was found and removed, False otherwise.

        """
        if routing_key in self.processors:
            del self.processors[routing_key]
            self.logger.debug(
                "processor_unregistered",
                exchange=self.exchange_name.value,
                routing_key=routing_key,
            )
            return True
        return False

    def get_memory_stats(self) -> dict[str, Any] | None:
        """Get memory optimization statistics.

        Returns:
            Memory stats dict or None if memory optimization is disabled
        """
        if self.memory_pool:
            return self.memory_pool.get_stats()
        return None

    def get_comprehensive_stats(self) -> dict[str, Any]:
        """Get comprehensive router statistics including all subsystems.

        Returns:
            Complete statistics dictionary
        """
        stats = {
            "exchange": self.exchange_name.value,
            "processors": self.get_processor_info(),
            "connection_id": self._connection_id,
        }

        # Add unified recovery system stats from error handler
        recovery_stats = self.stream_error_handler.get_statistics()
        if recovery_stats:
            stats["recovery_system"] = recovery_stats

        # Add memory optimization stats if available
        memory_stats = self.get_memory_stats()
        if memory_stats:
            stats["memory_optimization"] = memory_stats

        return stats

    def enable_high_frequency_mode(self) -> bool:
        """Enable high-frequency trading optimizations.

        This method enables memory optimization if not already enabled
        and configures the router for maximum performance.

        Returns:
            True if high-frequency mode was enabled, False if already enabled
        """
        if self.memory_optimization_mode == MemoryOptimizationMode.ENABLED:
            return False  # Already enabled

        self.memory_optimization_mode = MemoryOptimizationMode.ENABLED
        if self.memory_pool is None:
            self.memory_pool = MemoryPool(pool_size=2000)  # Larger pool for HFT

        self.logger.info(
            "high_frequency_mode_enabled",
            exchange=self.exchange_name.value,
            connection_id=self._connection_id,
            memory_pool_size=2000,
        )
        return True

    def disable_memory_optimization(self) -> bool:
        """Disable memory optimization and clear pools.

        Returns:
            True if memory optimization was disabled, False if already disabled
        """
        if self.memory_optimization_mode == MemoryOptimizationMode.DISABLED:
            return False  # Already disabled

        self.memory_optimization_mode = MemoryOptimizationMode.DISABLED
        if self.memory_pool:
            self.memory_pool.clear_pools()
            self.memory_pool = None

        self.logger.info(
            "memory_optimization_disabled",
            exchange=self.exchange_name.value,
            connection_id=self._connection_id,
        )
        return True
