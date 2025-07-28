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
    ErrorRecoveryMode,
    MemoryOptimizationMode,
)
from cyberdelta.apis.common.api_error_codes import APIErrorCode
from cyberdelta.apis.websocket.ws_context import (
    ExchangeType,
    WebSocketMessageContext,
)
from cyberdelta.apis.websocket.ws_error_recovery import (
    ConnectionRecovery,
    ErrorRecoveryConfig,
    WebSocketErrorRecovery,
)
from cyberdelta.apis.websocket.ws_memory_optimized import (
    MemoryOptimizedMessageContext,
    MemoryPool,
)
from cyberdelta.apis.websocket.ws_metrics import WebSocketMetricsCollector
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.apis.websocket.ws_typed_processor import TypeSafeWebSocketProcessor
from cyberdelta.apis.websocket.ws_validators import WebSocketPayloadValidators
from cyberdelta.config.structlog_config import get_logger


# Type variable for context types
ContextType = TypeVar("ContextType", bound="WebSocketMessageContext[BaseModel]")

# Message handler type - takes typed context
MessageHandler = Callable[[WebSocketContextProtocol], Awaitable[None]]


class EnvelopeValidatorNotSetError(ValueError):
    """Raised when envelope validator is required but not set."""

    def __init__(self) -> None:
        """Initialize with descriptive message."""
        super().__init__("Envelope validator is required but not set")


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


if TYPE_CHECKING:
    from cyberdelta.apis.websocket.ws_error_handler import BaseErrorHandler


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
        exchange_name: str,
        exchange_type: ExchangeType,
        error_handler: BaseErrorHandler,
        typed_processor: TypeSafeWebSocketProcessor,
        envelope_validator: Callable[[dict[str, Any]], EnvelopeType] | None = None,
        payload_validator: WebSocketPayloadValidators | None = None,
        metrics_collector: WebSocketMetricsCollector | None = None,
        recovery_config: ErrorRecoveryConfig | None = None,
        error_recovery_mode: ErrorRecoveryMode = ErrorRecoveryMode.ENABLED,
        memory_optimization_mode: MemoryOptimizationMode = MemoryOptimizationMode.DISABLED,
        memory_pool_size: int = 1000,
    ) -> None:
        """Initialize the WebSocket router.

        Args:
            exchange_name: Name of the exchange for logging and identification.
            exchange_type: Type of exchange (e.g., ExchangeType.BACKPACK).
            error_handler: Error handler for centralized error management.
            typed_processor: Required typed processor (use WebSocketRegistryFactory to create).
            envelope_validator: Optional envelope validator for type-safe message validation.
            payload_validator: Optional payload validator (default instance created if None).
            metrics_collector: Optional metrics collector for monitoring.
            recovery_config: Configuration for error recovery system.
            error_recovery_mode: Mode for automatic error recovery.
            memory_optimization_mode: Mode for memory optimization in
                high-frequency scenarios.
            memory_pool_size: Size of the memory pool for object reuse.

        """
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.error_handler = error_handler
        self.envelope_validator = envelope_validator
        self.payload_validator = payload_validator or WebSocketPayloadValidators()
        self.metrics_collector = metrics_collector or WebSocketMetricsCollector(exchange_name)

        # Store the required typed processor
        self.typed_processor = typed_processor
        self.logger = get_logger(f"WebSocketRouter.{exchange_name}")
        self._connection_id = str(uuid.uuid4())[:8]  # Short connection ID for context

        # Error recovery system
        self.error_recovery_mode = error_recovery_mode
        self.error_recovery: WebSocketErrorRecovery | None
        if error_recovery_mode.is_enabled:
            recovery_config = recovery_config or ErrorRecoveryConfig()
            self.error_recovery = WebSocketErrorRecovery(self._connection_id, recovery_config)
        else:
            self.error_recovery = None

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
            error_recovery_mode=error_recovery_mode.value,
            memory_optimization_mode=memory_optimization_mode.value,
            memory_pool_size=memory_pool_size if memory_optimization_mode.is_enabled else None,
        )

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
            exchange=self.exchange_name,
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
        """
        if self.memory_pool is None:
            # Fallback to direct creation if pool not available
            return MemoryOptimizedMessageContext(
                envelope_type=envelope_type,
                routing_key=routing_key,
                message_id=message_id,
                connection_id=self._connection_id,
                symbol=symbol,
            )

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
        await self.error_handler.handle_unroutable_message(
            message=message,
            reason=f"Invalid message envelope format: {error}",
            context={
                "exchange": self.exchange_name,
                "validation_error": str(error),
                "error_type": type(error).__name__,
                "message_keys": list(message.keys()),  # message is guaranteed to be dict
            },
        )

    async def _handle_missing_routing_key(
        self,
        message: dict[str, Any],
        envelope: EnvelopeType,
    ) -> None:
        """Handle case where routing key cannot be extracted."""
        await self.error_handler.handle_unroutable_message(
            message=message,
            reason="Unable to extract routing key from validated envelope",
            context={
                "exchange": self.exchange_name,
                "envelope_type": type(envelope).__name__,
            },
        )

    async def _handle_missing_handler(
        self,
        message: dict[str, Any],
        routing_key: str,
        handlers: dict[str, MessageHandler],
    ) -> None:
        """Handle case where no handler is registered."""
        self.logger.warning(
            "no_handler_for_routing_key",
            exchange=self.exchange_name,
            routing_key=routing_key,
            available_handlers=list(handlers.keys()),
        )

    async def _handle_missing_processor(
        self,
        routing_key: str,
        payload: dict[str, Any] | list[Any],
        context: WebSocketContextProtocol,
    ) -> None:
        """Handle case where no processor is found."""
        # Ensure payload is dict for error handler
        payload_dict = payload if isinstance(payload, dict) else {"data": payload}
        # Convert typed context to dict for error handler (temporary until error handler is updated)
        context_dict = context.model_dump(mode="python")
        await self.error_handler.handle_processing_error(
            error=ValueError(f"No processor found for routing key: {routing_key}"),
            payload=payload_dict,
            context=context_dict,
        )

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
            await self.error_handler.handle_routing_error(e, message)
            # Notify error recovery system if enabled
            if self.error_recovery:
                # Create structured WebSocketError for better error handling
                websocket_error = WebSocketErrorRecovery.create_websocket_error(
                    message=f"WebSocket routing error: {e!s}",
                    error_code=APIErrorCode.NETWORK_ISSUE,
                    original_exception=e,
                    metadata={
                        "exchange": self.exchange_name,
                        "message_keys": list(message.keys()),
                        "error_type": type(e).__name__,
                    },
                )
                await self.error_recovery.handle_connection_error(websocket_error)

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
            # Notify error recovery of successful operation
            if self.error_recovery:
                await self.error_recovery.handle_successful_operation()
        else:
            await self._handle_missing_processor(routing_key, payload, typed_context)

    def get_processor_info(self) -> dict[str, Any]:
        """Get information about registered processors.

        Returns:
            Dictionary with processor information.

        """
        return {
            "exchange": self.exchange_name,
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
            exchange=self.exchange_name,
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
                exchange=self.exchange_name,
                routing_key=routing_key,
            )
            return True
        return False

    async def start_error_recovery(self, connection: ConnectionRecovery) -> None:
        """Start error recovery system with connection implementation.

        Args:
            connection: Connection implementation that implements ConnectionRecovery protocol
        """
        if self.error_recovery:
            await self.error_recovery.start_recovery(connection)
            self.logger.info(
                "error_recovery_started",
                exchange=self.exchange_name,
                connection_id=self._connection_id,
            )

    async def stop_error_recovery(self) -> None:
        """Stop error recovery system."""
        if self.error_recovery:
            await self.error_recovery.stop_recovery()
            self.logger.info(
                "error_recovery_stopped",
                exchange=self.exchange_name,
                connection_id=self._connection_id,
            )

    async def handle_successful_operation(self) -> None:
        """Notify error recovery system of successful operation."""
        if self.error_recovery:
            await self.error_recovery.handle_successful_operation()

    async def handle_message_send_failure(self, message: dict[str, Any], error: Exception) -> None:
        """Handle message sending failure.

        Args:
            message: Failed message
            error: Failure reason
        """
        if self.error_recovery:
            await self.error_recovery.handle_message_failure(message, error)

    def get_connection_health(self) -> dict[str, Any] | None:
        """Get current connection health status.

        Returns:
            Health status dict or None if recovery is disabled
        """
        if self.error_recovery:
            health = self.error_recovery.get_health_status()
            return health.model_dump()
        return None

    def get_recovery_stats(self) -> dict[str, Any] | None:
        """Get error recovery statistics.

        Returns:
            Recovery stats dict or None if recovery is disabled
        """
        if self.error_recovery:
            return self.error_recovery.get_recovery_stats()
        return None

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
            "exchange": self.exchange_name,
            "processors": self.get_processor_info(),
            "connection_id": self._connection_id,
        }

        # Add error recovery stats if available
        recovery_stats = self.get_recovery_stats()
        if recovery_stats:
            stats["error_recovery"] = recovery_stats

        # Add memory optimization stats if available
        memory_stats = self.get_memory_stats()
        if memory_stats:
            stats["memory_optimization"] = memory_stats

        # Add connection health if available
        health_stats = self.get_connection_health()
        if health_stats:
            stats["connection_health"] = health_stats

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
            exchange=self.exchange_name,
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
            exchange=self.exchange_name,
            connection_id=self._connection_id,
        )
        return True
