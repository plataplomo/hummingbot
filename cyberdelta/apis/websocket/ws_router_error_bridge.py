"""Router Error Bridge for typed WebSocket error handling.

This module provides a bridge between the WebSocket router and the typed
error system, enabling seamless integration of router error handling with
the WebSocket stream error handler while maintaining fallback compatibility.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar

from pydantic import BaseModel

from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_exceptions import WebSocketValidationError
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.apis.websocket.ws_router_error_context import RouterErrorContextBuilder
from cyberdelta.apis.websocket.ws_stream_error_handler import WebSocketStreamErrorHandler
from cyberdelta.config.structlog_config import get_logger


if TYPE_CHECKING:
    from cyberdelta.apis.websocket.ws_error_handler import BaseErrorHandler
    from cyberdelta.apis.websocket.ws_router import BaseWebSocketRouter

# Type variable for any BaseModel envelope type
EnvelopeT = TypeVar("EnvelopeT", bound=BaseModel)


class RouterErrorBridge[EnvelopeT: BaseModel]:
    """Bridge between router errors and typed WebSocket error system.

    This bridge provides a centralized way to handle router errors using the
    typed WebSocket error system, while maintaining backward compatibility
    with the legacy dict-based error handling system.

    The bridge follows the same pattern as ProcessorErrorBridge, providing
    typed error handling for all router error scenarios.
    """

    def __init__(
        self,
        router: BaseWebSocketRouter[EnvelopeT],
        stream_error_handler: WebSocketStreamErrorHandler,
        legacy_error_handler: BaseErrorHandler,
    ) -> None:
        """Initialize the router error bridge.

        Args:
            router: The WebSocket router instance
            stream_error_handler: Typed WebSocket stream error handler
            legacy_error_handler: Legacy dict-based error handler for fallback
        """
        self.router = router
        self.stream_error_handler = stream_error_handler
        self.legacy_error_handler = legacy_error_handler
        self.logger = get_logger(f"RouterErrorBridge.{router.exchange_name}")

    async def handle_envelope_validation_error(
        self,
        error: Exception,
        message: dict[str, Any],
    ) -> None:
        """Handle envelope validation errors with typed error system.

        Args:
            error: The validation error that occurred
            message: The raw message that failed validation
        """
        try:
            # Create typed error context using RouterErrorContextBuilder
            error_context = RouterErrorContextBuilder.from_envelope_validation_error(
                router=self.router,
                message=message,
                validation_error=error,
                envelope_type=type(error).__name__,
            )

            # Create typed WebSocket error
            ws_error = WebSocketValidationError(
                message=f"Invalid message envelope format: {error}",
                context=error_context,
                field="envelope",
                value=message,
                code=WebSocketErrorCode.VALIDATION_FAILED,
                cause=error,
            )

            # Handle with typed error system
            await self.stream_error_handler.handle_stream_error(ws_error)

            self.logger.debug(
                "envelope_validation_error_handled",
                exchange=self.router.exchange_name,
                error_type=type(error).__name__,
                message_keys=list(message.keys()),
            )

        except Exception as bridge_error:
            # Bridge failure - fall back to legacy system
            self.logger.warning(
                "router_error_bridge_failed",
                bridge_error=str(bridge_error),
                original_error=str(error),
                exchange=self.router.exchange_name,
            )

            # Fallback to legacy error handling
            await self.legacy_error_handler.handle_unroutable_message(
                message=message,
                reason=f"Invalid message envelope format: {error}",
                context={
                    "exchange": self.router.exchange_name,
                    "validation_error": str(error),
                    "error_type": type(error).__name__,
                    "message_keys": list(message.keys()),
                },
            )

    async def handle_missing_routing_key_error(
        self,
        message: dict[str, Any],
        envelope: EnvelopeT,
    ) -> None:
        """Handle missing routing key errors with typed error system.

        Args:
            message: The raw message that couldn't be routed
            envelope: The validated envelope that lacks a routing key
        """
        try:
            # Create typed error context using RouterErrorContextBuilder
            error_context = RouterErrorContextBuilder.from_missing_routing_key_error(
                router=self.router,
                message=message,
                envelope=envelope,
            )

            # Create typed WebSocket error
            ws_error = WebSocketValidationError(
                message="Unable to extract routing key from validated envelope",
                context=error_context,
                field="routing_key",
                value=envelope,
                code=WebSocketErrorCode.ROUTER_ERROR,
                cause=ValueError("No routing key found in envelope"),
            )

            # Handle with typed error system
            await self.stream_error_handler.handle_stream_error(ws_error)

            self.logger.debug(
                "missing_routing_key_error_handled",
                exchange=self.router.exchange_name,
                envelope_type=type(envelope).__name__,
            )

        except Exception as bridge_error:
            # Bridge failure - fall back to legacy system
            self.logger.warning(
                "router_error_bridge_failed",
                bridge_error=str(bridge_error),
                exchange=self.router.exchange_name,
            )

            # Fallback to legacy error handling
            await self.legacy_error_handler.handle_unroutable_message(
                message=message,
                reason="Unable to extract routing key from validated envelope",
                context={
                    "exchange": self.router.exchange_name,
                    "envelope_type": type(envelope).__name__,
                },
            )

    async def handle_missing_processor_error(
        self,
        routing_key: str,
        payload: dict[str, Any] | list[Any],
        context: WebSocketContextProtocol,
    ) -> None:
        """Handle missing processor errors with typed error system.

        Args:
            routing_key: The routing key that has no processor
            payload: The message payload that couldn't be processed
            context: The typed WebSocket context
        """
        try:
            # Create typed error context using RouterErrorContextBuilder
            error_context = RouterErrorContextBuilder.from_missing_processor_error(
                router=self.router,
                routing_key=routing_key,
                payload=payload,
                context=context,
            )

            # Create typed WebSocket error
            ws_error = WebSocketValidationError(
                message=f"No processor found for routing key: {routing_key}",
                context=error_context,
                field="routing_key",
                value=routing_key,
                code=WebSocketErrorCode.PROCESSOR_ERROR,
                cause=ValueError(f"No processor found for routing key: {routing_key}"),
            )

            # Handle with typed error system
            await self.stream_error_handler.handle_stream_error(ws_error)

            self.logger.debug(
                "missing_processor_error_handled",
                exchange=self.router.exchange_name,
                routing_key=routing_key,
                available_processors=list(self.router.processors.keys()),
            )

        except Exception as bridge_error:
            # Bridge failure - fall back to legacy system
            self.logger.warning(
                "router_error_bridge_failed",
                bridge_error=str(bridge_error),
                routing_key=routing_key,
                exchange=self.router.exchange_name,
            )

            # Fallback to legacy error handling
            # For missing processor, we use a simple context dict since we don't have a ValidationError
            context_dict = {
                "exchange": self.router.exchange_name,
                "routing_key": routing_key,
                "connection_id": context.connection_id,
                "error_type": "missing_processor",
            }
            # Ensure payload is a dict for legacy handler
            if isinstance(payload, list):
                payload_dict = {"data": payload}
            else:
                payload_dict = payload
            await self.legacy_error_handler.handle_processing_error(
                error=ValueError(f"No processor found for routing key: {routing_key}"),
                payload=payload_dict,
                context=context_dict,
            )

    async def handle_missing_handler_error(
        self,
        routing_key: str,
        message: dict[str, Any],
        available_handlers: list[str],
    ) -> None:
        """Handle missing handler errors with typed error system.

        Args:
            routing_key: The routing key that has no handler
            message: The raw message that couldn't be handled
            available_handlers: List of available handler keys
        """
        try:
            # Create typed error context using RouterErrorContextBuilder
            error_context = RouterErrorContextBuilder.from_missing_handler_error(
                router=self.router,
                routing_key=routing_key,
                message=message,
                available_handlers=available_handlers,
            )

            # Create typed WebSocket error
            ws_error = WebSocketValidationError(
                message=f"No handler found for routing key: {routing_key}",
                context=error_context,
                field="routing_key",
                value=routing_key,
                code=WebSocketErrorCode.HANDLER_ERROR,
                cause=ValueError(f"No handler registered for routing key: {routing_key}"),
            )

            # Handle with typed error system
            await self.stream_error_handler.handle_stream_error(ws_error)

            self.logger.debug(
                "missing_handler_error_handled",
                exchange=self.router.exchange_name,
                routing_key=routing_key,
                available_handlers=available_handlers,
            )

        except Exception as bridge_error:
            # Bridge failure - fall back to legacy warning logging
            self.logger.warning(
                "router_error_bridge_failed_handler_lookup",
                bridge_error=str(bridge_error),
                routing_key=routing_key,
                exchange=self.router.exchange_name,
                available_handlers=available_handlers,
            )

            # Fallback to simple warning logging (original behavior)
            self.logger.warning(
                "no_handler_for_routing_key",
                exchange=self.router.exchange_name,
                routing_key=routing_key,
                available_handlers=available_handlers,
            )

    async def handle_routing_error(
        self,
        error: Exception,
        message: dict[str, Any],
        routing_stage: str = "general_routing",
    ) -> None:
        """Handle general routing errors with typed error system.

        Args:
            error: The routing error that occurred
            message: The raw message being routed when error occurred
            routing_stage: The stage of routing where error occurred
        """
        try:
            # Create typed error context using RouterErrorContextBuilder
            error_context = RouterErrorContextBuilder.from_routing_error(
                router=self.router,
                error=error,
                message=message,
                routing_stage=routing_stage,
            )

            # Create typed WebSocket error
            ws_error = WebSocketValidationError(
                message=f"WebSocket routing error: {error!s}",
                context=error_context,
                field="message_routing",
                value=message,
                code=WebSocketErrorCode.ROUTER_ERROR,
                cause=error,
            )

            # Handle with typed error system
            await self.stream_error_handler.handle_stream_error(ws_error)

            self.logger.debug(
                "routing_error_handled",
                exchange=self.router.exchange_name,
                routing_stage=routing_stage,
                error_type=type(error).__name__,
            )

        except Exception as bridge_error:
            # Bridge failure - fall back to legacy system
            self.logger.warning(
                "router_error_bridge_failed",
                bridge_error=str(bridge_error),
                original_error=str(error),
                routing_stage=routing_stage,
                exchange=self.router.exchange_name,
            )

            # Fallback to legacy error handling
            await self.legacy_error_handler.handle_routing_error(error, message)

    async def handle_message_send_failure(
        self,
        message: dict[str, Any],
        error: Exception,
    ) -> None:
        """Handle message send failures with typed error system.

        Args:
            message: The message that failed to send
            error: The send failure reason
        """
        try:
            # Create typed error context using RouterErrorContextBuilder
            error_context = RouterErrorContextBuilder.from_routing_error(
                router=self.router,
                error=error,
                message=message,
                routing_stage="message_send",
            )

            # Create typed WebSocket error
            ws_error = WebSocketValidationError(
                message=f"Failed to send WebSocket message: {error}",
                context=error_context,
                field="message_send",
                value=message,
                code=WebSocketErrorCode.ROUTER_ERROR,
                cause=error,
            )

            # Handle with typed error system
            await self.stream_error_handler.handle_stream_error(ws_error)

            self.logger.debug(
                "message_send_failure_handled",
                exchange=self.router.exchange_name,
                error_type=type(error).__name__,
            )

        except Exception as bridge_error:
            # Bridge failure - just log the original error
            self.logger.warning(
                "router_error_bridge_failed_message_send",
                bridge_error=str(bridge_error),
                original_error=str(error),
                exchange=self.router.exchange_name,
            )

    def is_available(self) -> bool:
        """Check if the error bridge is available for use.

        Returns:
            True if both router and stream error handler are available
        """
        return hasattr(self.stream_error_handler, "handle_stream_error")

    def get_bridge_info(self) -> dict[str, object]:
        """Get information about the error bridge configuration.

        Returns:
            Dictionary with bridge configuration information
        """
        return {
            "bridge_type": "RouterErrorBridge",
            "exchange": self.router.exchange_name,
            "router_type": type(self.router).__name__,
            "stream_handler_available": True,  # Always available in bridge
            "legacy_handler_available": True,  # Always available in bridge,
            "is_available": self.is_available(),
        }
