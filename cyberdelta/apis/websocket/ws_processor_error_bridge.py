"""Processor Error Handler Bridge for WebSocket error system integration.

This module provides a bridge between PydanticWebSocketProcessor and the new
typed WebSocket error system, enabling seamless integration while maintaining
backward compatibility with legacy error handlers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ValidationError

from cyberdelta.apis.websocket.ws_exceptions import WebSocketValidationError
from cyberdelta.apis.websocket.ws_processor_error_context import ProcessorErrorContextBuilder
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.apis.websocket.ws_stream_error_handler import WebSocketStreamErrorHandler
from cyberdelta.config.structlog_config import get_logger


if TYPE_CHECKING:
    from cyberdelta.apis.websocket.ws_processor import PydanticWebSocketProcessor


class ProcessorErrorBridge:
    """Bridge between processor and new WebSocket error system.

    This class provides a clean interface for processor error handling,
    abstracting away the complexity of dual error system management
    and ensuring proper error context creation and routing.
    """

    def __init__(
        self,
        processor: PydanticWebSocketProcessor[Any, Any],
        stream_error_handler: WebSocketStreamErrorHandler,
    ) -> None:
        """Initialize the processor error bridge.

        Args:
            processor: The processor instance to bridge
            stream_error_handler: The typed WebSocket error handler
        """
        self.processor = processor
        self.stream_error_handler = stream_error_handler
        self.logger = get_logger(f"ProcessorErrorBridge.{processor.processor_name}")

    async def handle_validation_error(
        self,
        error: ValidationError,
        payload: dict[str, Any] | list[Any],
        context: WebSocketContextProtocol,
    ) -> None:
        """Handle validation errors with typed error system.

        Args:
            error: The validation error that occurred
            payload: The payload that failed validation
            context: WebSocket context protocol
        """
        try:
            # Create typed payload for error handler
            typed_payload: BaseModel
            if isinstance(payload, dict):
                typed_payload = self.processor.raw_model.model_construct(**payload)
            else:
                typed_payload = self.processor.raw_model.model_construct(data=payload)
        except (ValueError, TypeError, AttributeError):
            # If we can't construct a valid payload, create minimal model
            typed_payload = self.processor.raw_model.model_construct()

        # Use typed error handling
        await self.stream_error_handler.handle_validation_error(
            error=error,
            context=context,
            payload=typed_payload,
        )

        self.logger.debug(
            "validation_error_handled",
            processor=self.processor.processor_name,
            error_type=type(error).__name__,
            exchange=context.exchange_name,
        )

    async def handle_transformation_error(
        self,
        error: Exception,
        validated_payload: BaseModel,
        context: WebSocketContextProtocol,
    ) -> None:
        """Handle transformation errors with typed error system.

        Args:
            error: The transformation error that occurred
            validated_payload: The validated payload that failed transformation
            context: WebSocket context protocol
        """
        # Create typed error context for transformation error
        error_context = ProcessorErrorContextBuilder.from_transformation_error(
            processor=self.processor,
            validated_payload=validated_payload,
            context=context,
            transformation_error=error,
        )

        # Handle as stream error
        ws_error = WebSocketValidationError(
            message=f"Transformation failed: {error}",
            context=error_context,
            field="transformation",
            cause=error,
        )
        await self.stream_error_handler.handle_stream_error(ws_error)

        self.logger.debug(
            "transformation_error_handled",
            processor=self.processor.processor_name,
            error_type=type(error).__name__,
            payload_type=type(validated_payload).__name__,
            exchange=context.exchange_name,
        )

    async def handle_handler_error(
        self,
        error: Exception,
        domain_model: object,  # Domain model from processor
        context: WebSocketContextProtocol,
        is_unexpected: bool = False,
    ) -> None:
        """Handle message handler errors with typed error system.

        Args:
            error: The handler error that occurred
            domain_model: The domain model that failed to be handled
            context: WebSocket context protocol
            is_unexpected: Whether this was an unexpected error type
        """
        # Create typed error context for handler error
        error_context = ProcessorErrorContextBuilder.from_handler_error(
            processor=self.processor,
            domain_model=domain_model,
            context=context,
            handler_error=error,
            is_unexpected=is_unexpected,
        )

        # Handle as stream error
        error_prefix = "Unexpected handler error" if is_unexpected else "Handler invocation failed"
        ws_error = WebSocketValidationError(
            message=f"{error_prefix}: {error}",
            context=error_context,
            field="handler",
            cause=error,
        )
        await self.stream_error_handler.handle_stream_error(ws_error)

        self.logger.debug(
            "handler_error_handled",
            processor=self.processor.processor_name,
            error_type=type(error).__name__,
            domain_model_type=type(domain_model).__name__,
            is_unexpected=is_unexpected,
            exchange=context.exchange_name,
        )

    async def handle_unexpected_error(
        self,
        error: Exception,
        context: WebSocketContextProtocol,
        stage: str = "unknown",
    ) -> None:
        """Handle unexpected processor errors with typed error system.

        Args:
            error: The unexpected error that occurred
            context: WebSocket context protocol
            stage: The processing stage where error occurred
        """
        # Create typed error context for unexpected processor error
        error_context = ProcessorErrorContextBuilder.from_unexpected_error(
            processor=self.processor,
            context=context,
            unexpected_error=error,
            stage=stage,
        )

        # Handle as stream error
        ws_error = WebSocketValidationError(
            message=f"Unexpected processing error in {stage}: {error}",
            context=error_context,
            field="processing_pipeline",
            cause=error,
        )
        await self.stream_error_handler.handle_stream_error(ws_error)

        self.logger.warning(
            "unexpected_error_handled",
            processor=self.processor.processor_name,
            error_type=type(error).__name__,
            stage=stage,
            exchange=context.exchange_name,
        )

    def get_processor_stats(self) -> dict[str, Any]:
        """Get processor statistics for error context enhancement.

        Returns:
            Dictionary of processor statistics.
        """
        metrics = self.processor.get_metrics()
        return {
            "total_processed": metrics.processing_metrics.total_processed,
            "total_errors": metrics.processing_metrics.get_total_errors(),
            "error_rate": metrics.processing_metrics.get_error_rate(),
            "uptime_seconds": metrics.processing_metrics.get_uptime_seconds(),
        }

    def should_use_typed_handler(self) -> bool:
        """Check if typed error handler should be used.

        Returns:
            True if stream error handler is available and should be used.
        """
        return True  # Stream error handler is always available in this bridge

    async def create_enhanced_error_context(
        self,
        base_context: WebSocketContextProtocol,
        error: Exception,
        stage: str,
    ) -> dict[str, Any]:
        """Create enhanced error context with processor metrics.

        Args:
            base_context: Base WebSocket context
            error: The error that occurred
            stage: Processing stage where error occurred

        Returns:
            Enhanced error context dictionary for legacy compatibility.
        """
        processor_stats = self.get_processor_stats()

        return {
            "connection_id": base_context.connection_id,
            "exchange": base_context.exchange_name,
            "routing_key": getattr(base_context, "routing_key", None),
            "channel": getattr(base_context, "channel", None),
            "sequence_number": getattr(base_context, "sequence_number", None),
            "processor_name": self.processor.processor_name,
            "raw_model": self.processor.raw_model.__name__,
            "transformer_type": type(self.processor.transformer).__name__,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "stage": stage,
            "processor_stats": processor_stats,
        }


class ProcessorErrorBridgeFactory:
    """Factory for creating processor error bridges."""

    @staticmethod
    def create_bridge(
        processor: PydanticWebSocketProcessor[Any, Any],
        stream_error_handler: WebSocketStreamErrorHandler | None = None,
    ) -> ProcessorErrorBridge | None:
        """Create a processor error bridge if a stream error handler is available.

        Args:
            processor: The processor instance to bridge
            stream_error_handler: Optional stream error handler

        Returns:
            ProcessorErrorBridge if stream error handler is available, None otherwise.
        """
        if stream_error_handler is None:
            return None

        return ProcessorErrorBridge(
            processor=processor,
            stream_error_handler=stream_error_handler,
        )

    @staticmethod
    def is_bridge_available(
        processor: PydanticWebSocketProcessor[Any, Any],
    ) -> bool:
        """Check if error bridge can be created for processor.

        Args:
            processor: The processor instance to check

        Returns:
            True if processor has stream error handler available.
        """
        return (
            hasattr(processor, "stream_error_handler")
            and processor.stream_error_handler is not None
        )
