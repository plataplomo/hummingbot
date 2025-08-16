"""Generic Pydantic WebSocket Message Processor.

This module provides generic, type-safe message processing for WebSocket
messages with Pydantic validation and transformation capabilities.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

import orjson
from pydantic import BaseModel, ValidationError

from cyberdelta.apis.websocket.metrics.processing_metrics import ProcessingMetrics, ProcessorMetrics
from cyberdelta.apis.websocket.websocket_states import MessageProcessingResult
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.config.structlog_config import get_logger


if TYPE_CHECKING:
    from cyberdelta.apis.websocket.metrics.general_metrics import WebSocketMetricsCollector

from cyberdelta.apis.enums.websocket import WebSocketErrorCode
from cyberdelta.apis.websocket.error_handling.error_handler import (
    WebSocketErrorHandler,
)
from cyberdelta.apis.websocket.exceptions import WebSocketStreamError
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext


# Type variables for input and output models
T = TypeVar("T", bound=BaseModel)  # Raw WebSocket message model
DomainModel = TypeVar("DomainModel", bound=BaseModel)  # Single domain model result
BatchResult = TypeVar("BatchResult", bound=BaseModel)  # Element type for batch results

# Protocol type variables
T_contra = TypeVar("T_contra", bound=BaseModel, contravariant=True)  # Protocol input
U_co = TypeVar("U_co", covariant=True)  # Protocol output (can be single or batch)

# Type variables for input and output models
TransformerResult = TypeVar("TransformerResult", bound=BaseModel)

# Union pattern for transformer results:
# - Single model: U (e.g., Trade)
# - Batch results: list[U] (e.g., list[Trade])
# - Failed/empty: None
# This flexible pattern allows both MapperTransformer and BatchMapperTransformer to work

# Message handler type - takes typed context
MessageHandler = Callable[[WebSocketContextProtocol], Awaitable[None]]


class MessageTransformer(Protocol[T_contra, U_co]):
    """Protocol for transforming validated WebSocket messages to domain models."""

    def transform(
        self,
        validated: T_contra,
        context: WebSocketContextProtocol | None = None,
    ) -> U_co:
        """Transform validated WebSocket message to domain model.

        Args:
            validated: The validated Pydantic model from WebSocket.
            context: Optional typed context containing additional data.

        Returns:
            The transformed domain model.

        Raises:
            Exception: If transformation fails.

        """
        ...


# Validation metrics now implemented using typed ProcessingMetrics
# See cyberdelta.apis.websocket.ws_processing_metrics for the typed implementation


class PydanticWebSocketProcessor[T: BaseModel, U: BaseModel]:
    """Generic processor for type-safe WebSocket message handling.

    This class provides a complete pipeline for processing WebSocket messages:
    1. Pydantic validation of raw messages
    2. Transformation to domain models
    3. Handler invocation with error handling
    4. Comprehensive metrics tracking
    """

    def __init__(
        self,
        raw_model: type[T],
        transformer: MessageTransformer[T, U | list[U] | None],
        stream_error_handler: WebSocketErrorHandler,
        processor_name: str | None = None,
        metrics_collector: WebSocketMetricsCollector | None = None,
    ) -> None:
        """Initialize the Pydantic processor.

        Args:
            raw_model: Pydantic model class for raw WebSocket messages.
            transformer: Transformer to convert raw to domain models.
            stream_error_handler: Required typed WebSocket error handler.
            processor_name: Optional name for logging (defaults to model name).
            metrics_collector: Optional metrics collector for enhanced monitoring.

        """
        self.raw_model = raw_model
        self.transformer = transformer
        self.stream_error_handler = stream_error_handler
        self.processor_name = processor_name or raw_model.__name__
        self.metrics = ProcessingMetrics()
        self.metrics_collector = metrics_collector
        self.logger = get_logger(f"PydanticProcessor.{self.processor_name}")

    async def process(
        self,
        payload: dict[str, Any] | list[Any],
        handler: MessageHandler,
        context: WebSocketContextProtocol,
    ) -> None:
        """Process WebSocket message through validation and transformation pipeline.

        Args:
            payload: Raw payload data to validate and process.
            handler: Handler function to call with transformed data.
            context: Typed context for processing and error handling.

        """
        start_time = time.perf_counter()
        # Optimized: Use orjson for fast serialization when calculating message size
        # This is 5-10x faster than standard json.dumps()
        message_size = len(orjson.dumps(payload)) if payload else 0
        message_type = context.routing_key or "unknown"
        result = MessageProcessingResult.FAILURE

        try:
            # Step 1: Validate with Pydantic
            validated = await self._validate_payload(payload, context, message_type)

            if validated is None:
                return

            # Step 2: Transform to domain model
            domain_model = await self._transform_message(validated, payload, context, message_type)
            if domain_model is None:
                return

            # Step 3: Store domain model and call handler
            # WebSocketContextProtocol implementations should have domain_model attribute
            # The concrete WebSocketMessageContext class has this field defined
            context.domain_model = domain_model

            success = await self._handle_message(domain_model, handler, context, message_type)

            # Record successful processing only if handler succeeded
            if success:
                result = MessageProcessingResult.SUCCESS
                processing_time = time.perf_counter() - start_time
                self.metrics.record_processing_time(processing_time)

            # Log performance metrics periodically
            if self.metrics.total_processed % 1000 == 0:
                self.logger.info(
                    "processor_metrics",
                    processor=self.processor_name,
                    total_processed=self.metrics.total_processed,
                    error_rate=self.metrics.get_error_rate(),
                    messages_per_second=self.metrics.get_messages_per_second(),
                    average_processing_time_ms=self.metrics.get_average_processing_time_ms(),
                )

        except Exception as e:
            # Catch-all for unexpected errors
            self.logger.exception(
                "unexpected_processing_error",
                processor=self.processor_name,
                error=str(e),
                routing_key=context.routing_key,
                exchange=context.exchange_type,
            )
            if self.metrics_collector:
                self.metrics_collector.record_error("unexpected", message_type, str(e))

            # Use direct stream error handler for unexpected processing errors
            error_context = StreamErrorContext(
                connection_id=context.connection_id,
                exchange=context.exchange_type.value,
            )

            stream_error = WebSocketStreamError(
                message=f"Unexpected processing error: {e}",
                code=WebSocketErrorCode.MESSAGE_VALIDATION_FAILED,
                context=error_context,
                cause=e,
            )

            await self.stream_error_handler.handle_stream_error(stream_error)
        finally:
            # Always record message metrics
            if self.metrics_collector:
                processing_time_ms = (time.perf_counter() - start_time) * 1000
                self.metrics_collector.record_message(
                    message_type,
                    processing_time_ms,
                    message_size,
                    result,
                )

    async def _validate_payload(
        self,
        payload: dict[str, Any] | list[Any],
        context: WebSocketContextProtocol,
        message_type: str,
    ) -> T | None:
        """Validate payload with Pydantic model.

        Returns:
            Validated Pydantic model instance, or None if validation fails.
        """
        try:
            validated = self.raw_model.model_validate(payload)
        except ValidationError as e:
            self.metrics.record_validation_error()
            if self.metrics_collector:
                self.metrics_collector.record_error("validation", message_type, str(e))

            # Pure typed error system - direct stream error handling for all validation errors
            # Create WebSocketStreamError for validation failures
            error_context = StreamErrorContext(
                connection_id=context.connection_id,
                exchange=context.exchange_type.value,
            )

            stream_error = WebSocketStreamError(
                message=f"Validation failed: {e}",
                code=WebSocketErrorCode.VALIDATION_FAILED,
                context=error_context,
                cause=e,
            )

            await self.stream_error_handler.handle_stream_error(stream_error)
            return None

        # Success case
        self.logger.debug(
            "message_validated",
            processor=self.processor_name,
            model=self.raw_model.__name__,
            routing_key=context.routing_key,
            exchange=context.exchange_type,
        )
        return validated

    async def _transform_message(
        self,
        validated: T,
        payload: dict[str, Any] | list[Any],
        context: WebSocketContextProtocol,
        message_type: str,
    ) -> U | list[U] | None:
        """Transform validated message to domain model.

        Returns:
            Transformed domain model(s), or None if transformation fails.
        """
        try:
            domain_model = self.transformer.transform(validated, context)

        except (ValidationError, ValueError, TypeError, AttributeError, KeyError) as e:
            self.metrics.record_transformation_error()
            if self.metrics_collector:
                self.metrics_collector.record_error("transformation", message_type, str(e))

            # Pure typed error system - direct stream error handling for all transformation errors
            # Create WebSocketStreamError for transformation errors
            error_context = StreamErrorContext(
                connection_id=context.connection_id,
                exchange=context.exchange_type.value,
            )

            stream_error = WebSocketStreamError(
                message=f"Transformation failed: {e}",
                code=WebSocketErrorCode.MESSAGE_VALIDATION_FAILED,
                context=error_context,
                cause=e,
            )

            await self.stream_error_handler.handle_stream_error(stream_error)
            return None

        # Success case
        self.logger.debug(
            "message_transformed",
            processor=self.processor_name,
            domain_model=type(domain_model).__name__,
            routing_key=context.routing_key,
            exchange=context.exchange_type,
        )
        return domain_model

    async def _handle_message(
        self,
        domain_model: U | list[U],
        handler: MessageHandler,
        context: WebSocketContextProtocol,
        message_type: str,
    ) -> bool:
        """Handle domain model with message handler.

        Returns:
            True if handling succeeded, False if it failed.

        Raises:
            CancelledError: If the async operation is cancelled.
            KeyboardInterrupt: If interrupted by user signal.
            SystemExit: If system exit is requested.
        """
        try:
            # Handler expects typed context, so just pass it directly
            # The handler is responsible for extracting domain model from context
            # For now, we'll create a new context with domain model attached
            # This is temporary until handlers are updated to work with domain models directly
            await handler(context)

        except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
            # Re-raise critical exceptions that should not be caught
            raise
        except (ValueError, TypeError, KeyError, AttributeError) as e:
            # Handle common processing errors specifically
            self.metrics.record_handler_error()
            if self.metrics_collector:
                self.metrics_collector.record_error("handler", message_type, str(e))

            # Pure typed error system - direct stream error handling for all handler errors
            error_context = StreamErrorContext(
                connection_id=context.connection_id,
                exchange=context.exchange_type.value,
            )

            stream_error = WebSocketStreamError(
                message=f"Handler error: {e}",
                code=WebSocketErrorCode.MESSAGE_VALIDATION_FAILED,
                context=error_context,
                cause=e,
            )

            await self.stream_error_handler.handle_stream_error(stream_error)
            return False
        except (OSError, RuntimeError, MemoryError) as e:
            # Catch any other runtime exceptions but log them as unexpected
            self.metrics.record_handler_error()
            if self.metrics_collector:
                self.metrics_collector.record_error("handler", message_type, str(e))
            self.logger.exception(
                "unexpected_handler_error",
                error_type=type(e).__name__,
                error_msg=str(e),
                routing_key=context.routing_key,
                exchange=context.exchange_type,
            )

            # Pure typed error system - direct stream error handling for unexpected handler errors
            error_context = StreamErrorContext(
                connection_id=context.connection_id,
                exchange=context.exchange_type.value,
            )

            stream_error = WebSocketStreamError(
                message=f"Unexpected handler error: {e}",
                code=WebSocketErrorCode.MESSAGE_VALIDATION_FAILED,
                context=error_context,
                cause=e,
            )

            await self.stream_error_handler.handle_stream_error(stream_error)
            return False
        else:
            # Success case
            self.logger.debug(
                "message_handled",
                processor=self.processor_name,
                routing_key=context.routing_key,
                exchange=context.exchange_type,
            )
            return True

    def get_metrics(self) -> ProcessorMetrics:
        """Get typed processing metrics.

        Returns:
            Typed ProcessorMetrics instance with all metrics and metadata.

        """
        return ProcessorMetrics.from_processor(
            processor_name=self.processor_name,
            raw_model_name=self.raw_model.__name__,
            transformer_type=type(self.transformer).__name__,
            processing_metrics=self.metrics,
        )

    def reset_metrics(self) -> None:
        """Reset processing metrics."""
        self.metrics = self.metrics.reset()
        self.logger.info(
            "processor_metrics_reset",
            processor=self.processor_name,
        )


class SimpleDictTransformer[T: BaseModel]:
    """Simple transformer that converts Pydantic models to dictionaries.

    This is useful for cases where no complex transformation is needed
    and the validated Pydantic model can be used directly as the domain model.
    """

    def transform(self, validated: T, context: WebSocketContextProtocol | None = None) -> T:
        """Transform by returning the validated model as-is.

        Args:
            validated: The validated Pydantic model.
            context: Optional typed context (ignored).

        Returns:
            The same model (no transformation).

        """
        return validated


class ProcessorFactory:
    """Factory for creating common processor configurations."""

    @staticmethod
    def create_simple_processor[T: BaseModel](
        raw_model: type[T],
        stream_error_handler: WebSocketErrorHandler,
        processor_name: str | None = None,
    ) -> PydanticWebSocketProcessor[T, T]:
        """Create a processor with no transformation (model passed through as-is).

        Args:
            raw_model: The Pydantic model for validation.
            stream_error_handler: Required typed WebSocket error handler.
            processor_name: Optional processor name.

        Returns:
            Configured processor instance.

        """
        return PydanticWebSocketProcessor(
            raw_model=raw_model,
            transformer=SimpleDictTransformer[T](),
            stream_error_handler=stream_error_handler,
            processor_name=processor_name,
        )

    @staticmethod
    def create_processor[T: BaseModel, U: BaseModel](
        raw_model: type[T],
        transformer: MessageTransformer[T, U],
        stream_error_handler: WebSocketErrorHandler,
        processor_name: str | None = None,
    ) -> PydanticWebSocketProcessor[T, U]:
        """Create a processor with custom transformation.

        Args:
            raw_model: The Pydantic model for validation.
            transformer: Custom transformer instance.
            stream_error_handler: Required typed WebSocket error handler.
            processor_name: Optional processor name.

        Returns:
            Configured processor instance.

        """
        return PydanticWebSocketProcessor(
            raw_model=raw_model,
            transformer=transformer,
            stream_error_handler=stream_error_handler,
            processor_name=processor_name,
        )
