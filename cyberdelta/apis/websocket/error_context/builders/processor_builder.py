"""Processor Error Context Builder for WebSocket message processing.

This module provides utilities to build typed StreamErrorContext objects
from processor state, eliminating all dict[str, Any] conversions in error handling.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from pydantic import BaseModel, Field, ValidationError

from cyberdelta.apis.common.error_foundation import ErrorMetadata
from cyberdelta.apis.models.websocket import StreamErrorContext
from cyberdelta.apis.exceptions.websocket import WebSocketContextCreationError
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol


if TYPE_CHECKING:
    from cyberdelta.apis.websocket.ws_message_processor import WebSocketMessageProcessor

# Type variables for generic domain models
T = TypeVar("T", bound=BaseModel)
U = TypeVar("U", bound=BaseModel)

# Constants for payload processing
DEFAULT_CONTENT_SAMPLE_SIZE = 50  # Default sample size for content extraction


class ProcessorErrorMetadata(ErrorMetadata):
    """Extended error metadata for processor-specific information."""

    processor_name: str = Field(..., description="Name of the processor")
    stage: str = Field(..., description="Processing stage where error occurred")
    raw_model_name: str | None = Field(default=None, description="Name of the raw model class")
    transformer_type: str | None = Field(default=None, description="Type of the transformer")
    payload_type: str | None = Field(default=None, description="Type of the payload")
    domain_model_name: str | None = Field(default=None, description="Name of the domain model")
    domain_model_count: int | None = Field(default=None, description="Number of domain models")
    error_type: str | None = Field(default=None, description="Type of the error that occurred")
    is_unexpected_error: bool = Field(
        default=False, description="Whether this was an unexpected error"
    )
    is_critical: bool = Field(default=False, description="Whether this is a critical error")

    # Processor metrics at time of error
    total_processed: int | None = Field(default=None, description="Total messages processed")
    total_errors: int | None = Field(default=None, description="Total errors encountered")
    error_rate: float | None = Field(default=None, description="Current error rate")

    # Error-specific fields
    validation_error_count: int | None = Field(
        default=None, description="Number of validation errors in this event"
    )
    validated_model_name: str | None = Field(
        default=None, description="Name of the validated model"
    )
    payload_summary: str | None = Field(default=None, description="Summary of the payload")


class ProcessorErrorContextBuilder:
    """Builder for creating typed error contexts from processor state.

    This class eliminates the need for dict[str, Any] conversions by building
    properly typed StreamErrorContext objects directly from processor state.
    """

    @staticmethod
    def from_validation_error[T: BaseModel, U: BaseModel](
        processor: WebSocketMessageProcessor[T, U],
        payload: dict[str, object] | list[object] | BaseModel | str | bytes | float | bool | None,
        context: WebSocketContextProtocol,
        validation_error: ValidationError,
    ) -> StreamErrorContext:
        """Create error context for validation errors.

        Args:
            processor: The processor instance
            payload: The payload that failed validation
            context: WebSocket context protocol
            validation_error: The Pydantic validation error

        Returns:
            Fully typed StreamErrorContext

        Raises:
            WebSocketContextCreationError: If context does not provide create_error_context method.
        """
        # Build metadata with processor-specific information
        processor_metrics = processor.get_metrics()
        metadata = ProcessorErrorMetadata(
            processor_name=processor.processor_name,
            stage="validation",
            raw_model_name=processor.raw_model.__name__,
            payload_type=type(payload).__name__,
            error_type=type(validation_error).__name__,
            retry_count=0,
            backoff_ms=1000,
            total_processed=processor_metrics.processing_metrics.total_processed,
            total_errors=processor_metrics.processing_metrics.get_total_errors(),
            error_rate=processor_metrics.processing_metrics.get_error_rate(),
            validation_error_count=len(validation_error.errors()),
            payload_summary=ProcessorErrorContextBuilder.extract_payload_summary(payload),
        )

        # Use the context's create_error_context method if available
        if hasattr(context, "create_error_context"):
            base_context = context.create_error_context()
            # Ensure we have a StreamErrorContext (cast for type safety)
            if isinstance(base_context, StreamErrorContext):
                base_context.metadata = metadata
                return base_context

        # Require StreamErrorContext - no fallback
        raise WebSocketContextCreationError(
            context_type=type(context).__name__, protocol_requirement="create_error_context"
        )

    @staticmethod
    def from_transformation_error[T: BaseModel, U: BaseModel](
        processor: WebSocketMessageProcessor[T, U],
        validated_payload: T,
        context: WebSocketContextProtocol,
        transformation_error: Exception,
    ) -> StreamErrorContext:
        """Create error context for transformation errors.

        Args:
            processor: The processor instance
            validated_payload: The validated payload that failed transformation
            context: WebSocket context protocol
            transformation_error: The transformation exception

        Returns:
            Fully typed StreamErrorContext

        Raises:
            WebSocketContextCreationError: If context does not provide create_error_context method.
        """
        # Build metadata with transformation-specific information
        processor_metrics = processor.get_metrics()
        metadata = ProcessorErrorMetadata(
            processor_name=processor.processor_name,
            stage="transformation",
            raw_model_name=processor.raw_model.__name__,
            transformer_type=type(processor.transformer).__name__,
            payload_type=type(validated_payload).__name__,
            validated_model_name=type(validated_payload).__name__,
            error_type=type(transformation_error).__name__,
            retry_count=0,
            backoff_ms=2000,  # Longer backoff for transformation errors
            total_processed=processor_metrics.processing_metrics.total_processed,
            total_errors=processor_metrics.processing_metrics.get_total_errors(),
            error_rate=processor_metrics.processing_metrics.get_error_rate(),
        )

        # Use the context's create_error_context method if available
        if hasattr(context, "create_error_context"):
            base_context = context.create_error_context()
            # Ensure we have a StreamErrorContext (cast for type safety)
            if isinstance(base_context, StreamErrorContext):
                base_context.metadata = metadata
                return base_context

        # Require StreamErrorContext - no fallback
        raise WebSocketContextCreationError(
            context_type=type(context).__name__, protocol_requirement="create_error_context"
        )

    @staticmethod
    def from_handler_error[T: BaseModel, U: BaseModel](
        processor: WebSocketMessageProcessor[T, U],
        domain_model: U | list[U],  # Accept single or batch domain models
        context: WebSocketContextProtocol,
        handler_error: Exception,
        is_unexpected: bool = False,
    ) -> StreamErrorContext:
        """Create error context for message handler errors.

        Args:
            processor: The processor instance
            domain_model: The domain model that failed to be handled
            context: WebSocket context protocol
            handler_error: The handler exception
            is_unexpected: Whether this was an unexpected error type

        Returns:
            Fully typed StreamErrorContext

        Raises:
            WebSocketContextCreationError: If context does not provide create_error_context method.
        """
        # Determine domain model info
        if isinstance(domain_model, list):
            # Handle batch domain models
            first_type = type(domain_model[0]).__name__ if domain_model else "Unknown"
            domain_model_name = f"list[{first_type}]"
            domain_model_count = len(domain_model)
        else:
            # Handle single domain model
            domain_model_name = type(domain_model).__name__
            domain_model_count = 1

        # Build metadata with handler-specific information
        processor_metrics = processor.get_metrics()
        metadata = ProcessorErrorMetadata(
            processor_name=processor.processor_name,
            stage="handler_invocation",
            raw_model_name=processor.raw_model.__name__,
            transformer_type=type(processor.transformer).__name__,
            domain_model_name=domain_model_name,
            domain_model_count=domain_model_count,
            error_type=type(handler_error).__name__,
            is_unexpected_error=is_unexpected,
            retry_count=0,
            backoff_ms=3000 if is_unexpected else 1500,  # Longer backoff for unexpected errors
            total_processed=processor_metrics.processing_metrics.total_processed,
            total_errors=processor_metrics.processing_metrics.get_total_errors(),
            error_rate=processor_metrics.processing_metrics.get_error_rate(),
        )

        # Use the context's create_error_context method if available
        if hasattr(context, "create_error_context"):
            base_context = context.create_error_context()
            # Ensure we have a StreamErrorContext (cast for type safety)
            if isinstance(base_context, StreamErrorContext):
                base_context.metadata = metadata
                return base_context

        # Require StreamErrorContext - no fallback
        raise WebSocketContextCreationError(
            context_type=type(context).__name__, protocol_requirement="create_error_context"
        )

    @staticmethod
    def from_unexpected_error[T: BaseModel, U: BaseModel](
        processor: WebSocketMessageProcessor[T, U],
        context: WebSocketContextProtocol,
        unexpected_error: Exception,
        stage: str = "unknown",
    ) -> StreamErrorContext:
        """Create error context for unexpected processor errors.

        Args:
            processor: The processor instance
            context: WebSocket context protocol
            unexpected_error: The unexpected exception
            stage: The processing stage where error occurred

        Returns:
            Fully typed StreamErrorContext

        Raises:
            WebSocketContextCreationError: If context does not provide create_error_context method.
        """
        # Build metadata with unexpected error information
        processor_metrics = processor.get_metrics()

        # Determine if this is a critical error
        critical_error_types = (MemoryError, SystemError, OSError)
        is_critical = isinstance(unexpected_error, critical_error_types)

        metadata = ProcessorErrorMetadata(
            processor_name=processor.processor_name,
            stage=stage,
            raw_model_name=processor.raw_model.__name__,
            transformer_type=type(processor.transformer).__name__,
            error_type=type(unexpected_error).__name__,
            is_unexpected_error=True,
            is_critical=is_critical,
            retry_count=0,
            backoff_ms=5000,  # Longest backoff for unexpected errors
            total_processed=processor_metrics.processing_metrics.total_processed,
            total_errors=processor_metrics.processing_metrics.get_total_errors(),
            error_rate=processor_metrics.processing_metrics.get_error_rate(),
        )

        # Use the context's create_error_context method if available
        if hasattr(context, "create_error_context"):
            base_context = context.create_error_context()
            # Ensure we have a StreamErrorContext (cast for type safety)
            if isinstance(base_context, StreamErrorContext):
                base_context.metadata = metadata
                return base_context

        # Require StreamErrorContext - no fallback
        raise WebSocketContextCreationError(
            context_type=type(context).__name__, protocol_requirement="create_error_context"
        )

    @staticmethod
    def extract_payload_summary(
        payload: dict[str, object] | list[object] | BaseModel | str | bytes | float | bool | None,
        max_chars: int = 200,
    ) -> str:
        """Extract a safe summary of payload for error context.

        Args:
            payload: The payload data from WebSocket messages
            max_chars: Maximum characters in summary

        Returns:
            Safe string summary of payload
        """
        try:
            if payload is None:
                return "None"

            # Handle common types directly to keep complexity low
            if isinstance(payload, dict):
                max_keys = 5  # Constant for magic number
                keys_list = [str(key) for key in list(payload.keys())[:max_keys]]
                summary = f"dict(keys={keys_list}"
                if len(payload) > max_keys:
                    summary += f", +{len(payload) - max_keys} more"
                summary += ")"
                return summary[:max_chars]

            if isinstance(payload, list):
                if not payload:
                    return "list(empty)"
                first_type = type(payload[0]).__name__ if payload[0] is not None else "Unknown"
                return f"list(length={len(payload)}, type={first_type})"[:max_chars]

            return f"{type(payload).__name__}({str(payload)[:50]})"

        except (ValueError, TypeError, AttributeError, KeyError, IndexError):
            try:
                type_name = type(payload).__name__ if payload is not None else "None"
            except (AttributeError, TypeError):
                type_name = "Unknown"
            return f"{type_name}(summary_failed)"

    @staticmethod
    def enhance_context_with_metrics[T: BaseModel, U: BaseModel](
        context: StreamErrorContext,
        processor: WebSocketMessageProcessor[T, U],
    ) -> StreamErrorContext:
        """Enhance error context with current processor metrics.

        Args:
            context: The base error context
            processor: The processor instance

        Returns:
            Enhanced StreamErrorContext with metrics
        """
        # Get current processor metrics
        processor_metrics = processor.get_metrics()

        # If metadata exists and is ProcessorErrorMetadata, enhance it
        if context.metadata and isinstance(context.metadata, ProcessorErrorMetadata):
            # Update the metadata fields with processor metrics
            context.metadata.total_processed = processor_metrics.processing_metrics.total_processed
            context.metadata.total_errors = processor_metrics.processing_metrics.get_total_errors()
            context.metadata.error_rate = processor_metrics.processing_metrics.get_error_rate()

        return context
