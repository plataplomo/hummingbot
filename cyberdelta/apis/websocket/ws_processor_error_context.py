"""Processor Error Context Builder for WebSocket message processing.

This module provides utilities to build typed StreamErrorContext objects
from processor state, eliminating all dict[str, Any] conversions in error handling.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from pydantic import BaseModel, Field, ValidationError

from cyberdelta.apis.common.error_foundation import ErrorMetadata
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext


if TYPE_CHECKING:
    from cyberdelta.apis.websocket.ws_processor import PydanticWebSocketProcessor

# Type variables for generic domain models
T = TypeVar("T", bound=BaseModel)
U = TypeVar("U", bound=BaseModel)


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
        processor: PydanticWebSocketProcessor[T, U],
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

        # Fallback: build context manually from protocol attributes
        return StreamErrorContext(
            connection_id=context.connection_id,
            exchange=context.exchange_name,
            channel=getattr(context, "channel", None),
            topic=getattr(context, "routing_key", None),
            sequence_number=getattr(context, "sequence_number", None),
            metadata=metadata,
        )

    @staticmethod
    def from_transformation_error[T: BaseModel, U: BaseModel](
        processor: PydanticWebSocketProcessor[T, U],
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

        # Fallback: build context manually
        return StreamErrorContext(
            connection_id=context.connection_id,
            exchange=context.exchange_name,
            channel=getattr(context, "channel", None),
            topic=getattr(context, "routing_key", None),
            sequence_number=getattr(context, "sequence_number", None),
            metadata=metadata,
        )

    @staticmethod
    def from_handler_error[T: BaseModel, U: BaseModel](
        processor: PydanticWebSocketProcessor[T, U],
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
        """
        # Determine domain model info
        if isinstance(domain_model, list):
            # Handle batch domain models
            if domain_model:
                first_type = type(domain_model[0]).__name__
            else:
                first_type = "Unknown"
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

        # Fallback: build context manually
        return StreamErrorContext(
            connection_id=context.connection_id,
            exchange=context.exchange_name,
            channel=getattr(context, "channel", None),
            topic=getattr(context, "routing_key", None),
            sequence_number=getattr(context, "sequence_number", None),
            metadata=metadata,
        )

    @staticmethod
    def from_unexpected_error[T: BaseModel, U: BaseModel](
        processor: PydanticWebSocketProcessor[T, U],
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

        # Fallback: build context manually
        return StreamErrorContext(
            connection_id=context.connection_id,
            exchange=context.exchange_name,
            channel=getattr(context, "channel", None),
            topic=getattr(context, "routing_key", None),
            sequence_number=getattr(context, "sequence_number", None),
            metadata=metadata,
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
            # Handle None case
            if payload is None:
                return "None"

            # Handle dict type - most common for WebSocket payloads
            if isinstance(payload, dict):
                keys_list: list[str] = []
                dict_keys = list(payload.keys())[:5]
                for key in dict_keys:
                    keys_list.append(str(key))
                base_summary = f"dict(keys={keys_list}"

                max_keys_to_show = 5
                payload_len = len(payload)
                if payload_len > max_keys_to_show:
                    more_text = f", +{payload_len - max_keys_to_show} more"
                    closing = ")"
                    # Check if the complete summary would exceed max_chars
                    if len(base_summary + more_text + closing) <= max_chars:
                        return base_summary + more_text + closing
                    # Truncate the keys part but keep the "+more" info
                    available_space = max_chars - len(more_text) - len(closing)
                    return base_summary[:available_space] + more_text + closing
                summary = base_summary + ")"
                return summary[:max_chars]

            # Handle list type - batch payloads
            if isinstance(payload, list):
                length = len(payload)
                if length == 0:
                    return "list(empty)"
                # Get first item type safely
                first_type = "Unknown"
                if length > 0 and payload[0] is not None:
                    first_type = type(payload[0]).__name__
                summary = f"list(length={length}, type={first_type})"
                return summary[:max_chars]

            # Handle BaseModel instances
            if isinstance(payload, BaseModel):
                model_name = type(payload).__name__
                # Try to get a few field names
                try:
                    # Use the class attribute instead of instance attribute (Pydantic v2)
                    fields = list(type(payload).model_fields.keys())[:3]
                    return f"{model_name}(fields={fields})"[:max_chars]
                except Exception:
                    return f"{model_name}()"[:max_chars]

            # Handle primitive types
            if isinstance(payload, bytes):
                # Handle bytes specially to avoid mypy str-bytes-safe error
                content = repr(payload)[:50]
                return (
                    f"bytes({content}...)" if len(repr(payload)) > 50 else f"bytes({payload!r})"
                )

            if isinstance(payload, str):
                content = payload[:50]
                return f"str({content}...)" if len(payload) > 50 else f"str({payload})"

            # isinstance check for int, float, bool - these are the remaining types in our union
            # This must be last as bool is a subclass of int
            return f"{type(payload).__name__}({payload})"

        except (ValueError, TypeError, AttributeError, KeyError, IndexError):
            # Catch specific exceptions that could occur during summary extraction
            try:
                type_name = type(payload).__name__ if payload is not None else "None"
            except Exception:
                type_name = "Unknown"
            return f"{type_name}(summary_failed)"

    @staticmethod
    def enhance_context_with_metrics[T: BaseModel, U: BaseModel](
        context: StreamErrorContext,
        processor: PydanticWebSocketProcessor[T, U],
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
