"""Router Error Context Builder for typed WebSocket error handling.

This module provides utilities to create typed error contexts from router state,
eliminating dict-based error context construction in WebSocket routing.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, TypeVar

from pydantic import BaseModel

from cyberdelta.apis.models.websocket import StreamErrorContext
from cyberdelta.apis.models.websocket.router import RouterErrorMetadata
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol


if TYPE_CHECKING:
    from cyberdelta.apis.websocket.ws_message_router import WebSocketMessageRouter

# Type variable for any BaseModel envelope type
EnvelopeT = TypeVar("EnvelopeT", bound=BaseModel)


class RouterErrorContextBuilder:
    """Builds typed error contexts from router state and operations.

    This builder creates StreamErrorContext instances with router-specific
    metadata, replacing manual dict construction in router error handling.
    """

    @staticmethod
    def from_envelope_validation_error[T: BaseModel](
        router: WebSocketMessageRouter[T],
        message: dict[str, Any],
        validation_error: Exception,
        envelope_type: str | None = None,
    ) -> StreamErrorContext:
        """Create error context from envelope validation failure.

        Args:
            router: Router instance that encountered the error
            message: Raw message that failed validation
            validation_error: The validation error that occurred
            envelope_type: Type of envelope that failed validation

        Returns:
            Typed error context for envelope validation error
        """
        metadata = RouterErrorMetadata(
            router_type=type(router).__name__,
            exchange_name=router.exchange_name,
            connection_id=router.connection_id,
            envelope_type=envelope_type,
            message_keys=list(message.keys()),
            message_size_bytes=len(str(message)),
            error_stage="envelope_validation",
        )

        return StreamErrorContext(
            connection_id=router.connection_id,
            exchange=router.exchange_name,
            channel=None,  # Not available at envelope validation stage
            topic=None,  # Not available at envelope validation stage
            sequence_number=None,  # Not available at envelope validation stage
            error_timestamp_ms=metadata.error_timestamp_ms or int(time.time() * 1000),
            extra_context={
                "raw_message": message,
                "router_metadata": metadata.model_dump(),
            },
        )

    @staticmethod
    def from_missing_routing_key_error[T: BaseModel](
        router: WebSocketMessageRouter[T],
        message: dict[str, Any],
        envelope: T,
    ) -> StreamErrorContext:
        """Create error context from missing routing key.

        Args:
            router: Router instance that encountered the error
            message: Raw message that couldn't be routed
            envelope: Validated envelope that lacks routing key

        Returns:
            Typed error context for missing routing key error
        """
        metadata = RouterErrorMetadata(
            router_type=type(router).__name__,
            exchange_name=router.exchange_name,
            connection_id=router.connection_id,
            envelope_type=type(envelope).__name__,
            message_keys=list(message.keys()),
            message_size_bytes=len(str(message)),
            error_stage="routing_key_extraction",
        )

        return StreamErrorContext(
            connection_id=router.connection_id,
            exchange=router.exchange_name,
            channel=None,  # Cannot determine without routing key
            topic=None,  # Cannot determine without routing key
            sequence_number=None,  # Not available at routing stage
            error_timestamp_ms=metadata.error_timestamp_ms or int(time.time() * 1000),
            extra_context={
                "raw_message": message,
                "router_metadata": metadata.model_dump(),
            },
        )

    @staticmethod
    def from_missing_processor_error[T: BaseModel](
        router: WebSocketMessageRouter[T],
        routing_key: str,
        payload: dict[str, Any] | list[Any],
        context: WebSocketContextProtocol,
    ) -> StreamErrorContext:
        """Create error context from missing processor.

        Args:
            router: Router instance that encountered the error
            routing_key: Routing key that has no processor
            payload: Message payload that couldn't be processed
            context: Typed WebSocket context (already available)

        Returns:
            Typed error context for missing processor error
        """
        metadata = RouterErrorMetadata(
            router_type=type(router).__name__,
            exchange_name=router.exchange_name,
            connection_id=router.connection_id,
            routing_key=routing_key,
            available_processors=list(router.processors.keys()),
            envelope_type=type(context).__name__ if hasattr(context, "__class__") else None,
            message_size_bytes=len(str(payload)),
            error_stage="processor_lookup",
        )

        # Extract information from existing typed context
        return StreamErrorContext(
            connection_id=context.connection_id,
            exchange=context.exchange_type,
            channel=getattr(context, "channel", None),
            topic=routing_key,  # Use routing key as topic
            sequence_number=getattr(context, "sequence_number", None),
            error_timestamp_ms=metadata.error_timestamp_ms or int(time.time() * 1000),
            extra_context={
                "raw_message": payload,  # Already typed as BaseModel
                "router_metadata": metadata.model_dump(),
            },
        )

    @staticmethod
    def from_missing_handler_error[T: BaseModel](
        router: WebSocketMessageRouter[T],
        routing_key: str,
        message: dict[str, Any],
        available_handlers: list[str],
    ) -> StreamErrorContext:
        """Create error context from missing handler.

        Args:
            router: Router instance that encountered the error
            routing_key: Routing key that has no handler
            message: Raw message that couldn't be handled
            available_handlers: List of available handler keys

        Returns:
            Typed error context for missing handler error
        """
        metadata = RouterErrorMetadata(
            router_type=type(router).__name__,
            exchange_name=router.exchange_name,
            connection_id=router.connection_id,
            routing_key=routing_key,
            available_handlers=available_handlers,
            message_keys=list(message.keys()),
            message_size_bytes=len(str(message)),
            error_stage="handler_lookup",
        )

        return StreamErrorContext(
            connection_id=router.connection_id,
            exchange=router.exchange_name,
            channel=None,  # Not yet determined at handler lookup
            topic=routing_key,
            sequence_number=None,  # Not available at handler lookup stage
            error_timestamp_ms=metadata.error_timestamp_ms or int(time.time() * 1000),
            extra_context={
                "raw_message": message,
                "router_metadata": metadata.model_dump(),
            },
        )

    @staticmethod
    def from_routing_error[T: BaseModel](
        router: WebSocketMessageRouter[T],
        error: Exception,
        message: dict[str, Any],
        routing_stage: str = "general_routing",
    ) -> StreamErrorContext:
        """Create error context from general routing error.

        Args:
            router: Router instance that encountered the error
            error: The routing error that occurred
            message: Raw message being routed when error occurred
            routing_stage: Stage of routing where error occurred

        Returns:
            Typed error context for general routing error
        """
        metadata = RouterErrorMetadata(
            router_type=type(router).__name__,
            exchange_name=router.exchange_name,
            connection_id=router.connection_id,
            message_keys=list(message.keys()),
            message_size_bytes=len(str(message)),
            error_stage=routing_stage,
        )

        return StreamErrorContext(
            connection_id=router.connection_id,
            exchange=router.exchange_name,
            channel=None,  # Not available for general routing errors
            topic=None,  # Not available for general routing errors
            sequence_number=None,  # Not available for general routing errors
            error_timestamp_ms=metadata.error_timestamp_ms or int(time.time() * 1000),
            extra_context={
                "raw_message": message,
                "router_metadata": metadata.model_dump(),
            },
        )

    @staticmethod
    def enhance_context_with_timing(
        context: StreamErrorContext,
        processing_start_time_ms: int,
    ) -> StreamErrorContext:
        """Enhance existing context with timing information.

        Args:
            context: Existing error context to enhance
            processing_start_time_ms: When processing started

        Returns:
            Enhanced context with timing information
        """
        # Always create/update extra_context with timing information
        enhanced_extra_context = context.extra_context.copy() if context.extra_context else {}
        enhanced_extra_context["processing_start_time_ms"] = processing_start_time_ms
        enhanced_extra_context["processing_duration_ms"] = (
            context.error_timestamp_ms - processing_start_time_ms
        )

        return context.model_copy(update={"extra_context": enhanced_extra_context})

    @staticmethod
    def create_recovery_context[T: BaseModel](
        router: WebSocketMessageRouter[T],
        original_context: StreamErrorContext,
        recovery_stage: str,
    ) -> StreamErrorContext:
        """Create error context for error recovery operations.

        Args:
            router: Router instance handling recovery
            original_context: Original error context that triggered recovery
            recovery_stage: Stage of recovery being attempted

        Returns:
            Typed error context for recovery operations
        """
        # Extract original metadata from extra_context
        original_extra_context = original_context.extra_context or {}

        # Create new recovery metadata
        recovery_metadata = RouterErrorMetadata(
            router_type=type(router).__name__,
            exchange_name=router.exchange_name,
            connection_id=router.connection_id,
            routing_key=original_extra_context.get("routing_key"),
            error_stage=f"recovery_{recovery_stage}",
            processing_start_time_ms=original_extra_context.get("error_timestamp_ms"),
        )

        return StreamErrorContext(
            connection_id=original_context.connection_id,
            exchange=original_context.exchange,
            channel=original_context.channel,
            topic=original_context.topic,
            sequence_number=original_context.sequence_number,
            error_timestamp_ms=recovery_metadata.error_timestamp_ms or int(time.time() * 1000),
            extra_context={
                **original_context.extra_context,
                "router_metadata": recovery_metadata.model_dump(),
            },
        )
