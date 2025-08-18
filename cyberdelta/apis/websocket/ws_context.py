"""Generic WebSocket context models and base classes.

This module provides base context objects for WebSocket message processing.
Exchange-specific contexts are in their respective packages.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from functools import cached_property
from typing import Any, TypeVar

import orjson
from pydantic import BaseModel, Field, computed_field

from cyberdelta.apis.models.websocket import StreamErrorContext
from cyberdelta.enums import ExchangeName


# Import UTC timezone

# Type variables for generic context typing
EnvelopeType = TypeVar("EnvelopeType", bound="BaseModel")


# Using unified ExchangeName enum from cyberdelta.enums


class WebSocketMessageContext[EnvelopeType: "BaseModel"](BaseModel):
    """Fully typed context for WebSocket message processing.

    This replaces dict[str, Any] contexts throughout the WebSocket pipeline
    to provide complete type safety and eliminate Pyright errors.
    """

    model_config = {
        "extra": "forbid",
        "frozen": False,  # Allow setting domain_model
        "arbitrary_types_allowed": True,  # Allow generic types
        "validate_assignment": True,
        "str_strip_whitespace": True,
    }

    # Core strongly typed fields
    validated_envelope: EnvelopeType
    exchange_type: ExchangeName
    routing_key: str
    timestamp: datetime
    message_id: str = Field(min_length=1, max_length=64)
    connection_id: str = Field(min_length=1, max_length=32)

    # Optional fields with proper validation
    symbol: str | None = Field(default=None, min_length=1, max_length=20)
    user_id: str | None = Field(default=None, min_length=1, max_length=64)

    # Processing metadata
    processing_start_time: float = Field(default_factory=time.perf_counter)

    # Authentication state - should be set by router/connection manager
    is_authenticated_channel: bool = Field(
        default=False,
        description="Whether this message came from an authenticated subscription/channel",
    )

    # Domain model - populated by processor after transformation
    # Type is Any because it varies based on the transformer used
    domain_model: Any = Field(default=None, exclude=True)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def exchange_name(self) -> str:
        """Get exchange name as string.

        Returns:
            str: Exchange name string value
        """
        return self.exchange_type.value

    @computed_field  # type: ignore[prop-decorator]
    @property
    def topic(self) -> str | None:
        """Extract topic with proper typing based on exchange.

        Returns:
            str | None: Topic/stream name from message envelope, or None if not available.
        """
        if self.exchange_type == ExchangeName.BACKPACK:
            return getattr(self.validated_envelope, "stream", None)
        # HYPERLIQUID
        return getattr(self.validated_envelope, "channel", None)

    @cached_property
    def message_size_bytes(self) -> int:
        """Calculate and cache message size for monitoring.

        This uses @cached_property to calculate the size only once and cache it,
        avoiding expensive JSON serialization on repeated access while maintaining
        the property interface for backward compatibility.

        Returns:
            int: Message size in bytes after JSON serialization, or 0 if serialization fails.
        """
        try:
            # Exclude computed fields and domain model to prevent recursion
            excluded_fields = {
                "domain_model",
                "exchange_name",
                "topic",
                "processing_duration_ms",
                "channel",
                "sequence_number",
            }
            data = self.model_dump(mode="json", exclude=excluded_fields)
            # Use orjson for fast serialization (5-10x faster than standard json)
            return len(orjson.dumps(data))
        except (TypeError, ValueError, orjson.JSONEncodeError):
            # If serialization fails, return 0
            return 0

    @computed_field  # type: ignore[prop-decorator]
    @property
    def processing_duration_ms(self) -> float:
        """Calculate processing duration in milliseconds.

        Returns:
            float: Time elapsed since processing started, in milliseconds.
        """
        return (time.perf_counter() - self.processing_start_time) * 1000

    @property
    def raw_model(self) -> object | None:
        """Get raw validated model (envelope) for compatibility with BaseContextProtocol."""
        return self.validated_envelope

    # ========================================================================
    # Error Context Creation (Step 13 - WebSocket Type Safety)
    # ========================================================================

    def create_error_context(
        self,
        channel: str | None = None,
        sequence_number: int | None = None,
        message_type: str | None = None,
    ) -> StreamErrorContext:
        """Create typed error context from WebSocket context.

        This method creates a fully typed StreamErrorContext for use with the
        new decoupled WebSocket error system. No dict conversions!

        Args:
            channel: Optional channel name override
            sequence_number: Optional sequence number
            message_type: Optional message type override

        Returns:
            StreamErrorContext: Fully typed error context for WebSocket errors
        """
        # Get current timestamp in milliseconds
        now = datetime.now(UTC)
        timestamp_ms = int(now.timestamp() * 1000)

        # Determine channel from topic or parameter
        # Access computed field value properly
        topic_value = self.topic
        error_channel = channel or topic_value

        # Get message type from envelope if not provided
        if message_type is None and hasattr(self.validated_envelope, "type"):
            message_type = str(getattr(self.validated_envelope, "type", None))
        elif message_type is None and hasattr(self.validated_envelope, "method"):
            message_type = str(getattr(self.validated_envelope, "method", None))

        # Calculate message size if possible
        try:
            # Access the cached property
            raw_size = self.message_size_bytes
        except (TypeError, ValueError, AttributeError):
            raw_size = None

        # Create the error context with full type safety
        return StreamErrorContext(
            # Core connection info
            connection_id=self.connection_id,
            exchange=self.exchange_type,
            environment="production",  # Could be configurable
            # Channel & subscription info
            channel=error_channel,
            topic=self.symbol,  # Symbol often serves as topic
            subscription_id=None,  # Would need to be tracked separately
            # Sequence & ordering
            sequence_number=sequence_number,
            expected_sequence=None,  # Would need sequence tracking
            last_received_sequence=None,  # Would need sequence tracking
            # Timing information
            error_timestamp_ms=timestamp_ms,
            connection_started_ms=None,  # Would need connection tracking
            last_message_received_ms=timestamp_ms,  # Current message time
            last_heartbeat_ms=None,  # Would need heartbeat tracking
            # Message context
            message_id=self.message_id,
            message_type=message_type,
            raw_message_size=raw_size,
            # Connection state - use actual auth state from connection manager
            is_authenticated=self.is_authenticated_channel,
            active_subscriptions=0,  # Would need subscription tracking
            pending_messages=0,  # Would need queue tracking
            reconnect_count=0,  # Would need reconnect tracking
            # Additional context
            user_id=self.user_id,
            session_id=self.connection_id,  # Using connection_id as session
            client_version=None,  # Would need version tracking
        )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def channel(self) -> str | None:
        """Get channel name for error context compatibility.

        Returns:
            str | None: Channel/topic name from the message
        """
        # Access computed field value properly
        return self.topic

    @computed_field  # type: ignore[prop-decorator]
    @property
    def sequence_number(self) -> int | None:
        """Get sequence number if available.

        Returns:
            int | None: Sequence number from envelope if present
        """
        # Try common sequence field names
        if hasattr(self.validated_envelope, "sequence"):
            return getattr(self.validated_envelope, "sequence", None)
        if hasattr(self.validated_envelope, "seq"):
            return getattr(self.validated_envelope, "seq", None)
        if hasattr(self.validated_envelope, "sequence_number"):
            return getattr(self.validated_envelope, "sequence_number", None)
        return None


# Note: Exchange-specific contexts moved to their respective packages:
# - BackpackMessageContext is in cyberdelta.apis.backpack.bp_ws_context
# - HyperliquidMessageContext is in cyberdelta.apis.hyperliquid.hl_ws_context
