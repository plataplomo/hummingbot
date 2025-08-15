"""Rich typed context for WebSocket stream errors.

Provides comprehensive context information for WebSocket errors,
including connection details, channel information, and sequence tracking.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field, field_validator

from cyberdelta.apis.common.error_foundation import ErrorChain, ErrorMetadata
from cyberdelta.apis.websocket.exceptions import WebSocketFieldValidationError
from cyberdelta.apis.websocket.validation import StreamErrorContextValidator


if TYPE_CHECKING:
    pass


# Helper factory for type inference
def _create_error_chain_list() -> list[ErrorChain]:
    """Create empty ErrorChain list for field defaults.

    Returns:
        Empty list for ErrorChain objects.
    """
    return []


class StreamErrorContext(BaseModel):
    """Rich context for WebSocket stream errors."""

    # ========================================================================
    # Connection Information
    # ========================================================================
    connection_id: str = Field(description="Unique identifier for the WebSocket connection")
    exchange: str = Field(description="Exchange name value from ExchangeName enum")
    environment: str = Field(
        default="production", description="Environment (production, staging, testnet)"
    )

    # ========================================================================
    # Channel & Subscription Information
    # ========================================================================
    channel: str | None = Field(
        default=None, description="Channel name (e.g., 'trades', 'orderbook', 'userEvents')"
    )
    topic: str | None = Field(
        default=None, description="Topic within channel (e.g., symbol for market data)"
    )
    subscription_id: str | None = Field(default=None, description="Unique subscription identifier")

    # ========================================================================
    # Sequence & Ordering
    # ========================================================================
    sequence_number: int | None = Field(
        default=None, description="Message sequence number for ordering"
    )
    expected_sequence: int | None = Field(
        default=None, description="Expected sequence number (for gap detection)"
    )
    last_received_sequence: int | None = Field(
        default=None, description="Last successfully received sequence number"
    )

    # ========================================================================
    # Timing Information
    # ========================================================================
    error_timestamp_ms: int = Field(
        default_factory=lambda: int(datetime.now(UTC).timestamp() * 1000),
        description="Error occurrence timestamp in milliseconds",
    )
    connection_started_ms: int | None = Field(
        default=None, description="Connection start timestamp in milliseconds"
    )
    last_message_received_ms: int | None = Field(
        default=None, description="Last message received timestamp in milliseconds"
    )
    last_heartbeat_ms: int | None = Field(
        default=None, description="Last heartbeat timestamp in milliseconds"
    )

    # ========================================================================
    # Message Context
    # ========================================================================
    message_id: str | None = Field(
        default=None, description="Unique message identifier if available"
    )
    message_type: str | None = Field(default=None, description="Type of message being processed")
    raw_message_size: int | None = Field(default=None, description="Size of raw message in bytes")

    # ========================================================================
    # Connection State
    # ========================================================================
    is_authenticated: bool = Field(default=False, description="Whether connection is authenticated")
    active_subscriptions: int = Field(default=0, description="Number of active subscriptions")
    pending_messages: int = Field(default=0, description="Number of messages pending processing")
    reconnect_count: int = Field(default=0, description="Number of reconnection attempts")

    # ========================================================================
    # Error Chain & Metadata
    # ========================================================================
    error_chain: list[ErrorChain] = Field(
        default_factory=_create_error_chain_list,
        description="Chain of errors leading to this error",
    )
    metadata: ErrorMetadata = Field(
        default_factory=ErrorMetadata, description="Additional error metadata"
    )

    # ========================================================================
    # Additional Context
    # ========================================================================
    user_id: str | None = Field(default=None, description="User identifier if authenticated")
    session_id: str | None = Field(default=None, description="Session identifier")
    client_version: str | None = Field(default=None, description="Client library version")
    extra_context: dict[str, Any] = Field(
        default_factory=dict, description="Additional context information"
    )

    # ========================================================================
    # Validation
    # ========================================================================

    @field_validator("connection_id")
    @classmethod
    def validate_connection_id(cls, v: str) -> str:
        """Validate connection ID format and length.

        Returns:
            The validated connection ID
        """
        return StreamErrorContextValidator.validate_connection_id(v)

    @field_validator("exchange")
    @classmethod
    def validate_exchange(cls, v: str) -> str:
        """Validate exchange name format and length.

        Returns:
            The validated exchange name
        """
        return StreamErrorContextValidator.validate_exchange(v)

    @field_validator("channel")
    @classmethod
    def validate_channel(cls, v: str | None) -> str | None:
        """Validate channel name format and length.

        Returns:
            The validated channel name or None
        """
        if v is None:
            return v
        return StreamErrorContextValidator.validate_channel(v)

    @field_validator("sequence_number", "expected_sequence", "last_received_sequence")
    @classmethod
    def validate_positive_sequence(cls, v: int | None) -> int | None:
        """Validate sequence numbers are positive.

        Returns:
            The validated sequence number or None

        Raises:
            WebSocketFieldValidationError: If sequence number is negative.
        """
        if v is not None and v < 0:
            raise WebSocketFieldValidationError(
                field_name="sequence_number",
                field_value=v,
                validation_error="Sequence number must be non-negative",
            )
        return v

    @field_validator("active_subscriptions", "pending_messages", "reconnect_count")
    @classmethod
    def validate_non_negative(cls, v: int) -> int:
        """Validate non-negative integers.

        Returns:
            The validated non-negative integer

        Raises:
            WebSocketFieldValidationError: If value is negative.
        """
        if v < 0:
            raise WebSocketFieldValidationError(
                field_name="field", field_value=v, validation_error="Field must be non-negative"
            )
        return v

    @field_validator("raw_message_size")
    @classmethod
    def validate_message_size(cls, v: int | None) -> int | None:
        """Validate message size.

        Returns:
            The validated message size or None

        Raises:
            WebSocketFieldValidationError: If message size is negative.
        """
        if v is not None and v < 0:
            raise WebSocketFieldValidationError(
                field_name="message_size",
                field_value=v,
                validation_error="Message size must be non-negative",
            )
        return v

    @field_validator(
        "error_timestamp_ms",
        "connection_started_ms",
        "last_message_received_ms",
        "last_heartbeat_ms",
    )
    @classmethod
    def validate_timestamp(cls, v: int | None) -> int | None:
        """Validate timestamps.

        Returns:
            The validated timestamp or None

        Raises:
            WebSocketFieldValidationError: If timestamp is negative or in the future.
        """
        if v is None:
            return v
        if v < 0:
            raise WebSocketFieldValidationError(
                field_name="timestamp",
                field_value=v,
                validation_error="Timestamp must be non-negative",
            )
        # Check for future timestamps (more than 5 seconds in future)
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        if v > now_ms + 5000:  # 5 seconds tolerance
            raise WebSocketFieldValidationError(
                field_name="timestamp",
                field_value=v,
                validation_error=f"Timestamp is in the future: {v}",
            )
        return v

    # ========================================================================
    # Methods
    # ========================================================================

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for logging.

        Returns:
            Dictionary representation of the context
        """
        return self.model_dump(exclude_none=True, mode="json")

    def get_connection_duration_ms(self) -> int | None:
        """Get connection duration in milliseconds.

        Returns:
            Connection duration in milliseconds or None if not available
        """
        if self.connection_started_ms is None:
            return None
        return self.error_timestamp_ms - self.connection_started_ms

    def get_time_since_last_message_ms(self) -> int | None:
        """Get time since last message in milliseconds.

        Returns:
            Time since last message in milliseconds or None if not available
        """
        if self.last_message_received_ms is None:
            return None
        return self.error_timestamp_ms - self.last_message_received_ms

    def get_time_since_last_heartbeat_ms(self) -> int | None:
        """Get time since last heartbeat in milliseconds.

        Returns:
            Time since last heartbeat in milliseconds or None if not available
        """
        if self.last_heartbeat_ms is None:
            return None
        return self.error_timestamp_ms - self.last_heartbeat_ms

    def has_sequence_gap(self) -> bool:
        """Check if there's a sequence gap.

        Returns:
            True if there is a sequence gap, False otherwise
        """
        if self.expected_sequence is None or self.sequence_number is None:
            return False
        return self.sequence_number != self.expected_sequence

    def get_sequence_gap_size(self) -> int | None:
        """Get size of sequence gap.

        Returns:
            Size of the sequence gap or None if no gap
        """
        if not self.has_sequence_gap():
            return None
        if self.expected_sequence is None or self.sequence_number is None:
            return None
        return abs(self.sequence_number - self.expected_sequence)

    def is_stale_connection(self, stale_threshold_ms: int = 30000) -> bool:
        """Check if connection is stale (no recent messages).

        Returns:
            True if connection is stale, False otherwise
        """
        time_since_last = self.get_time_since_last_message_ms()
        if time_since_last is None:
            return False
        return time_since_last > stale_threshold_ms

    def add_to_error_chain(self, error: Exception) -> None:
        """Add an error to the error chain."""
        self.error_chain.append(ErrorChain.from_exception(error, self.error_timestamp_ms))

    def get_summary(self) -> str:
        """Get a summary of the error context.

        Returns:
            Human-readable summary of the error context
        """
        parts = [
            f"Exchange: {self.exchange}",
            f"Connection: {self.connection_id[:8]}...",
        ]

        if self.channel:
            parts.append(f"Channel: {self.channel}")
        if self.topic:
            parts.append(f"Topic: {self.topic}")
        if self.sequence_number is not None:
            parts.append(f"Seq: {self.sequence_number}")
        if self.has_sequence_gap():
            parts.append(f"Gap: {self.get_sequence_gap_size()}")
        if self.reconnect_count > 0:
            parts.append(f"Reconnects: {self.reconnect_count}")

        return " | ".join(parts)

    model_config = {
        "frozen": False,
        "validate_assignment": True,
        "use_enum_values": False,
    }
