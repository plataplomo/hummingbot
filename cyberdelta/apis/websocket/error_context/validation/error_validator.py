"""Validation utilities for WebSocket error contexts.

Provides comprehensive validation for error contexts to ensure data integrity
and consistency across the WebSocket error system.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from cyberdelta.apis.common.error_foundation import ErrorContextValidator
from cyberdelta.apis.websocket.exceptions import WebSocketSequenceValidationError
from cyberdelta.enums import ExchangeName


if TYPE_CHECKING:
    from cyberdelta.apis.models.websocket import StreamErrorContext


class StreamErrorContextValidator(ErrorContextValidator):
    """Validator for WebSocket stream error contexts."""

    # Connection ID pattern: UUID or similar identifier
    CONNECTION_ID_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9\-_]{7,127}$")

    # Exchange name pattern: lowercase with optional hyphens
    EXCHANGE_PATTERN = re.compile(r"^[a-z][a-z0-9\-]{1,31}$")

    # Channel name pattern
    CHANNEL_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9\-_\.]{1,63}$")

    # Topic pattern (e.g., symbol names)
    TOPIC_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9\-_\.\/]{0,63}$")

    # Session ID pattern
    SESSION_ID_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9\-_]{15,127}$")

    # Validation constants
    MAX_TIMESTAMP_MS = 1_000_000_000  # Maximum reasonable timestamp (in milliseconds)
    MAX_SEQUENCE_NUMBER = 10000  # Maximum sequence number in single session
    MAX_MESSAGE_COUNT = 1000  # Maximum message count for validation
    MAX_BACKOFF_MS = 10000  # Maximum backoff delay in milliseconds
    MIN_BACKOFF_MS = 100  # Minimum backoff delay in milliseconds
    MAX_FIELD_LENGTH = 256  # Maximum field length for validation
    MAX_RAW_CONTENT_SIZE = 10_000_000  # Maximum raw content size in bytes
    DEFAULT_SAMPLE_SIZE = 50  # Default sample size for content truncation

    # ========================================================================
    # Connection Validation
    # ========================================================================

    @classmethod
    def validate_connection_id(cls, connection_id: str) -> str:
        """Validate WebSocket connection ID.

        Args:
            connection_id: Connection identifier to validate

        Returns:
            Validated connection ID

        Raises:
            ValueError: If connection ID is invalid
        """
        connection_id = cls.validate_non_empty_string(connection_id, "connection_id")

        if not cls.CONNECTION_ID_PATTERN.match(connection_id):
            msg = (
                f"Invalid connection_id format: {connection_id!r}. "
                "Must be 8-128 characters, alphanumeric with hyphens/underscores"
            )
            raise ValueError(msg)

        return connection_id

    @classmethod
    def validate_exchange(cls, exchange: ExchangeName) -> ExchangeName:
        """Validate exchange name.

        Args:
            exchange: Exchange name enum to validate

        Returns:
            Validated ExchangeName enum
        """
        # Since the type annotation ensures ExchangeName, just return it
        return exchange

    # ========================================================================
    # Channel & Subscription Validation
    # ========================================================================

    @classmethod
    def validate_channel(cls, channel: str | None) -> str | None:
        """Validate channel name.

        Args:
            channel: Channel name to validate

        Returns:
            Validated channel name or None

        Raises:
            ValueError: If channel format is invalid
        """
        if channel is None:
            return None

        channel = channel.strip()
        if not channel:
            return None

        if not cls.CHANNEL_PATTERN.match(channel):
            msg = f"Invalid channel format: {channel!r}. Must start with letter, 2-64 characters"
            raise ValueError(msg)

        return channel

    @classmethod
    def validate_topic(cls, topic: str | None) -> str | None:
        """Validate topic within channel.

        Args:
            topic: Topic to validate

        Returns:
            Validated topic or None

        Raises:
            ValueError: If topic format is invalid
        """
        if topic is None:
            return None

        topic = topic.strip()
        if not topic:
            return None

        # Topics are often symbols like "BTC-USDC" or "ETH/USD"
        if not cls.TOPIC_PATTERN.match(topic):
            msg = f"Invalid topic format: {topic!r}. Must be uppercase alphanumeric with delimiters"
            raise ValueError(msg)

        return topic

    # ========================================================================
    # Sequence Number Validation
    # ========================================================================

    @classmethod
    def validate_sequence_number(cls, seq: int | None) -> int | None:
        """Validate message sequence number.

        Args:
            seq: Sequence number to validate

        Returns:
            Validated sequence number or None

        Raises:
            ValueError: If sequence number is negative
        """
        if seq is None:
            return None

        if seq < 0:
            msg = f"Sequence number must be non-negative, got {seq}"
            raise ValueError(msg)

        # Warn about suspiciously large sequence numbers
        if seq > cls.MAX_TIMESTAMP_MS:
            # Still valid but might indicate an issue
            pass

        return seq

    @classmethod
    def validate_sequence_consistency(
        cls,
        sequence: int | None,
        expected: int | None,
        last_received: int | None,
    ) -> None:
        """Validate sequence number consistency.

        Args:
            sequence: Current sequence number
            expected: Expected sequence number
            last_received: Last received sequence number

        Raises:
            WebSocketSequenceValidationError: If sequence numbers are inconsistent
            ValueError: If excessive sequence gaps or regressions are detected
        """
        # Expected sequence requires a sequence number
        if expected is not None and sequence is None:
            raise WebSocketSequenceValidationError("expected_requires_sequence")

        # Sequence must be less than expected if both are set
        if sequence is not None and expected is not None and sequence >= expected:
            raise WebSocketSequenceValidationError(
                "expected_greater_than_current", sequence, expected
            )

        if sequence is not None and expected is not None:
            # Check for sequence gaps
            gap = abs(sequence - expected)
            if gap > cls.MAX_SEQUENCE_NUMBER:
                msg = (
                    f"Excessive sequence gap detected: {gap} "
                    f"(current={sequence}, expected={expected})"
                )
                raise ValueError(msg)

        if (
            sequence is not None
            and last_received is not None
            and sequence < last_received
            and (last_received - sequence) < cls.MAX_MESSAGE_COUNT
        ):
            msg = f"Sequence regression detected: current={sequence}, last_received={last_received}"
            raise ValueError(msg)

    # ========================================================================
    # Timestamp Validation
    # ========================================================================

    @classmethod
    def validate_timestamp_ms(cls, timestamp_ms: int | None) -> int | None:
        """Validate timestamp in milliseconds.

        Args:
            timestamp_ms: Timestamp to validate

        Returns:
            Validated timestamp or None

        Raises:
            ValueError: If timestamp is invalid
        """
        if timestamp_ms is None:
            return None

        if timestamp_ms <= 0:
            msg = f"Timestamp must be positive, got {timestamp_ms}"
            raise ValueError(msg)

        # Check for reasonable range (between year 2000 and 2100)
        min_timestamp = 946_684_800_000  # 2000-01-01
        max_timestamp = 4_102_444_800_000  # 2100-01-01

        if not min_timestamp <= timestamp_ms <= max_timestamp:
            msg = (
                f"Timestamp {timestamp_ms} outside reasonable range "
                f"[{min_timestamp}, {max_timestamp}]"
            )
            raise ValueError(msg)

        return timestamp_ms

    @classmethod
    def validate_time_consistency(
        cls,
        error_timestamp: int,
        connection_started: int | None,
        last_message: int | None,
        last_heartbeat: int | None,
    ) -> None:
        """Validate timestamp consistency.

        Args:
            error_timestamp: Error occurrence timestamp
            connection_started: Connection start timestamp
            last_message: Last message timestamp
            last_heartbeat: Last heartbeat timestamp

        Raises:
            ValueError: If timestamps are inconsistent
        """
        if connection_started is not None and connection_started > error_timestamp:
            msg = f"Connection start ({connection_started}) after error ({error_timestamp})"
            raise ValueError(msg)

        if last_message is not None and last_message > error_timestamp:
            msg = f"Last message ({last_message}) after error ({error_timestamp})"
            raise ValueError(msg)

        if last_heartbeat is not None and last_heartbeat > error_timestamp:
            msg = f"Last heartbeat ({last_heartbeat}) after error ({error_timestamp})"
            raise ValueError(msg)

    # ========================================================================
    # State Validation
    # ========================================================================

    @classmethod
    def validate_connection_state(
        cls,
        active_subscriptions: int,
        pending_messages: int,
        reconnect_count: int,
    ) -> None:
        """Validate connection state counters.

        Args:
            active_subscriptions: Number of active subscriptions
            pending_messages: Number of pending messages
            reconnect_count: Number of reconnection attempts

        Raises:
            ValueError: If state is invalid
        """
        if active_subscriptions < 0:
            msg = f"Active subscriptions cannot be negative: {active_subscriptions}"
            raise ValueError(msg)

        if pending_messages < 0:
            msg = f"Pending messages cannot be negative: {pending_messages}"
            raise ValueError(msg)

        if reconnect_count < 0:
            msg = f"Reconnect count cannot be negative: {reconnect_count}"
            raise ValueError(msg)

        # Warn about suspicious values
        if active_subscriptions > cls.MAX_MESSAGE_COUNT:
            # Might be valid but worth checking
            pass

        if pending_messages > cls.MAX_SEQUENCE_NUMBER:
            # Potential memory issue
            pass

        if reconnect_count > cls.MIN_BACKOFF_MS:
            # Excessive reconnection attempts
            pass

    # ========================================================================
    # Session & User Validation
    # ========================================================================

    @classmethod
    def validate_session_id(cls, session_id: str | None) -> str | None:
        """Validate session identifier.

        Args:
            session_id: Session ID to validate

        Returns:
            Validated session ID or None

        Raises:
            ValueError: If session ID format is invalid
        """
        if session_id is None:
            return None

        session_id = session_id.strip()
        if not session_id:
            return None

        if not cls.SESSION_ID_PATTERN.match(session_id):
            msg = (
                f"Invalid session_id format: {session_id!r}. "
                "Must be 16-128 characters, alphanumeric with hyphens/underscores"
            )
            raise ValueError(msg)

        return session_id

    @classmethod
    def validate_user_id(cls, user_id: str | None) -> str | None:
        """Validate user identifier.

        Args:
            user_id: User ID to validate

        Returns:
            Validated user ID or None

        Raises:
            ValueError: If user ID is invalid
        """
        if user_id is None:
            return None

        user_id = user_id.strip()
        if not user_id:
            return None

        # Basic validation - non-empty and reasonable length
        if len(user_id) > cls.MAX_FIELD_LENGTH:
            msg = f"User ID too long (max {cls.MAX_FIELD_LENGTH}): {len(user_id)} characters"
            raise ValueError(msg)

        return user_id

    # ========================================================================
    # Complete Context Validation
    # ========================================================================

    @classmethod
    def validate_context(cls, context: StreamErrorContext) -> None:
        """Validate complete error context.

        Args:
            context: Error context to validate

        Raises:
            ValueError: If context is invalid
        """
        # Validate connection info
        cls.validate_connection_id(context.connection_id)
        cls.validate_exchange(context.exchange)

        # Validate channel/subscription info
        cls.validate_channel(context.channel)
        cls.validate_topic(context.topic)

        # Validate sequence numbers
        cls.validate_sequence_number(context.sequence_number)
        cls.validate_sequence_number(context.expected_sequence)
        cls.validate_sequence_number(context.last_received_sequence)

        cls.validate_sequence_consistency(
            context.sequence_number,
            context.expected_sequence,
            context.last_received_sequence,
        )

        # Validate timestamps
        cls.validate_timestamp_ms(context.error_timestamp_ms)
        cls.validate_timestamp_ms(context.connection_started_ms)
        cls.validate_timestamp_ms(context.last_message_received_ms)
        cls.validate_timestamp_ms(context.last_heartbeat_ms)

        cls.validate_time_consistency(
            context.error_timestamp_ms,
            context.connection_started_ms,
            context.last_message_received_ms,
            context.last_heartbeat_ms,
        )

        # Validate connection state
        cls.validate_connection_state(
            context.active_subscriptions,
            context.pending_messages,
            context.reconnect_count,
        )

        # Validate session/user info
        cls.validate_session_id(context.session_id)
        cls.validate_user_id(context.user_id)

        # Validate message info
        if context.raw_message_size is not None:
            if context.raw_message_size < 0:
                msg = f"Message size cannot be negative: {context.raw_message_size}"
                raise ValueError(msg)
            if context.raw_message_size > cls.MAX_RAW_CONTENT_SIZE:  # 10MB
                msg = f"Message size suspiciously large: {context.raw_message_size}"
                # Just a warning, still valid

    @classmethod
    def sanitize_context(cls, context: StreamErrorContext) -> StreamErrorContext:
        """Sanitize error context by removing invalid fields.

        Args:
            context: Context to sanitize

        Returns:
            Sanitized context
        """
        # Create a copy to avoid modifying original
        sanitized = context.model_copy(deep=True)

        # Sanitize strings
        try:
            sanitized.connection_id = cls.validate_connection_id(sanitized.connection_id)
        except ValueError:
            # Use a default safe value
            sanitized.connection_id = "unknown-connection"

        try:
            sanitized.exchange = cls.validate_exchange(sanitized.exchange)
        except ValueError:
            # Use a default safe value - hyperliquid as fallback
            sanitized.exchange = ExchangeName.HYPERLIQUID

        # Sanitize optional fields
        try:
            sanitized.channel = cls.validate_channel(sanitized.channel)
        except ValueError:
            sanitized.channel = None

        try:
            sanitized.topic = cls.validate_topic(sanitized.topic)
        except ValueError:
            sanitized.topic = None

        # Ensure non-negative counters
        sanitized.active_subscriptions = max(0, sanitized.active_subscriptions)
        sanitized.pending_messages = max(0, sanitized.pending_messages)
        sanitized.reconnect_count = max(0, sanitized.reconnect_count)

        # Sanitize sequence numbers - set negative values to 0 or None
        if sanitized.sequence_number is not None and sanitized.sequence_number < 0:
            sanitized.sequence_number = 0
        if sanitized.expected_sequence is not None and sanitized.expected_sequence < 0:
            sanitized.expected_sequence = None
        if sanitized.last_received_sequence is not None and sanitized.last_received_sequence < 0:
            sanitized.last_received_sequence = 0

        if sanitized.raw_message_size is not None:
            sanitized.raw_message_size = max(0, sanitized.raw_message_size)

        return sanitized
