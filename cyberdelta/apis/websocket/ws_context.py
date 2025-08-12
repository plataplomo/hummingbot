"""Generic WebSocket context models and base classes.

This module provides base context objects for WebSocket message processing.
Exchange-specific contexts are in their respective packages.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any, TypeVar

import orjson
from pydantic import BaseModel, Field, computed_field

from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
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

    @computed_field  # type: ignore[prop-decorator]
    @property
    def is_private_message(self) -> bool:
        """Determine if message is private based on routing key.

        Returns:
            bool: True if message contains private data (account, user, balance, orders, fills).
        """
        private_patterns = {"account", "user", "balance", "orders", "fills"}
        return any(pattern in self.routing_key.lower() for pattern in private_patterns)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def message_size_bytes(self) -> int:
        """Calculate message size for monitoring.

        Returns:
            int: Message size in bytes after JSON serialization, or 0 if serialization fails.

        TODO: This computed field performs expensive JSON serialization and encoding
        on every access. Consider caching this value or using a simpler approximation
        for monitoring purposes to avoid performance overhead.
        """
        try:
            # Exclude ALL computed fields to prevent infinite recursion
            # This fixes the critical bug where computed fields trigger serialization loops
            excluded_fields = {
                "domain_model",
                "message_size_bytes",
                "processing_priority",
                "topic",
                "is_private_message",
            }
            data = self.model_dump(mode="json", exclude=excluded_fields)
            # Optimized: Use orjson for fast serialization (5-10x faster)
            # mode="json" ensures proper serialization of Decimal/datetime types
            return len(orjson.dumps(data))
        except (TypeError, ValueError, orjson.JSONEncodeError):
            # If serialization fails, return 0
            return 0

    @computed_field  # type: ignore[prop-decorator]
    @property
    def processing_priority(self) -> int:
        """Compute processing priority (1=highest, 5=lowest).

        Returns:
            int: Priority level - 1 for trades/user events, 2 for order books,
                3 for tickers/stats, 4 for everything else.
        """
        # High priority for trades and user events
        if "trades" in self.routing_key or "userEvents" in self.routing_key:
            return 1
        # Medium priority for order book updates
        if "depth" in self.routing_key or "l2Book" in self.routing_key:
            return 2
        # Lower priority for tickers and statistics
        if "ticker" in self.routing_key or "stats" in self.routing_key:
            return 3
        # Lowest priority for everything else
        return 4

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

    def get_transformer_params(self) -> dict[str, str]:
        """Get parameters needed by transformers for this exchange.

        Returns:
            dict[str, str]: Empty dict in base implementation. Exchange-specific contexts
                should override this method to provide appropriate parameters.
        """
        return {}

    def get_symbol_param(self) -> dict[str, str] | None:
        """Get symbol parameter if applicable to this exchange.

        Returns:
            dict[str, str] | None: None in base implementation. Exchange-specific contexts
                should override this method if they support symbol parameters.
        """
        return None

    def get_coin_param(self) -> dict[str, str] | None:
        """Get coin parameter if applicable to this exchange.

        Returns:
            dict[str, str] | None: None in base implementation. Exchange-specific contexts
                should override this method if they support coin parameters.
        """
        return None

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
            # Access computed field value properly
            raw_size = self.message_size_bytes
        except (TypeError, ValueError, AttributeError):
            raw_size = None

        # Create the error context with full type safety
        return StreamErrorContext(
            # Core connection info
            connection_id=self.connection_id,
            exchange=self.exchange_name,
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
            # Connection state
            is_authenticated=bool(self.is_private_message),  # Private implies auth
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
