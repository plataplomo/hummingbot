"""Typed context models for WebSocket message processing.

This module provides strongly-typed context objects to replace dict[str, Any]
contexts throughout the WebSocket processing pipeline, eliminating type
information loss and improving type safety.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, TypeVar, cast

from pydantic import BaseModel, Field, computed_field

# Import envelope types for specific contexts
from cyberdelta.apis.backpack.models.bp_ws_envelope import BackpackRawWebSocketEnvelope
from cyberdelta.apis.hyperliquid.models.hl_ws_envelope import HyperliquidRawWebSocketEnvelope


# Import UTC timezone

# Type variables for generic context typing
EnvelopeType = TypeVar("EnvelopeType", bound="BaseModel")


class ExchangeType(StrEnum):
    """Enum for exchange types with type safety."""

    BACKPACK = "backpack"
    HYPERLIQUID = "hyperliquid"


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
    exchange_type: ExchangeType
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

    @computed_field
    def topic(self) -> str | None:
        """Extract topic with proper typing based on exchange."""
        if self.exchange_type == ExchangeType.BACKPACK:
            return getattr(self.validated_envelope, "stream", None)
        # HYPERLIQUID
        return getattr(self.validated_envelope, "channel", None)

    @computed_field
    def is_private_message(self) -> bool:
        """Determine if message is private based on routing key."""
        private_patterns = {"account", "user", "balance", "orders", "fills"}
        return any(pattern in self.routing_key.lower() for pattern in private_patterns)

    @computed_field
    def message_size_bytes(self) -> int:
        """Calculate message size for monitoring."""
        return len(self.model_dump_json().encode("utf-8"))

    @computed_field
    def processing_priority(self) -> int:
        """Compute processing priority (1=highest, 5=lowest)."""
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

    @computed_field
    def processing_duration_ms(self) -> float:
        """Calculate processing duration in milliseconds."""
        return (time.perf_counter() - self.processing_start_time) * 1000


class BackpackMessageContext(WebSocketMessageContext[BackpackRawWebSocketEnvelope]):
    """Backpack-specific message context with enhanced typing."""

    @computed_field
    def stream_type(self) -> str:
        """Extract stream type from Backpack stream."""
        return self.validated_envelope.stream.split(".")[0]

    @computed_field
    def stream_symbol(self) -> str | None:
        """Extract symbol from Backpack stream format."""
        parts = self.validated_envelope.stream.split(".")
        return parts[1] if len(parts) > 1 else None

    @computed_field
    def stream_details(self) -> str | None:
        """Extract additional details from Backpack stream format."""
        parts = self.validated_envelope.stream.split(".")
        # Extract additional details (3rd part)
        stream_details_index = 2
        return parts[stream_details_index] if len(parts) > stream_details_index else None


class HyperliquidMessageContext(WebSocketMessageContext[HyperliquidRawWebSocketEnvelope]):
    """Hyperliquid-specific message context with enhanced typing."""

    @computed_field
    def channel_type(self) -> str:
        """Extract channel type from Hyperliquid channel."""
        return self.validated_envelope.channel

    @computed_field
    def coin(self) -> str | None:
        """Extract coin from Hyperliquid data with proper typing."""
        if hasattr(self.validated_envelope, "data"):
            data = self.validated_envelope.data
            if isinstance(data, dict) and "coin" in data:
                coin = data["coin"]
                return coin if isinstance(coin, str) else None
        return None

    @computed_field
    def subscription_type(self) -> str | None:
        """Extract subscription type if available."""
        # Hyperliquid doesn't have a subscription field in the envelope
        # This is kept for potential future use
        return None


# Context union type for type-safe handling
WebSocketContextUnion = BackpackMessageContext | HyperliquidMessageContext


def create_context_for_exchange(
    exchange_type: ExchangeType,
    validated_envelope: BaseModel,
    routing_key: str,
    message_id: str,
    connection_id: str,
    symbol: str | None = None,
    user_id: str | None = None,
) -> WebSocketContextUnion:
    """Factory function to create appropriate context based on exchange type."""
    timestamp = datetime.now(UTC)

    if exchange_type == ExchangeType.BACKPACK:
        return BackpackMessageContext(
            validated_envelope=cast("BackpackRawWebSocketEnvelope", validated_envelope),
            exchange_type=exchange_type,
            routing_key=routing_key,
            timestamp=timestamp,
            message_id=message_id,
            connection_id=connection_id,
            symbol=symbol,
            user_id=user_id,
        )
    # HYPERLIQUID
    return HyperliquidMessageContext(
        validated_envelope=cast("HyperliquidRawWebSocketEnvelope", validated_envelope),
        exchange_type=exchange_type,
        routing_key=routing_key,
        timestamp=timestamp,
        message_id=message_id,
        connection_id=connection_id,
        symbol=symbol,
        user_id=user_id,
    )
