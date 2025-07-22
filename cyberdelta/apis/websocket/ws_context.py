"""Generic WebSocket context models and base classes.

This module provides base context objects for WebSocket message processing.
Exchange-specific contexts are in their respective packages.
"""

from __future__ import annotations

import json
import time
from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any, TypeVar

from pydantic import BaseModel, Field, computed_field


if TYPE_CHECKING:
    pass


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
        """Calculate message size for monitoring.

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
            data = self.model_dump(mode="python", exclude=excluded_fields)
            return len(json.dumps(data, default=str).encode("utf-8"))
        except (TypeError, ValueError, UnicodeEncodeError):
            # If serialization fails, return 0
            return 0

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

    @property
    def exchange_name(self) -> str:
        """Get exchange name for compatibility with BaseContextProtocol."""
        return str(self.exchange_type)

    @property
    def raw_model(self) -> object | None:
        """Get raw validated model (envelope) for compatibility with BaseContextProtocol."""
        return self.validated_envelope

    def get_transformer_params(self) -> dict[str, str]:
        """Get parameters needed by transformers for this exchange.

        Base implementation returns empty dict. Exchange-specific contexts
        should override this method to provide appropriate parameters.
        """
        return {}

    def get_symbol_param(self) -> dict[str, str] | None:
        """Get symbol parameter if applicable to this exchange.

        Base implementation returns None. Exchange-specific contexts
        should override this method if they support symbol parameters.
        """
        return None

    def get_coin_param(self) -> dict[str, str] | None:
        """Get coin parameter if applicable to this exchange.

        Base implementation returns None. Exchange-specific contexts
        should override this method if they support coin parameters.
        """
        return None


# Note: Exchange-specific contexts moved to their respective packages:
# - BackpackMessageContext is in cyberdelta.apis.backpack.bp_ws_context
# - HyperliquidMessageContext is in cyberdelta.apis.hyperliquid.hl_ws_context
