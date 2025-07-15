"""Hyperliquid WebSocket Message Envelope Models.

This module defines Pydantic models for the WebSocket message envelope structure
used by Hyperliquid exchange.

All Hyperliquid WebSocket messages follow this format:
{
    "channel": "<channel_name>",
    "data": <channel_specific_payload>
}

Special case for subscription responses:
{
    "channel": "subscriptionResponse",
    "data": {
        "method": "subscription",
        "subscription": {"type": "l2Book", "coin": "BTC"}
    }
}

This module provides proper type-safe envelope validation before content processing,
bringing Hyperliquid to the same standard as the Backpack implementation.
"""

from __future__ import annotations

import time
from contextlib import suppress
from enum import StrEnum
from typing import Any, Literal, TypeGuard

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    ValidationInfo,
    computed_field,
    field_validator,
    model_validator,
)
from pydantic.json_schema import GenerateJsonSchema, JsonSchemaMode
from pydantic_core.core_schema import ValidatorFunctionWrapHandler

from cyberdelta.config.structlog_config import get_logger


def _is_string_dict(obj: object) -> TypeGuard[dict[str, Any]]:
    """Type guard to check if object is a string-keyed dictionary."""
    return isinstance(obj, dict)


def _is_any_list(obj: object) -> TypeGuard[list[Any]]:
    """Type guard to check if object is a list."""
    return isinstance(obj, list)


logger = get_logger(__name__)

# Type for channel input that can be normalized
ChannelInput = str | dict[str, Any]

# Type for data field that should be dict or list
DataInput = dict[str, Any] | list[Any]

# Constants for validation
MAX_DICT_SIZE = 1000
MAX_LIST_SIZE = 10000
MIN_VALIDATION_LOG_TIME_SECONDS = 0.001  # Log validation times over 1ms


def _raise_payload_size_error(size: int, data_type: str) -> None:
    """Raise error for payload size violations."""
    msg = f"Payload {data_type} too large: {size} items"
    raise ValueError(msg)


class HyperliquidChannelType(StrEnum):
    """Valid Hyperliquid WebSocket channels.

    Based on the channels found in ws_validators.py and hl_ws_router.py.
    """

    L2_BOOK = "l2Book"
    TRADES = "trades"
    USER_EVENTS = "userEvents"
    ALL_MIDS = "allMids"
    NOTIFICATION = "notification"
    WEB_DATA2 = "webData2"
    SUBSCRIPTION_RESPONSE = "subscriptionResponse"
    # Additional channels that might be used for routing
    ORDERS = "orders"  # Routed from userEvents
    FILLS = "fills"  # Routed from userEvents


class HyperliquidRawWebSocketEnvelope(BaseModel):
    """Raw WebSocket message envelope from Hyperliquid.

    This model validates the outer envelope structure of all Hyperliquid WebSocket messages
    according to the Hyperliquid WebSocket API specification.

    All Hyperliquid WebSocket messages follow this format:
    {
        "channel": "<channel_name>",
        "data": <channel_specific_payload>
    }

    The actual payload validation is handled by channel-specific processors.

    Examples:
    - L2 Book: {"channel": "l2Book", "data": {"coin": "BTC", "time": 123, "levels": [...]}}
    - Trades: {"channel": "trades", "data": {"coin": "ETH", "side": "B", "px": "3000.0", ...}}
    - User Events: {"channel": "userEvents", "data": {"fills": [...], "orders": [...], ...}}
    """

    channel: str = Field(
        ...,
        min_length=1,
        max_length=64,
        description=(
            "Channel identifier such as 'l2Book', 'trades', 'userEvents', etc. "
            "Determines the structure and processing of the data field."
        ),
    )
    data: dict[str, Any] | list[Any] = Field(
        ...,
        description=(
            "Channel-specific payload data. Structure varies by channel type. "
            "Can be a dict for most channels or a list for batch updates."
        ),
    )

    model_config = ConfigDict(
        extra="allow",  # Allow computed fields and extra data
        frozen=True,  # Make instances immutable after creation
        validate_assignment=True,
        json_schema_extra={
            "title": "Hyperliquid WebSocket Message Envelope",
            "description": "Standard envelope structure for all Hyperliquid WebSocket messages",
            "examples": [
                {
                    "channel": "l2Book",
                    "data": {
                        "coin": "BTC",
                        "levels": [["50000.0", "1.5", 1]],
                        "time": 1640995200000,
                    },
                },
                {
                    "channel": "trades",
                    "data": {
                        "coin": "ETH",
                        "side": "B",
                        "px": "3000.0",
                        "sz": "0.5",
                        "time": 1640995200000,
                    },
                },
                {"channel": "userEvents", "data": {"fills": [], "orders": [], "positions": []}},
            ],
            "x-ws-message-type": "market-data",
            "x-exchange": "hyperliquid",
            "x-api-version": "v1",
        },
    )

    @field_validator("channel", mode="before")
    @classmethod
    def normalize_channel(cls, v: ChannelInput) -> str:
        """Normalize channel field from standard input formats.

        This validator normalizes string input before main validation.
        """
        # Handle string inputs with normalization
        if isinstance(v, str):
            v = v.strip()

            # Normalize channel names to standard case
            channel_normalizations = {
                "l2book": "l2Book",
                "l2_book": "l2Book",
                "userEvents": "userEvents",
                "userevents": "userEvents",
                "allmids": "allMids",
                "all_mids": "allMids",
                "webdata2": "webData2",
                "web_data2": "webData2",
            }

            # Apply normalization if found
            normalized = channel_normalizations.get(v.lower())
            if normalized:
                return normalized

            return v

        msg = f"Invalid channel format: {v} (type: {type(v)})"
        raise ValueError(msg)

    @field_validator("channel", mode="after")
    @classmethod
    def validate_channel_format(cls, v: str, _info: ValidationInfo) -> str:
        """Validate channel name format and known types.

        This validator performs business logic validation after normalization.

        While we validate against known channels, we don't fail on unknown channels
        to allow for future API extensions. Unknown channels are logged as warnings
        in the router.

        Args:
            v: The channel value to validate.
            info: Validation context information.

        Returns:
            The validated channel string.

        Raises:
            ValueError: If channel is empty or has invalid characters.
        """
        if not v or not v.strip():
            msg = "Channel name cannot be empty"
            raise ValueError(msg)

        # Basic format validation - channel names should be alphanumeric
        if not v.replace("_", "").replace("2", "").isalnum():
            msg = f"Invalid characters in channel name: {v}"
            raise ValueError(msg)

        # Check if it's a known channel (but don't fail if unknown)
        with suppress(ValueError):
            # Unknown channel - will be logged as warning in router
            # This allows for API extensions without breaking the client
            HyperliquidChannelType(v)

        return v

    @field_validator("data", mode="wrap")
    @classmethod
    def validate_and_monitor_data(
        cls, v: DataInput, handler: ValidatorFunctionWrapHandler, info: ValidationInfo
    ) -> dict[str, Any] | list[Any]:
        """Wrap validator for data preprocessing with performance tracking.

        This validator provides monitoring and performance tracking during data validation.
        """
        start_time = time.perf_counter()

        try:
            # Perform size checks before expensive validation
            if isinstance(v, dict) and len(v) > MAX_DICT_SIZE:
                _raise_payload_size_error(len(v), "dict")
            if isinstance(v, list) and len(v) > MAX_LIST_SIZE:
                _raise_payload_size_error(len(v), "list")

            # Call the normal validation chain
            result: dict[str, Any] | list[Any] = handler(v)
        except Exception as e:
            # Log validation failures with context for monitoring
            duration = time.perf_counter() - start_time
            logger.warning(
                "hl_data_validation_failed",
                component="HyperliquidEnvelope",
                duration_ms=duration * 1000,
                error=str(e),
                data_type=type(v).__name__,
                channel=info.context.get("channel") if info.context else None,
            )
            raise
        else:
            # Log successful validation for monitoring
            duration = time.perf_counter() - start_time
            if duration > MIN_VALIDATION_LOG_TIME_SECONDS:  # Log validation over 1ms
                logger.debug(
                    "hl_data_validation_success",
                    component="HyperliquidEnvelope",
                    duration_ms=duration * 1000,
                    size=len(v) if hasattr(v, "__len__") else 0,
                )
            return result

    @classmethod
    def _validate_payload_size(cls, v: dict[str, Any] | list[Any]) -> None:
        """Validate payload size constraints."""
        if isinstance(v, dict) and len(v) > MAX_DICT_SIZE:
            msg = f"Payload dict too large: {len(v)} items"
            raise ValueError(msg)
        if isinstance(v, list) and len(v) > MAX_LIST_SIZE:
            msg = f"Payload list too large: {len(v)} items"
            raise ValueError(msg)

    def _validate_data_type(self, expected_types: type | tuple[type, ...]) -> None:
        """Validate data type matches expectations."""
        # Single type check
        if expected_types is dict and not isinstance(self.data, dict):
            msg = f"Channel '{self.channel}' expects dict data, got {type(self.data).__name__}"
            raise TypeError(msg)

        if expected_types is list and not isinstance(self.data, list):
            msg = f"Channel '{self.channel}' expects list data, got {type(self.data).__name__}"
            raise TypeError(msg)

        # Multiple type check
        if isinstance(expected_types, tuple):
            is_valid = (dict in expected_types and isinstance(self.data, dict)) or (
                list in expected_types and isinstance(self.data, list)
            )
            if not is_valid:
                type_names = " or ".join(getattr(t, "__name__", str(t)) for t in expected_types)
                msg = (
                    f"Channel '{self.channel}' expects {type_names} data, "
                    f"got {type(self.data).__name__}"
                )
                raise TypeError(msg)

    def _validate_channel_specific_fields(self) -> None:
        """Validate channel-specific required fields."""
        channel = self.channel

        if channel == "l2Book" and _is_string_dict(self.data) and "coin" not in self.data:
            msg = "L2 book data must contain 'coin' field"
            raise ValueError(msg)

        if channel == "trades" and _is_string_dict(self.data) and "coin" not in self.data:
            msg = "Trade data must contain 'coin' field"
            raise ValueError(msg)

    @model_validator(mode="after")
    def validate_channel_data_consistency(self) -> HyperliquidRawWebSocketEnvelope:
        """Validate channel type matches data structure.

        This model validator performs cross-field validation to ensure
        channel type is consistent with data structure.
        """
        if hasattr(self, "channel") and hasattr(self, "data"):
            # Define expected data structures per channel type
            data_expectations: dict[str, type | tuple[type, ...]] = {
                "l2Book": dict,  # L2 book updates are always dict
                "trades": (dict, list),  # Trades can be single dict or list of dicts
                "userEvents": dict,  # User events are always dict with event arrays
                "allMids": (dict, list),  # All mids can be dict or list
                "notification": dict,  # Notifications are always dict
                "webData2": (dict, list),  # Web data can be dict or list
            }

            expected_types = data_expectations.get(self.channel)
            if expected_types:
                self._validate_data_type(expected_types)

            # Additional validation for specific channels
            self._validate_channel_specific_fields()

        return self

    def extract_coin(self) -> str | None:
        """Extract coin from data with type safety, handling both dict and list data.

        Returns:
            Coin string if found in data, None otherwise.
        """
        # Handle dict data (most common case)
        if _is_string_dict(self.data):
            coin_data = self.data.get("coin")
            return coin_data if isinstance(coin_data, str) else None

        # Handle list data (e.g., trades channel)
        if _is_any_list(self.data) and len(self.data) > 0:
            first_item = self.data[0]
            if _is_string_dict(first_item):
                coin_data = first_item.get("coin")
                return coin_data if isinstance(coin_data, str) else None

        return None

    def get_routing_key(self) -> str:
        """Get routing key for message routing.

        Returns:
            String routing key based on the channel name.
        """
        # For most Hyperliquid channels, the channel name is the routing key
        if self.channel in {"l2Book", "trades", "allMids", "notification", "webData2"}:
            return self.channel

        # For userEvents, we route to the userEvents processor
        if self.channel == "userEvents":
            return "userEvents"

        # For unknown channels, return the channel name as routing key
        return self.channel

    def get_payload(self) -> dict[str, Any] | list[Any]:
        """Get payload data for processing.

        Returns:
            The payload data from the envelope.
        """
        return self.data

    def get_envelope_type(self) -> str:
        """Get human-readable envelope type name.

        Returns:
            String identifying the envelope type.
        """
        return "HyperliquidRawWebSocketEnvelope"

    @classmethod
    def model_json_schema(
        cls,
        by_alias: bool = True,
        ref_template: str = "#/$defs/{model}",
        schema_generator: type[GenerateJsonSchema] = GenerateJsonSchema,
        mode: JsonSchemaMode = "validation",
    ) -> dict[str, Any]:
        """Generate OpenAPI-compatible JSON schema for this model.

        Args:
            by_alias: Whether to use field aliases in the schema
            ref_template: Template for generating references
            schema_generator: Optional schema generator class
            mode: Schema generation mode

        Returns:
            JSON schema dictionary with Hyperliquid-specific extensions
        """
        schema = super().model_json_schema(
            by_alias=by_alias,
            ref_template=ref_template,
            schema_generator=schema_generator,
            mode=mode,
        )

        # Add WebSocket-specific extensions
        schema.update({
            "x-ws-protocol": "websocket",
            "x-exchange": "hyperliquid",
            "x-message-category": "envelope",
            "x-api-docs": "https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket-api",
            "x-validation-rules": {
                "channel_types": ["l2Book", "trades", "userEvents", "orderUpdates", "fillUpdates"],
                "data_constraints": "Size limited to prevent DoS attacks",
                "frozen_model": "Immutable after creation for thread safety",
            },
        })

        return schema


class HyperliquidUserEventEnvelope(HyperliquidRawWebSocketEnvelope):
    """Specialized envelope for userEvents channel messages.

    The userEvents channel has a specific structure where the data field
    contains different event types (fills, orders, positions, etc.).

    Example:
    {
        "channel": "userEvents",
        "data": {
            "fills": [...],
            "orders": [...],
            "positions": {...}
        }
    }
    """

    # No need to redefine fields - they're inherited from parent
    # We just add additional validation

    @field_validator("channel")
    @classmethod
    def validate_user_event_channel(cls, v: str) -> str:
        """Ensure channel is 'userEvents'."""
        if v != "userEvents":
            msg = f"HyperliquidUserEventEnvelope requires channel='userEvents', got '{v}'"
            raise ValueError(msg)
        return v

    @field_validator("data")
    @classmethod
    def validate_user_event_structure(cls, v: dict[str, Any] | list[Any]) -> dict[str, Any]:
        """Validate that user event data has expected structure.

        User events typically contain one or more of:
        - fills: List of fill events
        - orders: List of order updates
        - positions: Position updates
        - ledgerUpdates: Account ledger changes

        Args:
            v: The data dictionary to validate.

        Returns:
            The validated data dictionary.

        Note:
            We don't enforce required fields as the structure may vary
            based on what events occurred.
        """
        # Ensure data is a dict for userEvents
        if not isinstance(v, dict):
            msg = f"User event data must be a dict, got {type(v).__name__}"
            raise TypeError(msg)

        # User event data should be non-empty
        if not v:
            msg = "User event data cannot be empty"
            raise ValueError(msg)

        # Validate known user event types if present
        known_event_types = {"fills", "orders", "positions", "ledgerUpdates"}
        for key in v:
            if key in known_event_types and not isinstance(v[key], (list, dict)):
                msg = f"User event '{key}' must be a list or dict, got {type(v[key]).__name__}"
                raise ValueError(msg)

        return v

    def get_envelope_type(self) -> str:
        """Get human-readable envelope type name.

        Returns:
            String identifying the envelope type.
        """
        return "HyperliquidUserEventEnvelope"


class HyperliquidSubscriptionResponse(BaseModel):
    """Model for Hyperliquid subscription response messages.

    Hyperliquid sends a confirmation when you subscribe to a channel.
    Unlike Backpack's simple {"result": true}, Hyperliquid provides
    detailed information about what was subscribed.

    Example:
    {
        "channel": "subscriptionResponse",
        "data": {
            "method": "subscription",
            "subscription": {"type": "l2Book", "coin": "BTC"}
        }
    }
    """

    channel: Literal["subscriptionResponse"] = Field(
        default="subscriptionResponse",
        description="Always 'subscriptionResponse' for subscription confirmations",
    )
    data: dict[str, Any] = Field(..., description="Subscription confirmation details")

    model_config = ConfigDict(
        frozen=True,
        str_strip_whitespace=True,
        extra="forbid",
    )

    @field_validator("data")
    @classmethod
    def validate_subscription_data(cls, v: dict[str, Any]) -> dict[str, Any]:
        """Validate subscription response data structure."""
        # Check for required fields in subscription response
        if "method" not in v:
            msg = "Subscription response must contain 'method' field"
            raise ValueError(msg)

        if "subscription" not in v:
            msg = "Subscription response must contain 'subscription' field"
            raise ValueError(msg)

        # Validate subscription details
        sub_details = v["subscription"]
        if not isinstance(sub_details, dict):
            msg = f"Subscription details must be dict, got {type(sub_details).__name__}"
            raise TypeError(msg)

        # Check for type field in subscription
        if "type" not in sub_details:
            msg = "Subscription details must contain 'type' field"
            raise ValueError(msg)

        return v

    @computed_field
    def subscription_type(self) -> str | None:
        """Extract the subscription type (channel) from the response."""
        if _is_string_dict(self.data):
            subscription_data = self.data.get("subscription")
            if _is_string_dict(subscription_data):
                type_data = subscription_data.get("type")
                return type_data if isinstance(type_data, str) else None
        return None

    @computed_field
    def subscription_coin(self) -> str | None:
        """Extract the coin/symbol from the subscription if present."""
        if _is_string_dict(self.data):
            subscription_data = self.data.get("subscription")
            if _is_string_dict(subscription_data):
                coin_data = subscription_data.get("coin")
                return coin_data if isinstance(coin_data, str) else None
        return None

    @computed_field
    def is_successful(self) -> bool:
        """Check if the subscription was successful.

        For Hyperliquid, presence of subscription details indicates success.
        """
        if _is_string_dict(self.data):
            subscription = self.data.get("subscription")
            return subscription is not None and _is_string_dict(subscription)
        return False


# Union type for all possible envelope formats
HyperliquidWebSocketMessage = (
    HyperliquidRawWebSocketEnvelope | HyperliquidUserEventEnvelope | HyperliquidSubscriptionResponse
)


def detect_hyperliquid_envelope_type(message: dict[str, Any]) -> str:
    """Detect which type of Hyperliquid envelope a message uses.

    Args:
        message: Raw WebSocket message dictionary.

    Returns:
        One of: "standard", "userEvents", "subscriptionResponse", "unknown"
    """
    # Message is already typed as dict[str, Any]

    channel = message.get("channel")
    if not channel:
        return "unknown"

    if channel == "userEvents":
        return "userEvents"

    if channel == "subscriptionResponse":
        return "subscriptionResponse"

    return "standard"


def validate_hyperliquid_envelope(message: dict[str, Any]) -> HyperliquidWebSocketMessage:
    """Validate and parse a Hyperliquid WebSocket message envelope.

    This function validates the message envelope structure before any content processing,
    ensuring type safety and catching malformed messages early.

    Args:
        message: Raw WebSocket message dictionary.

    Returns:
        Validated envelope model instance (either HyperliquidRawWebSocketEnvelope
        or HyperliquidUserEventEnvelope).

    Raises:
        ValidationError: If the message doesn't match expected envelope format.
        ValueError: If the message structure is fundamentally invalid.

    Examples:
        >>> # Valid L2 book message
        >>> msg = {"channel": "l2Book", "data": {"coin": "BTC", "levels": []}}
        >>> envelope = validate_hyperliquid_envelope(msg)
        >>> assert envelope.channel == "l2Book"

        >>> # Invalid message - missing channel
        >>> msg = {"data": {"coin": "BTC"}}
        >>> validate_hyperliquid_envelope(msg)  # Raises ValueError

        >>> # Invalid message - wrong type
        >>> validate_hyperliquid_envelope("not a dict")  # Raises ValueError
    """
    # Check if message is empty
    if not message:
        msg = f"Hyperliquid WebSocket message must be a dictionary, got {type(message).__name__}"
        raise ValueError(msg)

    # Check required fields
    if "channel" not in message:
        msg = (
            "Missing required 'channel' field in Hyperliquid WebSocket message. "
            f"Received keys: {list(message.keys())}"
        )
        raise ValueError(msg)

    if "data" not in message:
        msg = (
            "Missing required 'data' field in Hyperliquid WebSocket message. "
            f"Received keys: {list(message.keys())}"
        )
        raise ValueError(msg)

    # Detect envelope type and validate accordingly
    envelope_type = detect_hyperliquid_envelope_type(message)

    try:
        if envelope_type == "userEvents":
            # Use specialized userEvents envelope
            return HyperliquidUserEventEnvelope.model_validate(message)
        if envelope_type == "subscriptionResponse":
            # Use subscription response model
            return HyperliquidSubscriptionResponse.model_validate(message)
        # Use standard envelope for all other channels
        return HyperliquidRawWebSocketEnvelope.model_validate(message)
    except ValidationError as e:
        # Re-raise with more context
        msg = (
            f"Invalid Hyperliquid WebSocket envelope format. "
            f"Channel: {message.get('channel', 'unknown')}. "
            f"Validation errors: {e}"
        )
        raise ValueError(msg) from e
