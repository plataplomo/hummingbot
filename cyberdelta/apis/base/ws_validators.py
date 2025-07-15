"""Shared WebSocket Payload Validators.

This module provides common validation utilities for WebSocket payloads
across different exchanges, promoting code reuse and consistent validation.
"""

from __future__ import annotations

import re
import time
from typing import Any

from cyberdelta.config.structlog_config import get_logger


class InvalidPayloadTypeError(TypeError):
    """Raised when payload type is invalid for WebSocket processing."""

    def __init__(self, context: str, actual_type: type, expected_type: str) -> None:
        """Initialize with context and type information."""
        super().__init__(
            f"Invalid {context} payload type: {actual_type.__name__}. Expected {expected_type}."
        )


class MissingRequiredFieldsError(ValueError):
    """Raised when required fields are missing from payload."""

    def __init__(self, context: str, missing_fields: list[str]) -> None:
        """Initialize with context and missing fields."""
        super().__init__(f"Missing required fields in {context}: {missing_fields}")


class InvalidItemTypeError(TypeError):
    """Raised when an item in a list has an invalid type."""

    def __init__(
        self, context: str, index: int, actual_type: type[Any], expected_type: type[Any]
    ) -> None:
        """Initialize with context and type information."""
        super().__init__(
            f"{context} payload item {index} has invalid type: "
            f"{actual_type.__name__}. Expected {expected_type.__name__}."
        )


class InvalidFieldTypeError(TypeError):
    """Raised when a field has an invalid type."""

    def __init__(self, context: str, actual_type: type[Any], expected_type: str) -> None:
        """Initialize with context and type information."""
        super().__init__(
            f"Invalid {context} type: {actual_type.__name__}. Expected {expected_type}."
        )


class PayloadSizeError(ValueError):
    """Raised when payload size constraints are violated."""

    def __init__(
        self, context: str, actual: int, constraint: str, limit: int, unit: str = "keys"
    ) -> None:
        """Initialize with size constraint details."""
        super().__init__(f"{context} payload must have {constraint} {limit} {unit}, got {actual}")


class UnexpectedFieldsError(ValueError):
    """Raised when unexpected fields are found in payload."""

    def __init__(
        self, context: str, unexpected_fields: list[str], allowed_fields: list[str]
    ) -> None:
        """Initialize with field information."""
        super().__init__(
            f"Unexpected fields in {context}: {unexpected_fields}. Allowed fields: {allowed_fields}"
        )


class InvalidFormatError(ValueError):
    """Raised when a value doesn't match the expected format pattern."""

    def __init__(self, context: str, value: str, pattern: str) -> None:
        """Initialize with format validation details."""
        super().__init__(f"Invalid {context} format: '{value}'. Must match pattern: {pattern}")


class InvalidNumericValueError(ValueError):
    """Raised when a string cannot be converted to a number."""

    def __init__(self, context: str, value: str) -> None:
        """Initialize with numeric validation details."""
        super().__init__(f"Invalid {context}: '{value}' is not a valid number")


class NumericRangeError(ValueError):
    """Raised when a numeric value is outside the allowed range."""

    def __init__(self, context: str, value: float, constraint: str, limit: float) -> None:
        """Initialize with range validation details."""
        super().__init__(f"Invalid {context}: {value} must be {constraint} {limit}")


class InvalidTimestampError(ValueError):
    """Raised when timestamp validation fails."""

    def __init__(self, context: str, value: int, reason: str) -> None:
        """Initialize with timestamp validation details."""
        super().__init__(f"{context} {reason}: {value}")


class InvalidTopicFormatError(ValueError):
    """Raised when topic format is invalid."""

    def __init__(self, topic: str, format_description: str) -> None:
        """Initialize with topic format validation details."""
        super().__init__(f"Invalid Backpack topic format: '{topic}'. Expected {format_description}")


class InvalidTopicTypeError(ValueError):
    """Raised when topic type is invalid."""

    def __init__(self, topic_type: str, valid_types: set[str]) -> None:
        """Initialize with topic type validation details."""
        super().__init__(
            f"Invalid Backpack topic type: '{topic_type}'. "
            f"Valid types: {', '.join(sorted(valid_types))}"
        )


# Constants for validation
YEAR_2000_TIMESTAMP = 946684800  # Jan 1, 2000
MIN_ORDER_BOOK_LEVEL_ITEMS = 2
TOPIC_PARTS_COUNT = 2  # Expected number of parts in topic.symbol format


logger = get_logger(__name__)


class WebSocketPayloadValidators:
    """Collection of reusable payload validation methods."""

    # Common patterns for validation
    SYMBOL_PATTERN = re.compile(r"^[A-Z0-9_-]{1,20}$")
    TOPIC_PATTERN = re.compile(r"^[a-zA-Z0-9._-]{1,50}$")
    ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]{1,100}$")

    @staticmethod
    def validate_dict_payload(
        payload: dict[str, Any] | list[Any] | str | float | bool | None,
        context: str = "message",
        min_keys: int = 0,
        max_keys: int | None = None,
    ) -> dict[str, Any]:
        """Validate that payload is a dictionary with optional size constraints.

        Args:
            payload: The payload to validate.
            context: Context string for error messages.
            min_keys: Minimum number of keys required.
            max_keys: Maximum number of keys allowed (None for no limit).

        Returns:
            The validated dictionary payload.

        Raises:
            ValueError: If validation fails.

        """
        if not isinstance(payload, dict):
            raise InvalidPayloadTypeError(context, type(payload), "dict")

        key_count = len(payload)
        if key_count < min_keys:
            raise PayloadSizeError(context, key_count, "at least", min_keys)

        if max_keys is not None and key_count > max_keys:
            raise PayloadSizeError(context, key_count, "at most", max_keys)

        return payload

    @staticmethod
    def validate_list_payload(
        payload: dict[str, Any] | list[Any] | str | float | bool | None,
        context: str = "message",
        min_length: int = 0,
        max_length: int | None = None,
        item_type: type | None = None,
    ) -> list[Any]:
        """Validate that payload is a list with optional constraints.

        Args:
            payload: The payload to validate.
            context: Context string for error messages.
            min_length: Minimum list length.
            max_length: Maximum list length (None for no limit).
            item_type: Expected type for list items (None for no type checking).

        Returns:
            The validated list payload.

        Raises:
            ValueError: If validation fails.

        """
        if not isinstance(payload, list):
            raise InvalidPayloadTypeError(context, type(payload), "list")

        length = len(payload)
        if length < min_length:
            raise PayloadSizeError(context, length, "at least", min_length, "items")

        if max_length is not None and length > max_length:
            raise PayloadSizeError(context, length, "at most", max_length, "items")

        if item_type is not None:
            for i, item in enumerate(payload):
                if not isinstance(item, item_type):
                    # Get the actual type of the item
                    actual_type = object if item is None else item.__class__
                    raise InvalidItemTypeError(context, i, actual_type, item_type)

        return payload

    @staticmethod
    def validate_required_fields(
        payload: dict[str, Any],
        required_fields: list[str],
        context: str = "message",
    ) -> dict[str, Any]:
        """Validate that required fields are present in payload.

        Args:
            payload: The payload to validate.
            required_fields: List of required field names.
            context: Context string for error messages.

        Returns:
            The validated payload.

        Raises:
            ValueError: If any required fields are missing.

        """
        missing_fields = [field for field in required_fields if field not in payload]
        if missing_fields:
            raise MissingRequiredFieldsError(context, missing_fields)
        return payload

    @staticmethod
    def validate_optional_fields(
        payload: dict[str, Any],
        allowed_fields: list[str],
        context: str = "message",
    ) -> dict[str, Any]:
        """Validate that only allowed fields are present in payload.

        Args:
            payload: The payload to validate.
            allowed_fields: List of allowed field names.
            context: Context string for error messages.

        Returns:
            The validated payload.

        Raises:
            ValueError: If any unexpected fields are present.

        """
        unexpected_fields = [field for field in payload if field not in allowed_fields]
        if unexpected_fields:
            raise UnexpectedFieldsError(context, unexpected_fields, allowed_fields)
        return payload

    @classmethod
    def validate_symbol(
        cls,
        symbol: str | float | bool | None,
        context: str = "symbol",
    ) -> str:
        """Validate trading symbol format.

        Args:
            symbol: The symbol to validate.
            context: Context string for error messages.

        Returns:
            The validated symbol string.

        Raises:
            ValueError: If symbol format is invalid.

        """
        if not isinstance(symbol, str):
            raise InvalidFieldTypeError(context, type(symbol), "str")

        if not cls.SYMBOL_PATTERN.match(symbol):
            raise InvalidFormatError(context, symbol, cls.SYMBOL_PATTERN.pattern)

        return symbol

    @classmethod
    def validate_topic(
        cls,
        topic: str | float | bool | None,
        context: str = "topic",
    ) -> str:
        """Validate WebSocket topic format.

        Args:
            topic: The topic to validate.
            context: Context string for error messages.

        Returns:
            The validated topic string.

        Raises:
            ValueError: If topic format is invalid.

        """
        if not isinstance(topic, str):
            raise InvalidFieldTypeError(context, type(topic), "str")

        if not cls.TOPIC_PATTERN.match(topic):
            raise InvalidFormatError(context, topic, cls.TOPIC_PATTERN.pattern)

        return topic

    @classmethod
    def validate_id(
        cls,
        id_value: str | float | bool | None,
        context: str = "id",
    ) -> str:
        """Validate ID field format.

        Args:
            id_value: The ID to validate.
            context: Context string for error messages.

        Returns:
            The validated ID string.

        Raises:
            ValueError: If ID format is invalid.

        """
        if not isinstance(id_value, str):
            raise InvalidFieldTypeError(context, type(id_value), "str")

        if not cls.ID_PATTERN.match(id_value):
            raise InvalidFormatError(context, id_value, cls.ID_PATTERN.pattern)

        return id_value

    @staticmethod
    def validate_numeric_string(
        value: str | float | bool | None,
        context: str = "numeric value",
        min_value: float | None = None,
        max_value: float | None = None,
    ) -> str:
        """Validate that a string represents a valid number.

        Args:
            value: The value to validate.
            context: Context string for error messages.
            min_value: Minimum allowed numeric value.
            max_value: Maximum allowed numeric value.

        Returns:
            The validated string.

        Raises:
            ValueError: If validation fails.

        """
        if not isinstance(value, str):
            raise InvalidFieldTypeError(context, type(value), "str")

        try:
            numeric_value = float(value)
        except ValueError as e:
            raise InvalidNumericValueError(context, value) from e

        if min_value is not None and numeric_value < min_value:
            raise NumericRangeError(context, numeric_value, "at least", min_value)

        if max_value is not None and numeric_value > max_value:
            raise NumericRangeError(context, numeric_value, "at most", max_value)

        return value

    @staticmethod
    def validate_timestamp(
        timestamp: float | str | bool | None,
        context: str = "timestamp",
        allow_future: bool = True,
    ) -> int:
        """Validate timestamp value.

        Args:
            timestamp: The timestamp to validate.
            context: Context string for error messages.
            allow_future: Whether to allow future timestamps.

        Returns:
            The validated timestamp.

        Raises:
            ValueError: If timestamp is invalid.

        """
        if not isinstance(timestamp, int):
            raise InvalidFieldTypeError(context, type(timestamp), "int")

        if timestamp < 0:
            raise InvalidTimestampError(context, timestamp, "cannot be negative")

        # Basic sanity check - not before year 2000 or too far in future
        if timestamp < YEAR_2000_TIMESTAMP:
            raise InvalidTimestampError(context, timestamp, "is too old")

        if not allow_future:
            current_time = int(time.time())
            if timestamp > current_time:
                raise InvalidTimestampError(context, timestamp, "cannot be in the future")

        return timestamp


class ExchangeSpecificValidators:
    """Exchange-specific validation utilities."""

    @staticmethod
    def validate_backpack_topic(topic: str) -> tuple[str, str]:
        """Validate and parse Backpack topic format.

        Args:
            topic: Topic string in format "type.symbol" (e.g., "depth.BTC_USDC").

        Returns:
            Tuple of (topic_type, symbol).

        Raises:
            ValueError: If topic format is invalid.

        """
        if "." not in topic:
            raise InvalidTopicFormatError(topic, "'type.symbol'")

        parts = topic.split(".", 1)
        if len(parts) != TOPIC_PARTS_COUNT:
            raise InvalidTopicFormatError(topic, "'type.symbol'")

        topic_type, symbol = parts

        # Valid Backpack topic types
        # NOTE: Backpack uses "trade" (singular) for WebSocket streams,
        # but "trades" is also accepted for compatibility
        valid_topic_types = {"depth", "ticker", "trade", "trades"}

        if topic_type not in valid_topic_types:
            raise InvalidTopicTypeError(topic_type, valid_topic_types)

        # Validate components
        WebSocketPayloadValidators.validate_topic(topic_type, "topic type")
        WebSocketPayloadValidators.validate_symbol(symbol, "symbol")

        return topic_type, symbol

    @staticmethod
    def validate_hyperliquid_channel(channel: str) -> str:
        """Validate Hyperliquid channel format.

        Args:
            channel: Channel name (e.g., "l2Book", "trades", "userEvents").

        Returns:
            The validated channel string.

        Raises:
            ValueError: If channel format is invalid.

        """
        # Known Hyperliquid channels
        valid_channels = {
            "l2Book",
            "trades",
            "userEvents",
            "allMids",
            "notification",
            "webData2",
        }

        if channel not in valid_channels:
            logger.warning(
                "unknown_hyperliquid_channel",
                channel=channel,
                valid_channels=list(valid_channels),
            )
            # Don't fail - just log warning for unknown channels

        return WebSocketPayloadValidators.validate_topic(channel, "channel")

    @staticmethod
    def validate_order_book_level(
        level: dict[str, Any] | list[Any] | str | float | bool | None,
    ) -> list[str]:
        """Validate order book level format (price, quantity pair).

        Args:
            level: Order book level to validate.

        Returns:
            Validated level as list of strings.

        Raises:
            ValueError: If level format is invalid.

        """
        level_list = WebSocketPayloadValidators.validate_list_payload(
            level, "order book level", min_length=2, max_length=2
        )

        # Validate price and quantity are numeric strings
        price, quantity = level_list[0], level_list[1]

        WebSocketPayloadValidators.validate_numeric_string(price, "price", min_value=0)
        WebSocketPayloadValidators.validate_numeric_string(quantity, "quantity", min_value=0)

        return level_list
