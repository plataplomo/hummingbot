"""Shared WebSocket Payload Validators.

This module provides common validation utilities for WebSocket payloads
across different exchanges, promoting code reuse and consistent validation.
"""

from __future__ import annotations

import re
import time
from typing import Any

from cyberdelta.apis.base.validation_policies import TimestampPolicy

# Import exceptions from unified hierarchy (Step 30: Migration completed)
from cyberdelta.apis.websocket.ws_exceptions import (
    InvalidFieldTypeError,
    InvalidFormatError,
    InvalidItemTypeError,
    InvalidNumericValueError,
    InvalidPayloadTypeError,
    InvalidTimestampError,
    MissingRequiredFieldsError,
    NumericRangeError,
    PayloadSizeError,
    UnexpectedFieldsError,
)
from cyberdelta.config.structlog_config import get_logger


# Type alias for validation input - covers all possible invalid input types
ValidationInput = dict[str, Any] | list[Any] | str | float | int | None


# Constants for validation
YEAR_2000_TIMESTAMP = 946684800  # Jan 1, 2000
MIN_ORDER_BOOK_LEVEL_ITEMS = 2


logger = get_logger(__name__)


class WebSocketPayloadValidators:
    """Collection of reusable payload validation methods."""

    # Common patterns for validation
    SYMBOL_PATTERN = re.compile(r"^[A-Z0-9_-]{1,20}$")
    TOPIC_PATTERN = re.compile(r"^[a-zA-Z0-9._-]{1,50}$")
    ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]{1,100}$")

    @staticmethod
    def validate_dict_payload(
        payload: ValidationInput,
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
            InvalidPayloadTypeError: If payload is not a dictionary.
            PayloadSizeError: If dictionary size constraints are violated.

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
        payload: ValidationInput,
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
            InvalidPayloadTypeError: If payload is not a list.
            PayloadSizeError: If list size constraints are violated.
            InvalidItemTypeError: If any item has an invalid type when item_type is specified.

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
            MissingRequiredFieldsError: If any required fields are missing.

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
            UnexpectedFieldsError: If any unexpected fields are present.

        """
        unexpected_fields = [field for field in payload if field not in allowed_fields]
        if unexpected_fields:
            raise UnexpectedFieldsError(context, unexpected_fields, allowed_fields)
        return payload

    @classmethod
    def validate_symbol(
        cls,
        symbol: ValidationInput,
        context: str = "symbol",
        exchange_id: str | None = None,
    ) -> str:
        """Validate trading symbol format.

        Args:
            symbol: The symbol to validate.
            context: Context string for error messages.
            exchange_id: Optional exchange identifier for enhanced validation.

        Returns:
            The validated symbol string.

        Raises:
            InvalidFieldTypeError: If symbol is not a string.
            InvalidFormatError: If symbol format is invalid.

        """
        if not isinstance(symbol, str):
            raise InvalidFieldTypeError(context, type(symbol), "str")

        # Pattern-based validation
        if not cls.SYMBOL_PATTERN.match(symbol):
            raise InvalidFormatError(context, symbol, cls.SYMBOL_PATTERN.pattern)

        return symbol

    @classmethod
    def validate_websocket_symbol(
        cls,
        symbol: ValidationInput,
        context: str = "symbol",
        exchange_id: str | None = None,
    ) -> str:
        """Validate WebSocket symbol with integer support.

        Args:
            symbol: The symbol to validate (can be string or integer).
            context: Context string for error messages.
            exchange_id: Optional exchange identifier for enhanced validation.

        Returns:
            The validated symbol string.

        Raises:
            InvalidFieldTypeError: If symbol is not a string or integer.
            InvalidFormatError: If symbol format doesn't match expected pattern.

        """
        # Convert to string if necessary
        if isinstance(symbol, int):
            symbol_str = str(symbol)
        elif isinstance(symbol, str):
            symbol_str = symbol
        else:
            raise InvalidFieldTypeError(context, type(symbol), "string or integer")

        # Apply pattern validation
        if not cls.SYMBOL_PATTERN.match(symbol_str):
            raise InvalidFormatError(context, symbol_str, cls.SYMBOL_PATTERN.pattern)

        return symbol_str

    @classmethod
    def validate_topic(
        cls,
        topic: ValidationInput,
        context: str = "topic",
    ) -> str:
        """Validate WebSocket topic format.

        Args:
            topic: The topic to validate.
            context: Context string for error messages.

        Returns:
            The validated topic string.

        Raises:
            InvalidFieldTypeError: If topic is not a string.
            InvalidFormatError: If topic format is invalid.

        """
        if not isinstance(topic, str):
            raise InvalidFieldTypeError(context, type(topic), "str")

        if not cls.TOPIC_PATTERN.match(topic):
            raise InvalidFormatError(context, topic, cls.TOPIC_PATTERN.pattern)

        return topic

    @classmethod
    def validate_id(
        cls,
        id_value: ValidationInput,
        context: str = "id",
    ) -> str:
        """Validate ID field format.

        Args:
            id_value: The ID to validate.
            context: Context string for error messages.

        Returns:
            The validated ID string.

        Raises:
            InvalidFieldTypeError: If ID is not a string.
            InvalidFormatError: If ID format is invalid.

        """
        if not isinstance(id_value, str):
            raise InvalidFieldTypeError(context, type(id_value), "str")

        if not cls.ID_PATTERN.match(id_value):
            raise InvalidFormatError(context, id_value, cls.ID_PATTERN.pattern)

        return id_value

    @staticmethod
    def validate_numeric_string(
        value: ValidationInput,
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
            InvalidFieldTypeError: If value is not a string.
            InvalidNumericValueError: If string cannot be converted to a number.
            NumericRangeError: If numeric value is outside the allowed range.

        """
        if not isinstance(value, str):
            raise InvalidFieldTypeError(context, type(value), "str")

        try:
            numeric_value = float(value)
        except ValueError as e:
            reason = f"'{value}' is not a valid number"
            raise InvalidNumericValueError(context, value, reason) from e

        if min_value is not None and numeric_value < min_value:
            raise NumericRangeError(context, numeric_value, min_value=min_value)

        if max_value is not None and numeric_value > max_value:
            raise NumericRangeError(context, numeric_value, max_value=max_value)

        return value

    @staticmethod
    def validate_timestamp(
        timestamp: int,
        context: str = "timestamp",
        timestamp_policy: TimestampPolicy = TimestampPolicy.ALLOW_FUTURE,
    ) -> int:
        """Validate timestamp value.

        Args:
            timestamp: The timestamp to validate.
            context: Context string for error messages.
            timestamp_policy: Policy for timestamp validation.

        Returns:
            The validated timestamp.

        Raises:
            InvalidTimestampError: If timestamp is negative, too old, or in the future
                (when RESTRICT_TO_PAST policy is used).

        """
        # Type is guaranteed by function signature annotation

        if timestamp < 0:
            raise InvalidTimestampError(context, timestamp, "cannot be negative")

        # Basic sanity check - not before year 2000 or too far in future
        if timestamp < YEAR_2000_TIMESTAMP:
            raise InvalidTimestampError(context, timestamp, "is too old")

        if timestamp_policy == TimestampPolicy.RESTRICT_TO_PAST:
            current_time = int(time.time())
            if timestamp > current_time:
                raise InvalidTimestampError(context, timestamp, "cannot be in the future")

        return timestamp
