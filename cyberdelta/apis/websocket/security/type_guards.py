"""TypeGuards for WebSocket message type safety.

This module provides TypeGuard functions to enable runtime type narrowing
and eliminate unnecessary isinstance checks throughout the WebSocket pipeline.
"""

from __future__ import annotations

from typing import Any, TypeGuard


# Define secure types for validation
SecureDict = dict[str, str | int | float | bool | None]
SecureList = list[str | int | float | bool | None]
SecureValue = str | int | float | bool | SecureDict | SecureList | None

# Constants for validation thresholds
MIN_STREAM_PARTS = 2
MAX_MESSAGE_ID_LENGTH = 64
MAX_CONNECTION_ID_LENGTH = 32


class WebSocketTypeGuards:
    """Collection of TypeGuard functions for WebSocket message processing."""

    @staticmethod
    def is_backpack_message(data: object) -> TypeGuard[dict[str, Any]]:
        """Type guard for Backpack messages.

        Args:
            data: Raw message data to check

        Returns:
            True if data contains Backpack message structure
        """
        return (
            isinstance(data, dict)
            and "stream" in data
            and isinstance(data["stream"], str)
            and "data" in data
        )

    @staticmethod
    def is_hyperliquid_message(data: object) -> TypeGuard[dict[str, Any]]:
        """Type guard for Hyperliquid messages.

        Args:
            data: Raw message data to check

        Returns:
            True if data contains Hyperliquid message structure
        """
        return (
            isinstance(data, dict)
            and "channel" in data
            and isinstance(data["channel"], str)
            and "data" in data
        )

    @staticmethod
    def is_hyperliquid_user_event(data: object) -> TypeGuard[dict[str, Any]]:
        """Type guard for Hyperliquid user event messages.

        Args:
            data: Raw message data to check

        Returns:
            True if data contains Hyperliquid user event structure
        """
        if not isinstance(data, dict):
            return False
        # Check each condition separately for better type safety
        has_channel = "channel" in data
        has_correct_channel = has_channel and data.get("channel") == "userEvents"  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
        has_data = "data" in data
        return has_channel and has_correct_channel and has_data  # pyright: ignore[reportUnknownVariableType]

    @staticmethod
    def is_secure_dict(obj: object) -> TypeGuard[SecureDict]:
        """Type guard for secure dictionary validation.

        Args:
            obj: Object to validate

        Returns:
            True if object is a secure dictionary with valid types
        """
        if not isinstance(obj, dict):
            return False
        # Check keys and values separately for better type safety
        valid_keys = all(isinstance(k, str) for k in obj)  # pyright: ignore[reportUnknownVariableType]
        valid_values = all(
            isinstance(v, (str, int, float, bool, type(None)))
            for v in obj.values()  # pyright: ignore[reportUnknownVariableType]
        )
        return valid_keys and valid_values

    @staticmethod
    def is_secure_list(obj: object) -> TypeGuard[SecureList]:
        """Type guard for secure list validation.

        Args:
            obj: Object to validate

        Returns:
            True if object is a secure list with valid types
        """
        if not isinstance(obj, list):
            return False
        # Check all items in the list for valid types
        valid_items = True
        for item in obj:  # pyright: ignore[reportUnknownVariableType]
            if not isinstance(item, (str, int, float, bool, type(None))):
                valid_items = False
                break
        return valid_items

    @staticmethod
    def is_secure_value(obj: object) -> TypeGuard[SecureValue]:
        """Type guard for secure value validation.

        Args:
            obj: Object to validate

        Returns:
            True if object is a secure value type
        """
        return (
            isinstance(obj, (str, int, float, bool, type(None)))
            or WebSocketTypeGuards.is_secure_dict(obj)
            or WebSocketTypeGuards.is_secure_list(obj)
        )

    @staticmethod
    def is_valid_stream_format(stream: object) -> TypeGuard[str]:
        """Type guard for valid Backpack stream format.

        Args:
            stream: Stream value to validate

        Returns:
            True if stream is a valid format string
        """
        return (
            isinstance(stream, str)
            and len(stream) > 0
            and "." in stream
            and len(stream.split(".")) >= MIN_STREAM_PARTS
        )

    @staticmethod
    def is_valid_channel_format(channel: object) -> TypeGuard[str]:
        """Type guard for valid Hyperliquid channel format.

        Args:
            channel: Channel value to validate

        Returns:
            True if channel is a valid format string
        """
        return (isinstance(channel, str) and len(channel) > 0 and channel.isalnum()) or channel in {
            "userEvents",
            "l2Book",
            "trades",
            "candle",
            "allMids",
        }

    @staticmethod
    def is_valid_message_id(message_id: object) -> TypeGuard[str]:
        """Type guard for valid message ID format.

        Args:
            message_id: Message ID to validate

        Returns:
            True if message_id is a valid string
        """
        return (
            isinstance(message_id, str)
            and 1 <= len(message_id) <= MAX_MESSAGE_ID_LENGTH
            and message_id.replace("-", "").replace("_", "").isalnum()
        )

    @staticmethod
    def is_valid_connection_id(connection_id: object) -> TypeGuard[str]:
        """Type guard for valid connection ID format.

        Args:
            connection_id: Connection ID to validate

        Returns:
            True if connection_id is a valid string
        """
        return (
            isinstance(connection_id, str)
            and 1 <= len(connection_id) <= MAX_CONNECTION_ID_LENGTH
            and connection_id.replace("-", "").replace("_", "").isalnum()
        )

    @staticmethod
    def is_numeric_string(value: object) -> TypeGuard[str]:
        """Type guard for numeric string values.

        Args:
            value: Value to check

        Returns:
            True if value is a string representing a number
        """
        if not isinstance(value, str):
            return False

        try:
            float(value)
        except ValueError:
            return False
        else:
            return True

    @staticmethod
    def has_required_envelope_fields(
        data: object,
        required_fields: list[str],
    ) -> TypeGuard[dict[str, Any]]:
        """Type guard for checking required envelope fields.

        Args:
            data: Data to validate
            required_fields: List of required field names

        Returns:
            True if all required fields are present
        """
        return (
            isinstance(data, dict)
            and all(field in data for field in required_fields)
            and all(data[field] is not None for field in required_fields)
        )


# Convenience aliases for commonly used type guards
is_backpack_message = WebSocketTypeGuards.is_backpack_message
is_hyperliquid_message = WebSocketTypeGuards.is_hyperliquid_message
is_secure_dict = WebSocketTypeGuards.is_secure_dict
is_secure_list = WebSocketTypeGuards.is_secure_list
is_secure_value = WebSocketTypeGuards.is_secure_value
is_valid_stream_format = WebSocketTypeGuards.is_valid_stream_format
is_valid_channel_format = WebSocketTypeGuards.is_valid_channel_format
