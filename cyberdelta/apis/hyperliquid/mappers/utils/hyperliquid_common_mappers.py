"""Common mapper utilities for Hyperliquid mappers.

This module contains shared utility functions that implement the base MapperProtocol
methods, following the Backpack pattern for consistency across all mappers.
"""

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, TypeGuard, TypeVar

from cyberdelta.apis.exceptions.field_validation import FieldError
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.utils.parsing import parse_decimal_value


class NumberValidationPolicy(Enum):
    """Policy for number validation in Hyperliquid mappers."""

    POSITIVE_ONLY = "positive_only"
    """Only positive numbers allowed (> 0)."""

    NON_NEGATIVE = "non_negative"
    """Non-negative numbers allowed (>= 0)."""


logger = get_logger(__name__)

T = TypeVar("T")


class NonPositiveValueError(FieldError):
    """Raised when a value must be positive but is not."""

    def __init__(self, field_name: str, value: Decimal) -> None:
        """Initialize non-positive value error."""
        super().__init__(
            "Value must be positive",
            field_name=field_name,
            source_value=value,
            code="VALUE_NOT_POSITIVE",
        )


class HyperliquidCommonMappers:
    """Common mapper utilities following the Backpack pattern.

    This class provides static methods that implement the base MapperProtocol
    methods, ensuring consistency across all Hyperliquid mappers.
    """

    # Constants
    EXPECTED_SYMBOL_PARTS = 1  # Hyperliquid uses single symbols like "BTC" or "ETH"

    @staticmethod
    def parse_decimal_safely(
        value: str | float | Decimal | None,
        default: Decimal = Decimal(0),
    ) -> Decimal:
        """Parse decimal values safely with default fallback.

        Args:
            value: The value to parse as a decimal
            default: Default value to return if parsing fails

        Returns:
            Parsed decimal value or default
        """
        try:
            if value is None:
                return default

            if isinstance(value, Decimal):
                return value

            return Decimal(str(value))
        except (ValueError, TypeError, InvalidOperation) as e:
            logger.warning(
                "decimal_parsing_failed",
                value=value,
                default=default,
                error=str(e),
                message="Failed to parse decimal value, using default",
            )
            return default

    @staticmethod
    def timestamp_ms_to_datetime(timestamp_ms: float | None) -> datetime | None:
        """Convert millisecond timestamp to datetime.

        Args:
            timestamp_ms: Millisecond timestamp

        Returns:
            Datetime object with UTC timezone or None if timestamp is None
        """
        if timestamp_ms is None:
            return None

        try:
            # Convert milliseconds to seconds
            timestamp_s = timestamp_ms / 1000.0
            return datetime.fromtimestamp(timestamp_s, tz=UTC)
        except (ValueError, OSError, OverflowError) as e:
            logger.warning(
                "timestamp_conversion_failed",
                timestamp_ms=timestamp_ms,
                error=str(e),
                message="Failed to convert timestamp to datetime",
            )
            return None

    # Type Guards and Safety Methods
    @staticmethod
    def _is_dict_with_key(value: object, key: str) -> TypeGuard[dict[str, Any]]:
        """Type guard to check if value is a dict with a specific key.

        Args:
            value: Value to check
            key: Key to look for in the dictionary

        Returns:
            True if value is a dict containing the key
        """
        return isinstance(value, dict) and key in value

    @staticmethod
    def _is_list(value: object) -> TypeGuard[list[Any]]:
        """Type guard to check if value is a list.

        Args:
            value: Value to check

        Returns:
            True if value is a list
        """
        return isinstance(value, list)

    @staticmethod
    def safe_get_nested(
        data: dict[str, Any],
        *keys: str,
        default: str | float | dict[str, Any] | list[Any] | bool | None = None,
    ) -> str | float | dict[str, Any] | list[Any] | bool | None:
        """Safely get a nested value from a dictionary.

        Args:
            data: Dictionary to search in
            *keys: Sequence of keys to traverse
            default: Default value if key path doesn't exist

        Returns:
            The value if found, otherwise default
        """
        current = data
        for key in keys:
            if not HyperliquidCommonMappers._is_dict_with_key(current, key):
                return default
            current = current[key]
        return current

    @staticmethod
    def ensure_list(
        value: object | None,
        default: list[object] | None = None,
    ) -> list[object] | None:
        """Ensure value is a list.

        If value is not a list, wrap it in a list. If value is None, return default.

        Args:
            value: Value to ensure is a list
            default: Default list if value is None

        Returns:
            List containing the value(s) or default
        """
        if value is None:
            return default
        if HyperliquidCommonMappers._is_list(value):
            return value
        return [value]

    # Validation Utilities
    @staticmethod
    def validate_positive_decimal(
        value: Decimal,
        field_name: str,
        validation_policy: NumberValidationPolicy = (NumberValidationPolicy.POSITIVE_ONLY),
    ) -> Decimal:
        """Validate that a decimal value meets the specified validation policy.

        Args:
            value: Decimal value to validate
            field_name: Name of the field for error reporting
            validation_policy: Policy determining acceptable values

        Returns:
            The validated decimal value

        Raises:
            NonPositiveValueError: If value is not positive
        """
        if (validation_policy == NumberValidationPolicy.POSITIVE_ONLY and value <= Decimal(0)) or (
            validation_policy == NumberValidationPolicy.NON_NEGATIVE and value < Decimal(0)
        ):
            raise NonPositiveValueError(field_name, value)

        return value

    # Mathematical Utilities
    @staticmethod
    def calculate_percentage(
        part: Decimal,
        whole: Decimal,
        decimal_places: int = 4,
    ) -> Decimal | None:
        """Calculate percentage safely.

        Args:
            part: The part value
            whole: The whole value
            decimal_places: Number of decimal places to round to

        Returns:
            Percentage as decimal or None if calculation is invalid
        """
        if whole == Decimal(0):
            return None

        try:
            # Calculate as ratio first (0.1234 for 12.34%)
            ratio = part / whole
            # Round to specified decimal places
            return ratio.quantize(Decimal(f"0.{'0' * decimal_places}"))
        except (ZeroDivisionError, InvalidOperation, ValueError):
            logger.warning(
                "percentage_calculation_failed",
                part=str(part),
                whole=str(whole),
                message="Failed to calculate percentage",
            )
            return None

    # Formatting Utilities
    @staticmethod
    def format_order_id(
        exchange_order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> str:
        """Format order ID for consistent display.

        Args:
            exchange_order_id: The exchange-assigned order ID
            client_order_id: The client-assigned order ID

        Returns:
            Formatted order ID string
        """
        if exchange_order_id and client_order_id:
            return f"{exchange_order_id} (client: {client_order_id})"
        if exchange_order_id:
            return exchange_order_id
        if client_order_id:
            return f"client: {client_order_id}"
        return "unknown"

    # Additional Utilities from Backpack Pattern

    @staticmethod
    def safe_divide(
        numerator: Decimal,
        denominator: Decimal,
        default: Decimal = Decimal(0),
    ) -> Decimal:
        """Safely divide two decimals with default on error.

        Args:
            numerator: The numerator
            denominator: The denominator
            default: Default value if division fails

        Returns:
            Result of division or default
        """
        if denominator == Decimal(0):
            return default

        try:
            return numerator / denominator
        except (InvalidOperation, ZeroDivisionError) as e:
            logger.warning(
                "safe_divide_failed",
                numerator=str(numerator),
                denominator=str(denominator),
                error=str(e),
                message="Division failed, returning default",
            )
            return default

    @staticmethod
    def round_to_tick_size(value: Decimal, tick_size: Decimal) -> Decimal:
        """Round a value to the nearest tick size.

        Args:
            value: The value to round
            tick_size: The tick size to round to

        Returns:
            Rounded value
        """
        if tick_size <= 0:
            return value

        try:
            # Round to nearest tick
            return (value / tick_size).quantize(Decimal(1)) * tick_size
        except (InvalidOperation, ZeroDivisionError):
            return value

    @staticmethod
    def clamp_value(
        value: Decimal,
        min_value: Decimal | None = None,
        max_value: Decimal | None = None,
    ) -> Decimal:
        """Clamp a value between min and max bounds.

        Args:
            value: The value to clamp
            min_value: Minimum allowed value
            max_value: Maximum allowed value

        Returns:
            Clamped value
        """
        if min_value is not None and value < min_value:
            return min_value
        if max_value is not None and value > max_value:
            return max_value
        return value

    # Enhanced Core Methods with Better Error Handling
    @staticmethod
    def parse_decimal_safely_enhanced(
        value: str | float | Decimal | None,
        default: Decimal = Decimal(0),
        field_name: str = "unknown_field",
    ) -> Decimal:
        """Enhanced decimal parsing with better error handling and logging.

        Args:
            value: The value to parse as a decimal
            default: Default value to return if parsing fails
            field_name: Name of the field for enhanced logging

        Returns:
            Parsed decimal value or default
        """
        if value is None:
            logger.debug(
                "decimal_parsing_null_value",
                field_name=field_name,
                default=str(default),
                message=f"Using default value for null field '{field_name}'",
            )
            return default

        if isinstance(value, Decimal):
            return value

        try:
            parsed_value = parse_decimal_value(value=value, allow_none=True, field_name=field_name)
        except (ValueError, TypeError, InvalidOperation, AttributeError) as e:
            logger.warning(
                "decimal_parsing_failed",
                field_name=field_name,
                value=value,
                default=str(default),
                error=str(e),
                message=f"Failed to parse decimal value for field '{field_name}', using default",
            )
            return default
        else:
            if parsed_value is not None:
                return parsed_value
            # If we get here, parsing succeeded but returned None, so use default
            logger.debug(
                "decimal_parsing_fallback",
                field_name=field_name,
                value=value,
                default=str(default),
                message=f"Using default value for unparseable field '{field_name}'",
            )
            return default

    # Status and State Conversion Utilities
    @staticmethod
    def map_order_status(raw_status: str) -> str:
        """Map Hyperliquid order status to internal format.

        Args:
            raw_status: Raw status from Hyperliquid API

        Returns:
            Mapped internal status
        """
        # Hyperliquid status mapping
        status_map = {
            "open": "OPEN",
            "filled": "FILLED",
            "cancelled": "CANCELLED",
            "rejected": "REJECTED",
            "triggered": "TRIGGERED",
            "untriggered": "PENDING",
            "partial": "PARTIALLY_FILLED",
            "partially_filled": "PARTIALLY_FILLED",
        }

        return status_map.get(raw_status.lower(), raw_status.upper())

    @staticmethod
    def map_order_side(raw_side: str) -> str:
        """Map Hyperliquid order side to internal format.

        Args:
            raw_side: Raw side from API (e.g., "A", "B", "buy", "sell")

        Returns:
            Mapped internal side ("BUY" or "SELL")
        """
        # Hyperliquid uses "A" for Ask (sell) and "B" for Bid (buy)
        side_map = {
            "A": "SELL",
            "B": "BUY",
            "ask": "SELL",
            "bid": "BUY",
            "sell": "SELL",
            "buy": "BUY",
        }

        return side_map.get(raw_side, raw_side.upper())

    @staticmethod
    def map_order_type(raw_type: str) -> str:
        """Map Hyperliquid order type to internal format.

        Args:
            raw_type: Raw type from API

        Returns:
            Mapped internal type
        """
        type_map = {
            "limit": "LIMIT",
            "market": "MARKET",
            "stop": "STOP",
            "stop_limit": "STOP_LIMIT",
            "trigger": "STOP",
            "triggered_tp": "TAKE_PROFIT",
            "triggered_sl": "STOP_LOSS",
        }

        return type_map.get(raw_type.lower(), raw_type.upper())

    @staticmethod
    def parse_leverage(leverage_value: str | float | Decimal | None) -> Decimal:
        """Parse leverage value with proper validation.

        Args:
            leverage_value: Raw leverage value

        Returns:
            Parsed leverage as Decimal (minimum 1)
        """
        try:
            leverage = HyperliquidCommonMappers.parse_decimal_safely(
                leverage_value,
                default=Decimal(1),
            )
            # Leverage should be at least 1
            return max(leverage, Decimal(1))
        except (ValueError, TypeError, InvalidOperation, AttributeError):
            return Decimal(1)
