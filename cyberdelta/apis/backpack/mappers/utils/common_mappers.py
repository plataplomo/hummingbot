"""Common mapping utilities shared across all Backpack mappers.

This module provides reusable utility functions for data transformation,
validation, and normalization that are commonly needed across different
Backpack mapper implementations.
"""

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import TypeGuard, TypeVar

from cyberdelta.apis.exceptions.field_validation import FieldError
from cyberdelta.config.structlog_config import get_logger


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


class BackpackCommonMappers:
    """Shared utilities for all Backpack mappers."""

    # Constants
    EXPECTED_SYMBOL_PARTS = 2
    EXPECTED_PERP_SYMBOL_PARTS = 3

    @staticmethod
    def parse_decimal_safely(
        value: str | float | Decimal | None, default: Decimal = Decimal(0)
    ) -> Decimal:
        """Safely parse decimal values with fallback.

        Args:
            value: The value to parse as Decimal
            default: Default value if parsing fails

        Returns:
            Parsed Decimal value or default
        """
        try:
            if value is None:
                return default
            if isinstance(value, Decimal):
                return value
            return Decimal(str(value))
        except (ValueError, TypeError, InvalidOperation) as e:
            logger.warning(
                "decimal_parse_failed",
                value=value,
                value_type=type(value).__name__,
                error=str(e),
                message="Failed to parse decimal value, using default",
            )
            return default

    @staticmethod
    def normalize_symbol(symbol: str) -> str:
        """Normalize symbol format for Backpack API.

        Backpack uses underscore-separated symbols (e.g., BTC_USDC)
        while some internal formats use slash-separated (e.g., BTC/USDC).

        Args:
            symbol: The symbol to normalize

        Returns:
            Normalized symbol in Backpack format
        """
        if not symbol:
            return symbol
        # Convert to uppercase and replace common separators
        return symbol.upper().replace("/", "_").replace("-", "_")

    @staticmethod
    def denormalize_symbol(symbol: str) -> str:
        """Convert Backpack symbol format to internal format.

        Converts underscore-separated symbols to slash-separated format.

        Args:
            symbol: Backpack format symbol (e.g., BTC_USDC or BTC_USDC_PERP)

        Returns:
            Internal format symbol (e.g., BTC/USDC or BTC/USDC/PERP)
        """
        if not symbol or "_" not in symbol:
            return symbol

        # Handle PERP symbols specially
        if symbol.endswith("_PERP"):
            # For PERP symbols, keep the PERP suffix as is
            parts = symbol.split("_")
            if len(parts) == BackpackCommonMappers.EXPECTED_PERP_SYMBOL_PARTS:
                return f"{parts[0]}/{parts[1]}/{parts[2]}"

        # Replace underscore with slash for internal format
        return symbol.replace("_", "/")

    @staticmethod
    def timestamp_ms_to_datetime(timestamp_ms: float | None) -> datetime | None:
        """Convert millisecond timestamp to timezone-aware datetime.

        Args:
            timestamp_ms: Timestamp in milliseconds since epoch

        Returns:
            Timezone-aware datetime in UTC or None if invalid
        """
        if timestamp_ms is None:
            return None
        try:
            # Convert milliseconds to seconds
            timestamp_s = float(timestamp_ms) / 1000.0
            return datetime.fromtimestamp(timestamp_s, tz=UTC)
        except (ValueError, TypeError, OverflowError) as e:
            logger.warning(
                "timestamp_conversion_failed",
                timestamp_ms=timestamp_ms,
                error=str(e),
                message="Failed to convert timestamp to datetime",
            )
            return None

    @staticmethod
    def _is_dict_with_key(obj: object, key: str) -> TypeGuard[dict[str, object]]:
        """Type guard to check if object is a dict containing the given key."""
        return isinstance(obj, dict) and key in obj

    @staticmethod
    def safe_get_nested(data: dict[str, object], *keys: str, default: object = None) -> object:
        """Safely get nested dictionary values.

        Args:
            data: The dictionary to extract from
            *keys: Sequence of keys to traverse
            default: Default value if key path doesn't exist

        Returns:
            The value at the key path or default
        """
        current: object = data
        for key in keys:
            if BackpackCommonMappers._is_dict_with_key(current, key):
                current = current[key]
            else:
                return default
        return current

    @staticmethod
    def _is_list(obj: object) -> TypeGuard[list[object]]:
        """Type guard to check if object is a list."""
        return isinstance(obj, list)

    @staticmethod
    def ensure_list(
        value: object | None, default: list[object] | None = None
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
        if BackpackCommonMappers._is_list(value):
            return value
        return [value]

    @staticmethod
    def calculate_percentage(value: Decimal, total: Decimal, precision: int = 4) -> Decimal:
        """Calculate percentage with safe division.

        Args:
            value: The value to calculate percentage for
            total: The total value (denominator)
            precision: Decimal places for rounding

        Returns:
            Percentage as Decimal (e.g., 0.1234 for 12.34%)
        """
        if total == 0:
            return Decimal(0)
        try:
            return (value / total).quantize(Decimal(f"0.{'0' * precision}"))
        except (InvalidOperation, ZeroDivisionError, ValueError) as e:
            logger.warning(
                "percentage_calculation_failed",
                value=str(value),
                total=str(total),
                error=str(e),
                message="Failed to calculate percentage",
            )
            return Decimal(0)

    @staticmethod
    def validate_positive_decimal(value: Decimal, field_name: str) -> Decimal:
        """Validate that a decimal value is positive.

        Args:
            value: The decimal value to validate
            field_name: Name of the field for error messages

        Returns:
            The validated value

        Raises:
            ValueError: If value is not positive
        """
        if value <= 0:
            raise NonPositiveValueError(field_name, value)
        return value

    @staticmethod
    def format_order_id(exchange_order_id: str | None, client_order_id: str | None) -> str:
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

    @staticmethod
    def is_valid_symbol(symbol: str) -> bool:
        """Check if symbol format is valid for Backpack.

        Args:
            symbol: The symbol to validate

        Returns:
            True if symbol is valid
        """
        if not symbol:
            return False
        # Backpack symbols should contain underscore and be uppercase
        parts = symbol.split("_")

        # Accept both spot (BASE_QUOTE) and perp (BASE_QUOTE_PERP) formats
        is_spot = (
            len(parts) == BackpackCommonMappers.EXPECTED_SYMBOL_PARTS
            and all(part.isalnum() and part.isupper() for part in parts)
            and len(parts[0]) > 0
            and len(parts[1]) > 0
        )

        is_perp = (
            len(parts) == BackpackCommonMappers.EXPECTED_PERP_SYMBOL_PARTS
            and all(part.isalnum() and part.isupper() for part in parts)
            and len(parts[0]) > 0
            and len(parts[1]) > 0
            and parts[2] == "PERP"
        )

        return is_spot or is_perp
