"""Base protocol definitions for Hyperliquid API.

This module defines the fundamental protocol interfaces that all Hyperliquid
components must implement. Following the Backpack pattern, these protocols
provide type safety, runtime validation, and consistent interfaces.
"""

from datetime import datetime
from decimal import Decimal
from typing import Protocol, runtime_checkable


@runtime_checkable
class MapperProtocol(Protocol):
    """Base protocol for all mappers.

    This protocol defines common utility methods that all mappers must implement,
    following the exact Backpack pattern. These methods provide consistent
    data transformation utilities across all mapper implementations.
    """

    @staticmethod
    def parse_decimal_safely(
        value: str | float | Decimal | None, default: Decimal = Decimal(0)
    ) -> Decimal:
        """Parse decimal values safely with default fallback.

        Args:
            value: The value to parse as a decimal
            default: Default value to return if parsing fails

        Returns:
            Parsed decimal value or default
        """
        ...

    @staticmethod
    def normalize_symbol(symbol: str) -> str:
        """Normalize symbol to internal format.

        Args:
            symbol: The symbol to normalize

        Returns:
            Normalized symbol string
        """
        ...

    @staticmethod
    def denormalize_symbol(symbol: str) -> str:
        """Denormalize symbol to exchange format.

        Args:
            symbol: The symbol to denormalize

        Returns:
            Denormalized symbol string
        """
        ...

    @staticmethod
    def timestamp_ms_to_datetime(timestamp_ms: float | None) -> datetime | None:
        """Convert millisecond timestamp to datetime.

        Args:
            timestamp_ms: Millisecond timestamp

        Returns:
            Datetime object or None if timestamp is None
        """
        ...


@runtime_checkable
class RequestBuilderProtocol(Protocol):
    """Base protocol for all request builders.

    This protocol defines the fundamental interface for building API requests.
    All request builders must implement this protocol to ensure consistency
    and type safety across the system.
    """

    def build_request(self, *args: object, **kwargs: object) -> dict[str, object]:
        """Build request payload.

        Args:
            *args: Positional arguments for request building
            **kwargs: Keyword arguments for request building

        Returns:
            Dictionary containing the request payload
        """
        ...


@runtime_checkable
class ResponseHandlerProtocol(Protocol):
    """Base protocol for all response handlers.

    This protocol defines the fundamental interface for handling API responses.
    All response handlers must implement this protocol to ensure consistent
    response processing across the system.
    """

    def handle_response(
        self, response: dict[str, object], status_code: int, headers: dict[str, str], context: str
    ) -> object:
        """Handle API response.

        Args:
            response: Raw response data from API
            status_code: HTTP status code
            headers: Response headers
            context: Context information about the request

        Returns:
            Processed response object
        """
        ...
