"""Common base protocol definitions for all API components.

These protocols define the expected interfaces for different component types
across all exchanges, providing type safety and documentation consistency.
"""

from datetime import datetime
from decimal import Decimal
from typing import Protocol, runtime_checkable

from cyberdelta.utils.typing import ParsedJsonResponse


__all__ = [
    "MapperProtocol",
    "RequestBuilderProtocol",
    "ResponseHandlerProtocol",
]


@runtime_checkable
class MapperProtocol(Protocol):
    """Base protocol for all mapper components.

    Mappers are responsible for transforming data between external API formats
    and internal domain models. This protocol provides common utility methods
    that all mappers must implement across all exchanges.
    """

    def parse_decimal_safely(
        self, value: str | float | Decimal | None, default: Decimal = Decimal(0)
    ) -> Decimal:
        """Safely parse decimal values with fallback.

        Args:
            value: The value to parse as a decimal
            default: Default value to return if parsing fails

        Returns:
            Parsed decimal value or default
        """
        ...

    def timestamp_ms_to_datetime(self, timestamp_ms: float | None) -> datetime | None:
        """Convert millisecond timestamp to datetime.

        Args:
            timestamp_ms: Millisecond timestamp

        Returns:
            Datetime object or None if timestamp is None
        """
        ...


@runtime_checkable
class RequestBuilderProtocol(Protocol):
    """Base protocol for all request builder components.

    Request builders construct properly formatted API requests including
    parameters, headers, and payloads for any exchange API.
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
    """Base protocol for all response handler components.

    Response handlers process raw API responses, perform validation,
    and convert them to appropriate raw model types for any exchange.
    """

    def handle_response(
        self, response: ParsedJsonResponse, status_code: int, headers: dict[str, str], context: str
    ) -> object:
        """Handle API response.

        Args:
            response: Raw response data from API (dict, list, or string)
            status_code: HTTP status code
            headers: Response headers
            context: Context information about the request

        Returns:
            Processed response object
        """
        ...
