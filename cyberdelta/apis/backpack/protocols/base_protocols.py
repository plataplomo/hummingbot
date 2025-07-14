"""Base protocol definitions for Backpack API components.

These protocols define the expected interfaces for different component types,
providing better type safety and documentation.
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
    """Protocol for all mapper components.

    Mappers are responsible for transforming data between external API formats
    and internal domain models.
    """

    @staticmethod
    def parse_decimal_safely(
        value: str | float | Decimal | None, default: Decimal = Decimal(0)
    ) -> Decimal:
        """Safely parse decimal values with fallback."""
        ...

    @staticmethod
    def normalize_symbol(symbol: str) -> str:
        """Convert symbol to Backpack format (underscore-separated)."""
        ...

    @staticmethod
    def denormalize_symbol(symbol: str) -> str:
        """Convert symbol from Backpack to internal format (slash-separated)."""
        ...

    @staticmethod
    def timestamp_ms_to_datetime(timestamp_ms: float | None) -> datetime | None:
        """Convert millisecond timestamp to UTC datetime."""
        ...


@runtime_checkable
class RequestBuilderProtocol(Protocol):
    """Protocol for all request builder components.

    Request builders construct properly formatted API requests including
    parameters, headers, and payloads.
    """

    def build_request(self, *args: object, **kwargs: object) -> dict[str, object]:
        """Generic request builder dispatch method."""
        ...


@runtime_checkable
class ResponseHandlerProtocol(Protocol):
    """Protocol for all response handler components.

    Response handlers process raw API responses, perform validation,
    and convert them to appropriate raw model types.
    """

    def handle_response(
        self, response: ParsedJsonResponse, status_code: int, headers: dict[str, str], context: str
    ) -> object:
        """Generic response handler dispatch method."""
        ...
