"""Market data service exceptions for CyberDelta.

These exceptions handle errors specific to market data service operations,
including parameter validation and data availability.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cyberdelta.apis.common.api_error import APIError
from cyberdelta.apis.common.api_error_codes import APIErrorCode


if TYPE_CHECKING:
    from cyberdelta.enums import ExchangeName


class MarketDataServiceError(APIError):
    """Base class for market data service errors."""

    def __init__(
        self,
        message: str,
        *,
        code: int | str | None = None,
        service_method: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Initialize market data service error.

        Args:
            message: Human-readable error description
            code: Error code (defaults to INVALID_REQUEST)
            service_method: Name of the service method where error occurred
            metadata: Additional error context
        """
        if code is None:
            code = APIErrorCode.INVALID_REQUEST.value

        super().__init__(
            message=message,
            code=code,
            metadata={
                "service_method": service_method,
                **(metadata or {}),
            },
        )
        self.service_method = service_method


class EmptySymbolError(MarketDataServiceError):
    """Raised when a required symbol parameter is empty or None."""

    def __init__(self, service_method: str) -> None:
        """Initialize empty symbol error.

        Args:
            service_method: The service method that requires a symbol
        """
        super().__init__(
            message=f"[{service_method}] 'symbol' must be a non-empty string.",
            service_method=service_method,
        )


class InvalidLimitError(MarketDataServiceError):
    """Raised when limit parameter is invalid (e.g., non-positive)."""

    def __init__(self, service_method: str, limit: int | None = None) -> None:
        """Initialize invalid limit error.

        Args:
            service_method: The service method with invalid limit
            limit: The invalid limit value
        """
        message = f"[{service_method}] 'limit' must be positive when provided."
        if limit is not None:
            message = f"[{service_method}] 'limit' must be positive when provided, got {limit}."

        super().__init__(
            message=message,
            service_method=service_method,
            metadata={"limit": limit},
        )


class InvalidTimeRangeError(MarketDataServiceError):
    """Raised when time range parameters are invalid."""

    def __init__(
        self,
        service_method: str,
        field_name: str,
        value: object = None,
        reason: str | None = None,
    ) -> None:
        """Initialize invalid time range error.

        Args:
            service_method: The service method with invalid time range
            field_name: Name of the time field (e.g., 'start_time', 'end_time')
            value: The invalid time value
            reason: Specific reason for invalidity
        """
        if reason:
            message = f"[{service_method}] '{field_name}' {reason}"
        else:
            message = f"[{service_method}] '{field_name}' must be positive when provided."

        super().__init__(
            message=message,
            service_method=service_method,
            metadata={"field_name": field_name, "value": value},
        )


class EmptySymbolListError(MarketDataServiceError):
    """Raised when a required symbol list is empty."""

    def __init__(self, service_method: str) -> None:
        """Initialize empty symbol list error.

        Args:
            service_method: The service method that requires symbols
        """
        super().__init__(
            message=f"[{service_method}] At least one symbol is required for Backpack.",
            service_method=service_method,
        )


class EmptySymbolInListError(MarketDataServiceError):
    """Raised when a symbol in a list is empty."""

    def __init__(self, service_method: str, index: int | None = None) -> None:
        """Initialize empty symbol in list error.

        Args:
            service_method: The service method with empty symbol in list
            index: Index of the empty symbol in the list
        """
        message = f"[{service_method}] All symbols in list must be non-empty strings."
        if index is not None:
            message = f"[{service_method}] Symbol at index {index} must be a non-empty string."

        super().__init__(
            message=message,
            service_method=service_method,
            metadata={"index": index} if index is not None else {},
        )


class NullSymbolsError(MarketDataServiceError):
    """Raised when symbols parameter is None when it shouldn't be."""

    def __init__(self, service_method: str) -> None:
        """Initialize null symbols error.

        Args:
            service_method: The service method that got None symbols
        """
        super().__init__(
            message=f"[{service_method}] symbols cannot be None",
            service_method=service_method,
        )


class UnsupportedIntervalError(MarketDataServiceError):
    """Raised when an unsupported time interval/timeframe is provided."""

    def __init__(
        self,
        service_method: str,
        interval: str,
        supported_intervals: list[str] | None = None,
    ) -> None:
        """Initialize unsupported interval error.

        Args:
            service_method: The service method with unsupported interval
            interval: The unsupported interval value
            supported_intervals: List of supported intervals
        """
        message = f"[{service_method}] Unsupported interval '{interval}'."
        if supported_intervals:
            message = (
                f"[{service_method}] Unsupported interval '{interval}'. "
                f"Supported intervals: {', '.join(supported_intervals)}"
            )

        super().__init__(
            message=message,
            service_method=service_method,
            metadata={
                "interval": interval,
                "supported_intervals": supported_intervals,
            },
        )


class NoFundingDataError(APIError):
    """Raised when no funding rate data is available for a symbol."""

    def __init__(self, symbol: str) -> None:
        """Initialize no funding data error.

        Args:
            symbol: The symbol with no funding data
        """
        super().__init__(
            message=f"No funding rate data available for {symbol}",
            code=APIErrorCode.FUNDING_RATE_UNAVAILABLE.value,
            metadata={"symbol": symbol},
        )


class SymbolNotFoundError(MarketDataServiceError):
    """Raised when a requested symbol is not found in available symbols."""

    def __init__(
        self,
        symbol: str,
        available_symbols: list[str] | None = None,
        exchange: ExchangeName | None = None,
        service_method: str | None = None,
    ) -> None:
        """Initialize symbol not found error.

        Args:
            symbol: The symbol that was not found
            available_symbols: List of available symbols
            exchange: Exchange enum value where symbol lookup failed
            service_method: Service method where error occurred
        """
        prefix = f"[{exchange.value}] " if exchange else ""
        message = f"{prefix}Symbol '{symbol}' not found"

        if available_symbols:
            max_displayed_symbols = 10
            displayed_symbols = available_symbols[:max_displayed_symbols]
            # Convert Symbol objects to strings for display
            symbol_strings = [str(sym) for sym in displayed_symbols]
            message += f". Available symbols: {', '.join(symbol_strings)}"
            if len(available_symbols) > max_displayed_symbols:
                remaining_count = len(available_symbols) - max_displayed_symbols
                message += f" (and {remaining_count} more)"

        super().__init__(
            message=message,
            code=APIErrorCode.SYMBOL_NOT_FOUND.value,
            service_method=service_method,
            metadata={
                "symbol": symbol,
                "available_symbols": available_symbols,
                "exchange": exchange.value if exchange else None,
            },
        )
        self.symbol = symbol
        self.available_symbols = available_symbols
        self.exchange = exchange


class NotImplementedServiceError(APIError):
    """Raised when a service method is not implemented."""

    def __init__(self, service_name: str, method_name: str) -> None:
        """Initialize not implemented service error.

        Args:
            service_name: Name of the service class
            method_name: Name of the unimplemented method
        """
        super().__init__(
            message=f"{method_name} is not implemented for {service_name}",
            code=APIErrorCode.INVALID_REQUEST.value,
            metadata={
                "service": service_name,
                "method": method_name,
            },
        )
