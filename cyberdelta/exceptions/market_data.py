"""Market data-related exceptions for CyberDelta.

These exceptions handle market data errors including missing data,
invalid symbols, and unavailable funding rates.
"""

from typing import Any

from cyberdelta.apis.common import APIError, APIErrorCode


class MarketDataError(APIError):
    """Base class for market data-related errors."""

    def __init__(
        self,
        message: str,
        *,
        code: int | str | None = None,
        http_status: int | None = None,
        exchange_code: str | int | None = None,
        exchange_message: str | None = None,
        retry_after: float | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        """Initialize market data error.

        Args:
            message: Human-readable error description
            code: Error code (defaults to UNKNOWN if not provided)
            http_status: HTTP status code
            exchange_code: Exchange-specific error code
            exchange_message: Exchange-specific error message
            retry_after: Seconds to wait before retry
            metadata: Additional error context
            original_exception: The underlying exception
        """
        super().__init__(
            message=message,
            code=code or APIErrorCode.UNKNOWN.value,
            http_status=http_status,
            exchange_code=exchange_code,
            exchange_message=exchange_message,
            retry_after=retry_after,
            metadata=metadata,
            original_exception=original_exception,
        )


class SymbolNotFoundError(MarketDataError):
    """Raised when a trading symbol is not found or invalid."""

    def __init__(
        self,
        symbol: str,
        exchange: str | None = None,
        available_symbols: list[str] | None = None,
    ) -> None:
        """Initialize symbol not found error.

        Args:
            symbol: The invalid symbol
            exchange: Optional exchange name
            available_symbols: Optional list of valid symbols
        """
        self.symbol = symbol
        self.exchange = exchange
        self.available_symbols = available_symbols

        message = f"Symbol '{symbol}' not found"
        if exchange:
            message = f"{message} on {exchange}"

        super().__init__(
            message=message,
            code=APIErrorCode.INVALID_SYMBOL.value,
            exchange_code="SYMBOL_NOT_FOUND",
            metadata={
                "symbol": symbol,
                "exchange": exchange,
                "available_symbols": available_symbols[:10] if available_symbols else None,
            },
        )


class DataUnavailableError(MarketDataError):
    """Raised when requested market data is unavailable."""

    def __init__(
        self,
        data_type: str,
        symbol: str | None = None,
        reason: str | None = None,
        retry_after: float | None = None,
    ) -> None:
        """Initialize data unavailable error.

        Args:
            data_type: Type of data requested
            symbol: Optional trading symbol
            reason: Optional reason for unavailability
            retry_after: Optional seconds to wait before retry
        """
        self.data_type = data_type
        self.symbol = symbol
        self.reason = reason

        message = f"{data_type} data unavailable"
        if symbol:
            message = f"{message} for {symbol}"
        if reason:
            message = f"{message}: {reason}"

        super().__init__(
            message=message,
            code=APIErrorCode.UNKNOWN.value,  # No specific data unavailable code
            retry_after=retry_after,
            metadata={
                "data_type": data_type,
                "symbol": symbol,
                "reason": reason,
            },
        )


class FundingRateUnavailableError(MarketDataError):
    """Raised when funding rate data is unavailable."""

    def __init__(
        self,
        symbol: str,
        exchange: str | None = None,
        timestamp: str | None = None,
        reason: str | None = None,
    ) -> None:
        """Initialize funding rate unavailable error.

        Args:
            symbol: Trading symbol
            exchange: Optional exchange name
            timestamp: Optional timestamp when requested
            reason: Optional reason for unavailability
        """
        self.symbol = symbol
        self.exchange = exchange
        self.timestamp = timestamp
        self.reason = reason

        message = f"Funding rate unavailable for {symbol}"
        if exchange:
            message = f"{message} on {exchange}"
        if timestamp:
            message = f"{message} at {timestamp}"
        if reason:
            message = f"{message}: {reason}"

        super().__init__(
            message=message,
            code=APIErrorCode.FUNDING_RATE_UNAVAILABLE.value,
            metadata={
                "symbol": symbol,
                "exchange": exchange,
                "timestamp": timestamp,
                "reason": reason,
            },
        )


class OrderBookError(MarketDataError):
    """Raised when order book data has issues."""

    def __init__(
        self,
        symbol: str,
        issue: str,
        exchange: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize order book error.

        Args:
            symbol: Trading symbol
            issue: Description of the issue
            exchange: Optional exchange name
            details: Optional additional details
        """
        self.symbol = symbol
        self.issue = issue
        self.exchange = exchange
        self.details = details

        message = f"Order book error for {symbol}: {issue}"
        if exchange:
            message = f"{message} on {exchange}"

        metadata = {
            "symbol": symbol,
            "issue": issue,
            "exchange": exchange,
        }
        if details:
            metadata.update(details)

        super().__init__(
            message=message,
            code=APIErrorCode.UNKNOWN.value,  # No specific orderbook error code
            metadata=metadata,
        )


class TickerError(MarketDataError):
    """Raised when ticker data has issues."""

    def __init__(
        self,
        symbol: str,
        field: str | None = None,
        value: object = None,
        reason: str | None = None,
    ) -> None:
        """Initialize ticker error.

        Args:
            symbol: Trading symbol
            field: Optional field with issue
            value: Optional problematic value
            reason: Optional reason for error
        """
        self.symbol = symbol
        self.field = field
        self.value = value
        self.reason = reason

        if field:
            message = f"Invalid ticker data for {symbol}, field '{field}'"
            if value is not None:
                message = f"{message} has value {value}"
            if reason:
                message = f"{message}: {reason}"
        else:
            message = f"Ticker error for {symbol}"
            if reason:
                message = f"{message}: {reason}"

        super().__init__(
            message=message,
            code=APIErrorCode.UNKNOWN.value,  # No specific ticker error code
            metadata={
                "symbol": symbol,
                "field": field,
                "value": value,
                "reason": reason,
            },
        )
