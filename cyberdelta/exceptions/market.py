"""Market data exceptions for CyberDelta.

Domain-specific exceptions for market data processing that follow
the principle of fail-fast without fallbacks.
"""

from typing import Any


class MarketDataError(ValueError):
    """Base class for market data errors."""

    def __init__(
        self,
        message: str,
        *,
        symbol: str | None = None,
        data_type: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Initialize market data error.

        Args:
            message: Human-readable error description
            symbol: Symbol that caused the error
            data_type: Type of market data (tick, quote, trade, orderbook)
            metadata: Additional error context
        """
        super().__init__(message)
        self.symbol = symbol
        self.data_type = data_type
        self.metadata = metadata or {}


class MarketDataMissingPriceError(MarketDataError):
    """Raised when market data is missing required price information."""

    def __init__(self, symbol: str, data_type: str) -> None:
        """Initialize missing price error.

        Args:
            symbol: Symbol with missing price
            data_type: Type of data missing price
        """
        message = f"{data_type.capitalize()} data for {symbol} missing price"
        super().__init__(message, symbol=symbol, data_type=data_type)


class MissingQuoteError(MarketDataError):
    """Raised when quote data is missing bid or ask."""

    def __init__(self, symbol: str, missing_field: str) -> None:
        """Initialize missing quote error.

        Args:
            symbol: Symbol with missing quote data
            missing_field: Which field is missing (bid/ask)
        """
        message = f"Quote data for {symbol} missing {missing_field}"
        super().__init__(message, symbol=symbol, data_type="quote")


class MissingVolumeError(MarketDataError):
    """Raised when trade data is missing volume."""

    def __init__(self, symbol: str) -> None:
        """Initialize missing volume error.

        Args:
            symbol: Symbol with missing volume
        """
        message = f"Trade data for {symbol} missing volume"
        super().__init__(message, symbol=symbol, data_type="trade")


class MissingOrderbookError(MarketDataError):
    """Raised when orderbook data is missing bids or asks."""

    def __init__(self, symbol: str, missing_field: str) -> None:
        """Initialize missing orderbook error.

        Args:
            symbol: Symbol with missing orderbook data
            missing_field: Which field is missing (bids/asks)
        """
        message = f"Orderbook data for {symbol} missing {missing_field}"
        super().__init__(message, symbol=symbol, data_type="orderbook")
