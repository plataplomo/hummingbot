"""Funding rate exceptions for CyberDelta.

These exceptions handle funding rate data errors including source failures,
data availability issues, and multi-tier provider errors.
"""

__all__ = [
    "AllSourcesFailedError",
    "ArbitrageFieldError",
    "FundingError",
    "FundingRateSourceError",
    "NegativeLongPriceError",
    "NegativeShortPriceError",
    "NegativeSizeError",
    "NoFallbackSourceError",
    "NoFundingDataError",
    "NoValidWeightedDataError",
    "NullTimestampError",
]


class FundingError(Exception):
    """Base exception for all funding-related errors."""


# Alias for backward compatibility
FundingRateSourceError = FundingError


class NoFundingDataError(FundingError):
    """Raised when no funding rate data is available for a symbol."""

    def __init__(self, exchange: str, symbol: str) -> None:
        """Initialize no funding data error.

        Args:
            exchange: Exchange identifier
            symbol: Trading symbol
        """
        self.exchange = exchange
        self.symbol = symbol
        super().__init__(f"No funding rate data available for {exchange}:{symbol}")


class NoFallbackSourceError(FundingError):
    """Raised when no fallback funding source is registered for an exchange."""

    def __init__(self, exchange: str) -> None:
        """Initialize no fallback source error.

        Args:
            exchange: Exchange identifier
        """
        self.exchange = exchange
        super().__init__(f"No fallback source registered for {exchange}")


class AllSourcesFailedError(FundingError):
    """Raised when all funding rate sources fail for a symbol."""

    def __init__(self, exchange: str, symbol: str, original_error: Exception | None = None) -> None:
        """Initialize all sources failed error.

        Args:
            exchange: Exchange identifier
            symbol: Trading symbol
            original_error: The underlying exception that caused the failure
        """
        self.exchange = exchange
        self.symbol = symbol
        self.original_error = original_error
        super().__init__(f"All sources failed for {exchange}:{symbol}")


class NoValidWeightedDataError(FundingError):
    """Raised when no valid weighted funding rate data is available."""

    def __init__(self, exchange: str, symbol: str) -> None:
        """Initialize no valid weighted data error.

        Args:
            exchange: Exchange identifier
            symbol: Trading symbol
        """
        self.exchange = exchange
        self.symbol = symbol
        super().__init__(f"No valid, weighted funding rate data available for {exchange}:{symbol}")


class ArbitrageFieldError(ValueError, FundingError):
    """Base class for arbitrage opportunity field validation errors."""

    def __init__(self, field_name: str, reason: str, value: object = None) -> None:
        """Initialize arbitrage field error.

        Args:
            field_name: Name of the field that failed validation
            reason: Reason for validation failure
            value: The invalid value
        """
        self.field_name = field_name
        self.reason = reason
        self.value = value
        super().__init__(f"{reason}")


class NullTimestampError(ArbitrageFieldError):
    """Raised when timestamp field is null/None."""

    def __init__(self) -> None:
        """Initialize null timestamp error."""
        super().__init__(field_name="timestamp", reason="timestamp cannot be None")


class NegativeLongPriceError(ArbitrageFieldError):
    """Raised when long price has a non-positive value."""

    def __init__(self, value: float | None = None) -> None:
        """Initialize negative long price error.

        Args:
            value: The invalid price value
        """
        super().__init__(
            field_name="long_price",
            reason="Long price must be positive.",
            value=value,
        )


class NegativeShortPriceError(ArbitrageFieldError):
    """Raised when short price has a non-positive value."""

    def __init__(self, value: float | None = None) -> None:
        """Initialize negative short price error.

        Args:
            value: The invalid price value
        """
        super().__init__(
            field_name="short_price",
            reason="Short price must be positive.",
            value=value,
        )


class NegativeSizeError(ArbitrageFieldError):
    """Raised when optimal size is non-positive."""

    def __init__(self, value: float | None = None) -> None:
        """Initialize negative size error.

        Args:
            value: The invalid size value
        """
        super().__init__(
            field_name="optimal_size",
            reason="Optimal size must be positive if present.",
            value=value,
        )
