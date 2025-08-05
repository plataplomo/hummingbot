"""Portfolio service specific exceptions.

These exceptions handle portfolio service errors that don't require
API error handling capabilities. They inherit from ConfigurationError
to avoid circular dependencies with the API layer.
"""

from cyberdelta.exceptions.base import ConfigurationError


class PortfolioError(ConfigurationError):
    """Base class for portfolio service specific errors."""


class PortfolioNotInitializedError(PortfolioError):
    """Raised when portfolio service operations are attempted before initialization."""

    def __init__(self) -> None:
        super().__init__("Portfolio service not initialized")


class PortfolioStateError(PortfolioError):
    """Raised when portfolio state is invalid or corrupted."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class PortfolioStateNotInitializedError(PortfolioStateError):
    """Raised when portfolio state operations are attempted before state is initialized."""

    def __init__(self) -> None:
        super().__init__("Portfolio state not initialized")


class ExchangeNotSupportedError(PortfolioError):
    """Raised when an unsupported exchange is encountered."""

    def __init__(self, exchange: str) -> None:
        super().__init__(f"API client for {exchange} does not support balance fetching")


class ReconciliationError(PortfolioError):
    """Raised when portfolio reconciliation fails."""

    def __init__(self, exchange_count: int) -> None:
        super().__init__(f"Portfolio reconciliation failed for all {exchange_count} exchanges")


class InvalidPositionDataError(PortfolioError):
    """Raised when position data is invalid or incomplete."""

    def __init__(self, message: str) -> None:
        super().__init__(f"Invalid position data: {message}")


class MissingEntryPriceError(InvalidPositionDataError):
    """Raised when a position is missing its entry price."""

    def __init__(self, position_key: str) -> None:
        super().__init__(f"Position {position_key} has no entry price - cannot calculate PnL")
