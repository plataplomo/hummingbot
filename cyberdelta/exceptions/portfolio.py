"""Portfolio service specific exceptions.

These exceptions handle portfolio service errors that don't require
API error handling capabilities. They inherit from ConfigurationError
to avoid circular dependencies with the API layer.
"""

from cyberdelta.enums import ExchangeName
from cyberdelta.exceptions.base import ConfigurationError


class PortfolioError(ConfigurationError):
    """Base class for portfolio service specific errors."""


class PortfolioNotInitializedError(PortfolioError):
    """Raised when portfolio service operations are attempted before initialization."""

    def __init__(self) -> None:
        """Initialize with default error message."""
        super().__init__("Portfolio service not initialized")


class PortfolioStateError(PortfolioError):
    """Raised when portfolio state is invalid or corrupted."""

    def __init__(self, message: str) -> None:
        """Initialize with custom error message."""
        super().__init__(message)


class PortfolioStateNotInitializedError(PortfolioStateError):
    """Raised when portfolio state operations are attempted before state is initialized."""

    def __init__(self) -> None:
        """Initialize with default error message."""
        super().__init__("Portfolio state not initialized")


class ExchangeNotSupportedError(PortfolioError):
    """Raised when an unsupported exchange is encountered."""

    def __init__(self, exchange: ExchangeName) -> None:
        """Initialize with exchange enum."""
        super().__init__(f"API client for {exchange.value} does not support balance fetching")


class ReconciliationError(PortfolioError):
    """Raised when portfolio reconciliation fails."""

    def __init__(self, exchange_count: int) -> None:
        """Initialize with failed exchange count."""
        super().__init__(f"Portfolio reconciliation failed for all {exchange_count} exchanges")


class InvalidPositionDataError(PortfolioError):
    """Raised when position data is invalid or incomplete."""

    def __init__(self, message: str) -> None:
        """Initialize with position error details."""
        super().__init__(f"Invalid position data: {message}")


class MissingEntryPriceError(InvalidPositionDataError):
    """Raised when a position is missing its entry price."""

    def __init__(self, position_key: str) -> None:
        """Initialize with position key that lacks entry price."""
        super().__init__(f"Position {position_key} has no entry price - cannot calculate PnL")


class MissingClosePriceError(InvalidPositionDataError):
    """Raised when a position close event is missing close price."""

    def __init__(self, symbol: str) -> None:
        """Initialize with symbol missing close price."""
        super().__init__(
            f"Position close event for {symbol} missing close_price - "
            "cannot calculate accurate realized PnL"
        )


class MissingRealizedPnLError(InvalidPositionDataError):
    """Raised when a position close event is missing realized PnL."""

    def __init__(self, symbol: str) -> None:
        """Initialize with symbol missing realized PnL."""
        super().__init__(
            f"Position close event for {symbol} missing realized_pnl - "
            "cannot update position without accurate PnL data"
        )


class MissingBalanceDataError(PortfolioError):
    """Raised when balance event is missing required data."""

    def __init__(self, symbol: str, missing_field: str) -> None:
        """Initialize with symbol and missing field."""
        super().__init__(
            f"Balance event for {symbol} missing {missing_field} - cannot update balance"
        )


class StorageError(Exception):
    """Exception raised for storage operation failures."""

    def __init__(
        self, message: str, operation: str, original_error: Exception | None = None
    ) -> None:
        """Initialize storage error.

        Args:
            message: Human-readable error description
            operation: Storage operation that failed (save, load, delete, etc.)
            original_error: The underlying exception that caused the failure
        """
        self.message = message
        self.operation = operation
        self.original_error = original_error

        formatted_message = f"Storage operation '{operation}' failed: {message}"
        if original_error:
            formatted_message += f" (caused by: {original_error})"

        super().__init__(formatted_message)
