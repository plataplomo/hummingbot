"""Financial calculation and currency operation exceptions.

These exceptions handle errors in financial models and calculations,
ensuring proper error handling for monetary operations.
"""

from __future__ import annotations


class CurrencyMismatchError(ValueError):
    """Raised when operations between different currencies are attempted."""

    def __init__(self, currency1: str, currency2: str, operation: str) -> None:
        """Initialize currency mismatch error.

        Args:
            currency1: First currency in the operation
            currency2: Second currency in the operation
            operation: The operation that was attempted
        """
        self.currency1 = currency1
        self.currency2 = currency2
        self.operation = operation

        message = f"Cannot {operation} {currency1} and {currency2}"
        super().__init__(message)


class InvalidAmountError(ValueError):
    """Raised when an invalid amount value is provided."""

    def __init__(self, reason: str = "Invalid amount value") -> None:
        """Initialize invalid amount error.

        Args:
            reason: Specific reason why the amount is invalid
        """
        self.reason = reason
        super().__init__(reason)


class DivisionByZeroError(ValueError):
    """Raised when division by zero is attempted in financial calculations."""

    def __init__(self, context: str = "Financial calculation") -> None:
        """Initialize division by zero error.

        Args:
            context: Context where the division by zero occurred
        """
        self.context = context
        message = f"Division by zero in {context}"
        super().__init__(message)


class BalanceLockTimeoutError(RuntimeError):
    """Raised when balance lock acquisition times out."""

    def __init__(self, asset: str, exchange: str, timeout_seconds: float) -> None:
        """Initialize balance lock timeout error.

        Args:
            asset: Asset symbol that couldn't be locked
            exchange: Exchange where lock failed
            timeout_seconds: Timeout duration that was exceeded
        """
        self.asset = asset
        self.exchange = exchange
        self.timeout_seconds = timeout_seconds

        message = (
            f"Failed to acquire balance lock for {asset} on {exchange} within {timeout_seconds}s"
        )
        super().__init__(message)
