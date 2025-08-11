"""Trading and order-related exceptions for CyberDelta.

Domain-specific exceptions for trading operations, order processing,
and risk management that follow fail-fast principles.
"""

from typing import Any


class TradingError(ValueError):
    """Base class for trading-related errors."""

    def __init__(
        self,
        message: str,
        *,
        order_id: str | None = None,
        symbol: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Initialize trading error.

        Args:
            message: Human-readable error description
            order_id: Order ID that caused the error
            symbol: Symbol involved in the error
            metadata: Additional error context
        """
        super().__init__(message)
        self.order_id = order_id
        self.symbol = symbol
        self.metadata = metadata or {}


class OrderDataError(TradingError):
    """Raised when order data is missing or invalid."""

    def __init__(self, order_id: str, missing_field: str) -> None:
        """Initialize order data error.

        Args:
            order_id: ID of the order with missing data
            missing_field: Name of the missing field
        """
        message = f"Order {order_id} missing {missing_field}"
        super().__init__(message, order_id=order_id)


class PositionDataError(TradingError):
    """Raised when position data is missing or invalid."""

    def __init__(self, field_info: str) -> None:
        """Initialize position data error.

        Args:
            field_info: Information about the missing/invalid field
        """
        message = f"Position data error: {field_info}"
        super().__init__(message)


class SignalDataError(TradingError):
    """Raised when trading signal data is missing required fields."""

    def __init__(self, signal_id: str, missing_field: str) -> None:
        """Initialize signal data error.

        Args:
            signal_id: ID of the signal with missing data
            missing_field: Name of the missing field
        """
        message = f"Signal {signal_id} missing {missing_field}"
        super().__init__(message)


class RiskAssessmentError(TradingError):
    """Raised when risk assessment cannot be performed due to missing data."""

    def __init__(self, reason: str) -> None:
        """Initialize risk assessment error.

        Args:
            reason: Why risk assessment failed
        """
        message = f"Risk assessment failed: {reason}"
        super().__init__(message)


class BalanceLockError(TradingError):
    """Raised when balance lock operation has missing data."""

    def __init__(self, asset: str, missing_field: str) -> None:
        """Initialize balance lock error.

        Args:
            asset: Asset symbol with the lock error
            missing_field: Missing field name
        """
        message = f"Balance lock event for {asset} missing {missing_field}"
        super().__init__(message, symbol=asset)


class PositionNoneError(PositionDataError):
    """Raised when position is None when it shouldn't be."""

    def __init__(self) -> None:
        """Initialize position none error."""
        super().__init__("position is None")


class RealizedPnLNoneError(PositionDataError):
    """Raised when realized PnL is None when it shouldn't be."""

    def __init__(self) -> None:
        """Initialize realized PnL none error."""
        super().__init__("realized_pnl is None")


class TotalEquityNoneError(RiskAssessmentError):
    """Raised when total equity is None during risk assessment."""

    def __init__(self) -> None:
        """Initialize total equity none error."""
        super().__init__("total_equity_usd is None")
