"""Market Order Error Hierarchy.

This module defines custom exceptions for market order execution failures.
"""

from decimal import Decimal


class MarketOrderError(Exception):
    """Base exception for market order execution errors."""

    pass


class InsufficientLiquidityError(MarketOrderError):
    """Raised when there's not enough liquidity in the order book.

    Attributes:
        symbol: Trading symbol
        requested_quantity: Requested order quantity
        available_quantity: Available liquidity in the order book
    """

    def __init__(
        self,
        symbol: str,
        requested_quantity: Decimal,
        available_quantity: Decimal,
        message: str | None = None,
    ) -> None:
        """Initialize InsufficientLiquidityError.

        Args:
            symbol: Trading symbol
            requested_quantity: Requested order quantity
            available_quantity: Available liquidity
            message: Optional custom error message
        """
        self.symbol = symbol
        self.requested_quantity = requested_quantity
        self.available_quantity = available_quantity

        if message is None:
            message = (
                f"Insufficient liquidity for {symbol}: "
                f"requested {requested_quantity}, available {available_quantity}"
            )

        super().__init__(message)


class PriceDeviationError(MarketOrderError):
    """Raised when aggressive price deviates too far from reference.

    Attributes:
        symbol: Trading symbol
        aggressive_price: Calculated aggressive price
        reference_price: Reference price (best bid/ask)
        deviation_pct: Actual deviation percentage
        max_deviation_pct: Maximum allowed deviation
    """

    def __init__(
        self,
        symbol: str,
        aggressive_price: Decimal,
        reference_price: Decimal,
        deviation_pct: Decimal,
        max_deviation_pct: Decimal,
        message: str | None = None,
    ) -> None:
        """Initialize PriceDeviationError.

        Args:
            symbol: Trading symbol
            aggressive_price: Calculated aggressive price
            reference_price: Reference price
            deviation_pct: Actual deviation percentage
            max_deviation_pct: Maximum allowed deviation
            message: Optional custom error message
        """
        self.symbol = symbol
        self.aggressive_price = aggressive_price
        self.reference_price = reference_price
        self.deviation_pct = deviation_pct
        self.max_deviation_pct = max_deviation_pct

        if message is None:
            message = (
                f"Price deviation for {symbol} exceeds limit: "
                f"{deviation_pct:.2%} > {max_deviation_pct:.2%} "
                f"(aggressive: {aggressive_price}, reference: {reference_price})"
            )

        super().__init__(message)
