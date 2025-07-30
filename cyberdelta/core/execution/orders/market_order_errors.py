"""Market Order Error Hierarchy.

This module defines custom exceptions for market order execution failures.
"""

from decimal import Decimal


class MarketOrderError(Exception):
    """Base exception for market order execution errors."""

    @classmethod
    def disabled_error(cls) -> "MarketOrderError":
        """Create error for disabled market orders.

        Returns:
            MarketOrderError: Error instance for disabled market orders
        """
        return cls("Market orders are disabled in configuration")

    @classmethod
    def timeout_error(cls, timeout_seconds: int) -> "MarketOrderError":
        """Create error for market order timeout.

        Returns:
            MarketOrderError: Error instance for timeout
        """
        return cls(f"Market order timed out after {timeout_seconds}s")

    @classmethod
    def no_orders_error(cls) -> "MarketOrderError":
        """Create error when no orders were executed.

        Returns:
            MarketOrderError: Error instance for no orders executed
        """
        return cls("No orders were executed")

    @classmethod
    def invalid_price_error(cls, price: object) -> "MarketOrderError":
        """Create error for invalid aggressive price.

        Returns:
            MarketOrderError: Error instance for invalid price
        """
        return cls(f"Invalid aggressive price: {price}")

    @classmethod
    def no_order_book_error(cls, symbol: str) -> "MarketOrderError":
        """Create error when no order book is available.

        Returns:
            MarketOrderError: Error instance for missing order book
        """
        return cls(f"Cannot calculate market order price: no order book for {symbol}")


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


class MarketOrderParameterError(ValueError):
    """Raised when market order parameters are invalid or malformed."""

    @classmethod
    def empty_symbol_error(cls) -> "MarketOrderParameterError":
        """Create error for empty symbol.

        Returns:
            MarketOrderParameterError: Error instance for empty symbol
        """
        return cls("Symbol must be a non-empty string")

    @classmethod
    def invalid_quantity_error(cls) -> "MarketOrderParameterError":
        """Create error for invalid quantity.

        Returns:
            MarketOrderParameterError: Error instance for invalid quantity
        """
        return cls("Quantity must be a positive Decimal")

    @classmethod
    def infinite_quantity_error(cls) -> "MarketOrderParameterError":
        """Create error for infinite quantity.

        Returns:
            MarketOrderParameterError: Error instance for infinite quantity
        """
        return cls("Quantity must be finite")

    @classmethod
    def finite_decimal_error(cls) -> "MarketOrderParameterError":
        """Create error for non-finite decimal.

        Returns:
            MarketOrderParameterError: Error instance for non-finite decimal
        """
        return cls("Percentage must be a finite decimal")

    @classmethod
    def positive_error(cls) -> "MarketOrderParameterError":
        """Create error for non-positive value.

        Returns:
            MarketOrderParameterError: Error instance for non-positive value
        """
        return cls("Percentage must be positive")

    @classmethod
    def slippage_default_error(cls) -> "MarketOrderParameterError":
        """Create error for missing default in slippage map.

        Returns:
            MarketOrderParameterError: Error instance for missing default slippage
        """
        return cls("slippage_by_symbol must contain a 'default' entry")

    @classmethod
    def slippage_invalid_error(cls, symbol: str, slippage: object) -> "MarketOrderParameterError":
        """Create error for invalid slippage value.

        Returns:
            MarketOrderParameterError: Error instance for invalid slippage
        """
        return cls(f"Invalid slippage for {symbol}: {slippage}")

    @classmethod
    def config_disabled_error(cls) -> "MarketOrderParameterError":
        """Create error for disabled configuration.

        Returns:
            MarketOrderParameterError: Error instance for disabled configuration
        """
        return cls("Market orders are disabled in configuration")

    @classmethod
    def config_slippage_error(cls) -> "MarketOrderParameterError":
        """Create error for invalid slippage configuration.

        Returns:
            MarketOrderParameterError: Error instance for invalid slippage config
        """
        return cls("Maximum slippage must be positive")

    @classmethod
    def config_deviation_error(cls) -> "MarketOrderParameterError":
        """Create error for invalid price deviation configuration.

        Returns:
            MarketOrderParameterError: Error instance for invalid deviation config
        """
        return cls("Maximum price deviation must be positive")
