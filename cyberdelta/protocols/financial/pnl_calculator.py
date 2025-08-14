"""PnL calculator protocol for type-safe financial calculations."""

from decimal import Decimal
from typing import Protocol

from cyberdelta.models import DerivativePosition, Fill
from cyberdelta.models.financial import PnLResult


class PnLCalculatorProtocol(Protocol):
    """Protocol for PnL calculation implementations.

    This protocol defines the interface for different PnL calculation strategies
    such as FIFO, LIFO, or weighted average. It ensures type safety across
    different calculation methods while allowing for exchange-specific implementations.

    Use Cases:
    - Exchange-specific PnL calculation rules
    - Different accounting methods (FIFO, LIFO, weighted average)
    - Testing with predictable mock implementations
    - Regulatory compliance with specific calculation requirements
    """

    def calculate_unrealized_pnl(
        self,
        position: DerivativePosition,
        mark_price: Decimal,
        include_fees: bool | None = None,
        target_currency: str | None = None,
    ) -> PnLResult:
        """Calculate unrealized PnL for a position.

        Args:
            position: Position to calculate PnL for
            mark_price: Current market price for the position
            include_fees: Override config default for fee inclusion
            target_currency: Target currency for result (uses base if None)

        Returns:
            PnLResult with amount, currency, and calculation metadata

        Raises:
            ValueError: If position or mark_price are invalid
            CurrencyConversionError: If currency conversion fails
        """
        ...

    def calculate_realized_pnl(
        self,
        position: DerivativePosition,
        fill: Fill,
        include_fees: bool | None = None,
        target_currency: str | None = None,
    ) -> PnLResult:
        """Calculate realized PnL for position closing fill.

        Args:
            position: Position being closed/reduced
            fill: Fill that closes/reduces the position
            include_fees: Override config default for fee inclusion
            target_currency: Target currency for result (uses base if None)

        Returns:
            PnLResult with realized amount, currency, and calculation metadata

        Raises:
            ValueError: If fill doesn't close position or inputs are invalid
            CurrencyConversionError: If currency conversion fails
        """
        ...

    def calculate_portfolio_pnl(
        self,
        positions: list[DerivativePosition],
        mark_prices: dict[str, Decimal],
        include_fees: bool | None = None,
        target_currency: str | None = None,
    ) -> PnLResult:
        """Calculate total portfolio PnL across all positions.

        Args:
            positions: List of all portfolio positions
            mark_prices: Current market prices by symbol
            include_fees: Override config default for fee inclusion
            target_currency: Target currency for result (uses base if None)

        Returns:
            PnLResult with total portfolio PnL and calculation metadata

        Raises:
            ValueError: If positions or mark_prices are inconsistent
            CurrencyConversionError: If currency conversion fails
        """
        ...
