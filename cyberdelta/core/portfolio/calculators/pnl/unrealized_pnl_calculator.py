"""Unrealized P&L calculator with mark-to-market calculations."""

from __future__ import annotations

import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.calculators.base.base_calculator import BaseCalculator
from cyberdelta.core.portfolio.portfolio_types.calculation_types import (
    CalculationMetadata,
    PortfolioUnrealizedPnLResult,
    UnrealizedPnLResult,
)


if TYPE_CHECKING:
    from cyberdelta.core.models import DerivativePosition
    from cyberdelta.core.portfolio.portfolio_types.service_protocols import PriceServiceProtocol

logger = get_logger(__name__)

# Constants
STANDARD_CURRENCY_CODE_LENGTH = 3


class CurrencyConverter:
    """Simple currency converter interface for unrealized P&L calculations."""

    async def convert(self, amount: Decimal, from_currency: str, to_currency: str) -> Decimal:
        """Convert amount from one currency to another."""
        # For now, implement basic conversion logic
        # In production, this would use real exchange rates
        if from_currency == to_currency:
            return amount

        # Placeholder - use 1:1 conversion for now
        logger.warning(
            "currency_conversion_placeholder",
            from_currency=from_currency,
            to_currency=to_currency,
            amount=amount,
        )
        return amount


class UnrealizedPnLCalculator(BaseCalculator[UnrealizedPnLResult]):
    """Calculates unrealized P&L using current market prices.

    Handles mark-to-market calculations for derivative positions.
    """

    def __init__(
        self,
        name: str = "UnrealizedPnLCalculator",
        config: dict[str, Any] | None = None,
        price_service: PriceServiceProtocol | None = None,
        currency_converter: CurrencyConverter | None = None,
    ) -> None:
        """Initialize the unrealized P&L calculator.

        Args:
            name: Calculator name
            config: Configuration dictionary
            price_service: Service for price data
            currency_converter: Service for currency conversions
        """
        super().__init__(name, config)

        self.price_service = price_service
        self.currency_converter = currency_converter or CurrencyConverter()

        logger.info("unrealized_pnl_calculator_created", calculator_name=name)

    async def calculate(
        self, position: DerivativePosition, base_currency: str = "USD", **kwargs: object
    ) -> UnrealizedPnLResult:
        """Calculate unrealized P&L for a single position.

        Args:
            position: Position to calculate P&L for
            base_currency: Currency for P&L calculation
            **kwargs: Additional calculation parameters

        Returns:
            UnrealizedPnLResult with calculation details
        """
        return await self.calculate_for_position(position, base_currency)

    async def calculate_for_position(
        self, position: DerivativePosition, base_currency: str = "USD"
    ) -> UnrealizedPnLResult:
        """Calculate unrealized P&L for a single position.

        Args:
            position: Position to calculate P&L for
            base_currency: Currency for P&L calculation

        Returns:
            UnrealizedPnLResult with calculation details
        """
        if position.size == Decimal(0):
            return UnrealizedPnLResult(
                pnl=Decimal(0),
                current_price=Decimal(0),
                entry_price=position.entry_price or Decimal(0),
                position_size=Decimal(0),
                currency=base_currency,
                metadata=CalculationMetadata(
                    calculation_method="unrealized_pnl", notes="no_position"
                ),
            )

        # Get current market price
        if not self.price_service:
            logger.warning("no_price_service_using_position_price", symbol=position.symbol)
            current_price = position.entry_price
        else:
            try:
                current_price = await self.price_service.get_price_in_currency(
                    position.symbol, base_currency
                )
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
                logger.exception("failed_to_get_current_price", symbol=position.symbol)
                current_price = position.entry_price

        # Calculate unrealized P&L
        entry_price = position.entry_price or Decimal(0)
        current_price = current_price or Decimal(0)
        position_size = position.size

        if position_size > Decimal(0):
            # Long position: P&L = (current_price - entry_price) * size
            pnl = (current_price - entry_price) * position_size
        else:
            # Short position: P&L = (entry_price - current_price) * abs(size)
            pnl = (entry_price - current_price) * abs(position_size)

        # Convert to base currency if needed
        try:
            pnl_in_base_currency = await self.currency_converter.convert(
                pnl,
                "USD",
                base_currency,  # Assuming P&L is in USD initially
            )
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            logger.warning("currency_conversion_failed", base_currency=base_currency)
            pnl_in_base_currency = pnl

        logger.debug(
            "unrealized_pnl_calculated",
            symbol=position.symbol,
            position_size=position_size,
            entry_price=entry_price,
            current_price=current_price,
            pnl=pnl_in_base_currency,
            currency=base_currency,
        )

        return UnrealizedPnLResult(
            pnl=pnl_in_base_currency,
            current_price=current_price,
            entry_price=entry_price,
            position_size=position_size,
            currency=base_currency,
            metadata=CalculationMetadata(
                calculation_method="unrealized_pnl",
                calculation_timestamp=self._get_current_timestamp(),
                notes=f"symbol={position.symbol}",
            ),
        )

    async def calculate_for_portfolio(
        self, positions: list[DerivativePosition], base_currency: str = "USD"
    ) -> PortfolioUnrealizedPnLResult:
        """Calculate unrealized P&L for a portfolio of positions.

        Args:
            positions: List of positions to calculate P&L for
            base_currency: Currency for P&L calculation

        Returns:
            PortfolioUnrealizedPnLResult with aggregated calculation
        """
        if not positions:
            return PortfolioUnrealizedPnLResult(
                total_pnl=Decimal(0),
                currency=base_currency,
                position_results=[],
                metadata=CalculationMetadata(
                    calculation_method="portfolio_unrealized_pnl", notes="no_positions"
                ),
            )

        position_results: list[UnrealizedPnLResult] = []
        total_pnl = Decimal(0)

        # Calculate P&L for each position
        for position in positions:
            try:
                result = await self.calculate_for_position(position, base_currency)
                position_results.append(result)
                total_pnl += result.pnl
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
                logger.exception("portfolio_pnl_calculation_failed", symbol=position.symbol)
                # Create a zero P&L result for failed calculations
                failed_result = UnrealizedPnLResult(
                    pnl=Decimal(0),
                    current_price=position.entry_price or Decimal(0),
                    entry_price=position.entry_price or Decimal(0),
                    position_size=position.size,
                    currency=base_currency,
                    metadata=CalculationMetadata(
                        calculation_method="unrealized_pnl",
                        notes=f"error={e} symbol={position.symbol}",
                    ),
                )
                position_results.append(failed_result)

        logger.info(
            "portfolio_unrealized_pnl_calculated",
            total_pnl=total_pnl,
            position_count=len(positions),
            currency=base_currency,
        )

        return PortfolioUnrealizedPnLResult(
            total_pnl=total_pnl,
            currency=base_currency,
            position_results=position_results,
            metadata=CalculationMetadata(
                calculation_method="portfolio_unrealized_pnl",
                calculation_timestamp=self._get_current_timestamp(),
                notes=(
                    f"position_count={len(positions)} "
                    f"successful_calculations={
                        len([
                            r
                            for r in position_results
                            if r.metadata and 'error' not in r.metadata.notes
                        ])
                    }"
                ),
            ),
        )

    def validate_inputs(
        self, position: DerivativePosition, base_currency: str = "USD", **kwargs: object
    ) -> None:
        """Validate inputs for unrealized P&L calculation.

        Args:
            position: Position to validate
            base_currency: Currency to validate
            **kwargs: Additional parameters

        Raises:
            ValueError: If inputs are invalid
        """
        if position.entry_price is not None and position.entry_price <= Decimal(0):
            raise ValueError

        if not base_currency:
            raise ValueError

        if len(base_currency) != STANDARD_CURRENCY_CODE_LENGTH:
            logger.warning(
                "unusual_currency_code_length",
                base_currency=base_currency,
                length=len(base_currency),
            )

    def _get_current_timestamp(self) -> float:
        """Get current timestamp for metadata."""
        return time.time()

    async def calculate_portfolio_summary(
        self, positions: list[DerivativePosition], base_currency: str = "USD"
    ) -> dict[str, Any]:
        """Calculate portfolio summary metrics.

        Args:
            positions: List of positions
            base_currency: Currency for calculations

        Returns:
            Dictionary with portfolio summary metrics
        """
        portfolio_result = await self.calculate_for_portfolio(positions, base_currency)

        # Calculate additional metrics
        long_pnl = sum(
            result.pnl
            for result in portfolio_result.position_results
            if result.position_size > Decimal(0)
        )

        short_pnl = sum(
            result.pnl
            for result in portfolio_result.position_results
            if result.position_size < Decimal(0)
        )

        winning_positions = len([
            result for result in portfolio_result.position_results if result.pnl > Decimal(0)
        ])

        losing_positions = len([
            result for result in portfolio_result.position_results if result.pnl < Decimal(0)
        ])

        return {
            "total_unrealized_pnl": portfolio_result.total_pnl,
            "long_pnl": long_pnl,
            "short_pnl": short_pnl,
            "winning_positions": winning_positions,
            "losing_positions": losing_positions,
            "total_positions": len(portfolio_result.position_results),
            "currency": base_currency,
            "calculation_timestamp": self._get_current_timestamp(),
        }
